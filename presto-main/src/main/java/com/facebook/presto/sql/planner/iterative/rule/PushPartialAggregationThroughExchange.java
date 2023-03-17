/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.FeaturesConfig.PartialAggregationStrategy;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.optimizations.SymbolMapper;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.getPartialAggregationByteReductionThreshold;
import static com.facebook.presto.SystemSessionProperties.getPartialAggregationStrategy;
import static com.facebook.presto.SystemSessionProperties.isMergeAggregationsWithAndWithoutFilter;
import static com.facebook.presto.SystemSessionProperties.isStreamingForPartialAggregationEnabled;
import static com.facebook.presto.operator.aggregation.AggregationUtils.isDecomposable;
import static com.facebook.presto.spi.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.PartialAggregationStrategy.AUTOMATIC;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.PartialAggregationStrategy.NEVER;
import static com.facebook.presto.sql.planner.PlannerUtils.addProjections;
import static com.facebook.presto.sql.planner.PlannerUtils.getFinalAggregation;
import static com.facebook.presto.sql.planner.PlannerUtils.getIntermediateAggregation;
import static com.facebook.presto.sql.planner.PlannerUtils.getVariableForIntermediateAggregation;
import static com.facebook.presto.sql.planner.PlannerUtils.removeFilterAndMask;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.GATHER;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.planner.plan.Patterns.exchange;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.relational.Expressions.constantNull;
import static com.facebook.presto.sql.relational.Expressions.specialForm;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class PushPartialAggregationThroughExchange
        implements Rule<AggregationNode>
{
    private final FunctionAndTypeManager functionAndTypeManager;

    public PushPartialAggregationThroughExchange(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionManager is null");
    }

    private static final Capture<ExchangeNode> EXCHANGE_NODE = Capture.newCapture();

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .with(source().matching(
                    exchange()
                            .matching(node -> !node.getOrderingScheme().isPresent())
                            .capturedAs(EXCHANGE_NODE)));

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode aggregationNode, Captures captures, Context context)
    {
        ExchangeNode exchangeNode = captures.get(EXCHANGE_NODE);
        boolean decomposable = isDecomposable(aggregationNode, functionAndTypeManager);

        if (aggregationNode.getStep().equals(SINGLE) &&
                aggregationNode.hasEmptyGroupingSet() &&
                aggregationNode.hasNonEmptyGroupingSet() &&
                exchangeNode.getType() == REPARTITION) {
            // single-step aggregation w/ empty grouping sets in a partitioned stage, so we need a partial that will produce
            // the default intermediates for the empty grouping set that will be routed to the appropriate final aggregation.
            // TODO: technically, AddExchanges generates a broken plan that this rule "fixes"
            checkState(
                    decomposable,
                    "Distributed aggregation with empty grouping set requires partial but functions are not decomposable");
            return Result.ofPlanNode(split(aggregationNode, context));
        }

        PartialAggregationStrategy partialAggregationStrategy = getPartialAggregationStrategy(context.getSession());
        if (!decomposable ||
                partialAggregationStrategy == NEVER ||
                partialAggregationStrategy == AUTOMATIC &&
                        partialAggregationNotUseful(aggregationNode, exchangeNode, context) &&
                        aggregationNode.getGroupingKeys().size() == 1) {
            return Result.empty();
        }

        // partial aggregation can only be pushed through exchange that doesn't change
        // the cardinality of the stream (i.e., gather or repartition)
        if ((exchangeNode.getType() != GATHER && exchangeNode.getType() != REPARTITION) ||
                exchangeNode.getPartitioningScheme().isReplicateNullsAndAny()) {
            return Result.empty();
        }

        if (exchangeNode.getType() == REPARTITION) {
            // if partitioning columns are not a subset of grouping keys,
            // we can't push this through
            List<VariableReferenceExpression> partitioningColumns = exchangeNode.getPartitioningScheme()
                    .getPartitioning()
                    .getArguments()
                    .stream()
                    .filter(VariableReferenceExpression.class::isInstance)
                    .map(VariableReferenceExpression.class::cast)
                    .collect(Collectors.toList());

            if (!aggregationNode.getGroupingKeys().containsAll(partitioningColumns)) {
                return Result.empty();
            }
        }

        // currently, we only support plans that don't use pre-computed hash functions
        if (aggregationNode.getHashVariable().isPresent() || exchangeNode.getPartitioningScheme().getHashColumn().isPresent()) {
            return Result.empty();
        }

        switch (aggregationNode.getStep()) {
            case SINGLE:
                // Split it into a FINAL on top of a PARTIAL and
                return Result.ofPlanNode(split(aggregationNode, context));
            case PARTIAL:
                // Push it underneath each branch of the exchange
                return Result.ofPlanNode(pushPartial(aggregationNode, exchangeNode, context));
            default:
                return Result.empty();
        }
    }

    private PlanNode pushPartial(AggregationNode aggregation, ExchangeNode exchange, Context context)
    {
        List<PlanNode> partials = new ArrayList<>();
        for (int i = 0; i < exchange.getSources().size(); i++) {
            PlanNode source = exchange.getSources().get(i);

            SymbolMapper.Builder mappingsBuilder = SymbolMapper.builder();
            for (int outputIndex = 0; outputIndex < exchange.getOutputVariables().size(); outputIndex++) {
                VariableReferenceExpression output = exchange.getOutputVariables().get(outputIndex);
                VariableReferenceExpression input = exchange.getInputs().get(i).get(outputIndex);
                if (!output.equals(input)) {
                    mappingsBuilder.put(output, input);
                }
            }

            SymbolMapper symbolMapper = mappingsBuilder.build();
            AggregationNode mappedPartial = symbolMapper.map(aggregation, source, context.getIdAllocator());

            Assignments.Builder assignments = Assignments.builder();

            for (VariableReferenceExpression output : aggregation.getOutputVariables()) {
                VariableReferenceExpression input = symbolMapper.map(output);
                assignments.put(output, input);
            }
            partials.add(new ProjectNode(exchange.getSourceLocation(), context.getIdAllocator().getNextId(), mappedPartial, assignments.build(), LOCAL));
        }

        for (PlanNode node : partials) {
            verify(aggregation.getOutputVariables().equals(node.getOutputVariables()));
        }
        // Since this exchange source is now guaranteed to have the same symbols as the inputs to the partial
        // aggregation, we don't need to rewrite symbols in the partitioning function
        List<VariableReferenceExpression> aggregationOutputs = aggregation.getOutputVariables();
        PartitioningScheme partitioning = new PartitioningScheme(
                exchange.getPartitioningScheme().getPartitioning(),
                aggregationOutputs,
                exchange.getPartitioningScheme().getHashColumn(),
                exchange.getPartitioningScheme().isReplicateNullsAndAny(),
                exchange.getPartitioningScheme().getBucketToPartition());

        return new ExchangeNode(
                aggregation.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                exchange.getType(),
                exchange.getScope(),
                partitioning,
                partials,
                ImmutableList.copyOf(Collections.nCopies(partials.size(), aggregationOutputs)),
                exchange.isEnsureSourceOrdering(),
                Optional.empty());
    }

    private PlanNode split(AggregationNode node, Context context)
    {
        if (isMergeAggregationsWithAndWithoutFilter(context.getSession())) {
            if (!node.getGroupingKeys().isEmpty() &&
                    node.getAggregations().values().stream().map(x -> functionAndTypeManager.getFunctionMetadata(x.getFunctionHandle())).noneMatch(x -> x.isCalledOnNullInput())) {
                return mergeAggsWithAndWithoutFilter(node, context);
            }
        }
        // otherwise, add a partial and final with an exchange in between
        Map<VariableReferenceExpression, AggregationNode.Aggregation> intermediateAggregation = new HashMap<>();
        Map<VariableReferenceExpression, AggregationNode.Aggregation> finalAggregation = new HashMap<>();
        for (Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation> entry : node.getAggregations().entrySet()) {
            AggregationNode.Aggregation originalAggregation = entry.getValue();
            VariableReferenceExpression intermediateVariable = getVariableForIntermediateAggregation(originalAggregation, functionAndTypeManager, context.getVariableAllocator());

            checkState(!originalAggregation.getOrderBy().isPresent(), "Aggregate with ORDER BY does not support partial aggregation");
            intermediateAggregation.put(intermediateVariable, getIntermediateAggregation(originalAggregation, functionAndTypeManager));

            // rewrite final aggregation in terms of intermediate function
            finalAggregation.put(entry.getKey(), getFinalAggregation(originalAggregation, functionAndTypeManager, intermediateVariable));
        }

        // We can always enable streaming aggregation for partial aggregations. But if the table is not pre-group by the groupby columns, it may have regressions.
        // This session property is just a solution to force enabling when we know the execution would benefit from partial streaming aggregation.
        // We can work on determining it based on the input table properties later.
        List<VariableReferenceExpression> preGroupedSymbols = ImmutableList.of();
        if (isStreamingForPartialAggregationEnabled(context.getSession())) {
            preGroupedSymbols = ImmutableList.copyOf(node.getGroupingSets().getGroupingKeys());
        }

        PlanNode partial = new AggregationNode(
                node.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                node.getSource(),
                intermediateAggregation,
                node.getGroupingSets(),
                // preGroupedSymbols reflect properties of the input. Splitting the aggregation and pushing partial aggregation
                // through the exchange may or may not preserve these properties. Hence, it is safest to drop preGroupedSymbols here.
                preGroupedSymbols,
                PARTIAL,
                node.getHashVariable(),
                node.getGroupIdVariable());

        return new AggregationNode(
                node.getSourceLocation(),
                node.getId(),
                partial,
                finalAggregation,
                node.getGroupingSets(),
                // preGroupedSymbols reflect properties of the input. Splitting the aggregation and pushing partial aggregation
                // through the exchange may or may not preserve these properties. Hence, it is safest to drop preGroupedSymbols here.
                ImmutableList.of(),
                FINAL,
                node.getHashVariable(),
                node.getGroupIdVariable());
    }

    private PlanNode mergeAggsWithAndWithoutFilter(AggregationNode node, Context context)
    {
        checkState(node.getAggregations().values().stream().noneMatch(x -> x.getFilter().isPresent()),
                "All aggregation filters should already be rewritten to mask before this optimization rule");

        Set<AggregationNode.Aggregation> aggregationsWithoutMask = node.getAggregations().values().stream().filter(x -> !x.getMask().isPresent()).collect(toImmutableSet());
        Map<AggregationNode.Aggregation, AggregationNode.Aggregation> aggregationsToCombine = node.getAggregations().values().stream()
                .filter(x -> x.getMask().isPresent() && aggregationsWithoutMask.contains(removeFilterAndMask(x)))
                .collect(toImmutableMap(identity(), x -> removeFilterAndMask(x)));
        Set<AggregationNode.Aggregation> aggregationsForPartialAgg = node.getAggregations().values().stream()
                .filter(x -> !aggregationsToCombine.containsKey(x)).collect(Collectors.toSet());

        ImmutableMap.Builder<VariableReferenceExpression, AggregationNode.Aggregation> intermediateAggregationBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<VariableReferenceExpression, AggregationNode.Aggregation> finalAggregationBuilder = ImmutableMap.builder();

        ImmutableMap.Builder<AggregationNode.Aggregation, VariableReferenceExpression> intermediateVariablesForAggregationBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<AggregationNode.Aggregation, VariableReferenceExpression> maskVariablesBuilder = ImmutableMap.builder();
        for (Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation> entry : node.getAggregations().entrySet()) {
            AggregationNode.Aggregation originalAggregation = entry.getValue();
            if (aggregationsForPartialAgg.contains(originalAggregation)) {
                checkState(!originalAggregation.getOrderBy().isPresent(), "Aggregate with ORDER BY does not support partial aggregation");
                VariableReferenceExpression intermediateVariable = getVariableForIntermediateAggregation(originalAggregation, functionAndTypeManager, context.getVariableAllocator());
                intermediateAggregationBuilder.put(intermediateVariable, getIntermediateAggregation(originalAggregation, functionAndTypeManager));
                intermediateVariablesForAggregationBuilder.put(originalAggregation, intermediateVariable);

                // rewrite final aggregation in terms of intermediate function
                finalAggregationBuilder.put(entry.getKey(), getFinalAggregation(originalAggregation, functionAndTypeManager, intermediateVariable));
            }
            else {
                maskVariablesBuilder.put(originalAggregation, originalAggregation.getMask().get());
            }
        }
        ImmutableMap<AggregationNode.Aggregation, VariableReferenceExpression> intermediateVariablesForAggregation = intermediateVariablesForAggregationBuilder.build();
        ImmutableMap<AggregationNode.Aggregation, VariableReferenceExpression> maskVariables = maskVariablesBuilder.build();

        AggregationNode.GroupingSetDescriptor partialGroupingSetDescriptor;
        ImmutableList.Builder<VariableReferenceExpression> groupingVariables = ImmutableList.builder();
        if (maskVariables.isEmpty()) {
            partialGroupingSetDescriptor = node.getGroupingSets();
        }
        else {
            AggregationNode.GroupingSetDescriptor groupingSetDescriptor = node.getGroupingSets();
            groupingVariables.addAll(groupingSetDescriptor.getGroupingKeys());
            groupingVariables.addAll(maskVariables.values());
            partialGroupingSetDescriptor = new AggregationNode.GroupingSetDescriptor(
                    groupingVariables.build(), groupingSetDescriptor.getGroupingSetCount(), groupingSetDescriptor.getGlobalGroupingSets());
        }

        // We can always enable streaming aggregation for partial aggregations. But if the table is not pre-group by the groupby columns, it may have regressions.
        // This session property is just a solution to force enabling when we know the execution would benefit from partial streaming aggregation.
        // We can work on determining it based on the input table properties later.
        List<VariableReferenceExpression> preGroupedSymbols = ImmutableList.of();
        if (isStreamingForPartialAggregationEnabled(context.getSession())) {
            preGroupedSymbols = ImmutableList.copyOf(node.getGroupingSets().getGroupingKeys());
        }

        PlanNode partial = new AggregationNode(
                node.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                node.getSource(),
                intermediateAggregationBuilder.build(),
                partialGroupingSetDescriptor,
                // preGroupedSymbols reflect properties of the input. Splitting the aggregation and pushing partial aggregation
                // through the exchange may or may not preserve these properties. Hence, it is safest to drop preGroupedSymbols here.
                preGroupedSymbols,
                PARTIAL,
                node.getHashVariable(),
                node.getGroupIdVariable());

        if (!maskVariables.isEmpty()) {
            ImmutableList.Builder<RowExpression> projectionsFromPartialAgg = ImmutableList.builder();
            ImmutableList.Builder<VariableReferenceExpression> variablesForPartialAggResult = ImmutableList.builder();
            Map<AggregationNode.Aggregation, List<VariableReferenceExpression>> resultVariablesForAggregation = node.getAggregations().entrySet().stream()
                    .collect(Collectors.groupingBy(Map.Entry::getValue, Collectors.mapping(Map.Entry::getKey, Collectors.toList())));
            // project out the aggs with filters
            for (Map.Entry<AggregationNode.Aggregation, AggregationNode.Aggregation> entry : aggregationsToCombine.entrySet()) {
                AggregationNode.Aggregation originalAggregation = entry.getKey();
                AggregationNode.Aggregation aggregationWithoutMask = entry.getValue();
                VariableReferenceExpression maskVariable = maskVariables.get(originalAggregation);
                VariableReferenceExpression intermediateVariable = intermediateVariablesForAggregation.get(aggregationWithoutMask);
                RowExpression conditionalResult = ifThenElse(maskVariable, intermediateVariable, constantNull(intermediateVariable.getType()));
                projectionsFromPartialAgg.add(conditionalResult);
                VariableReferenceExpression maskedPartialResult = context.getVariableAllocator().newVariable(intermediateVariable);
                variablesForPartialAggResult.add(maskedPartialResult);

                // Here this is a merged partial agg so we mask only for final agg
                AggregationNode.Aggregation maskedFinalAggregation = getFinalAggregation(aggregationWithoutMask, functionAndTypeManager, maskedPartialResult);
                for (VariableReferenceExpression outputVariable : resultVariablesForAggregation.get(originalAggregation)) {
                    finalAggregationBuilder.put(outputVariable, maskedFinalAggregation);
                }
            }

            partial = addProjections(partial, context.getIdAllocator(), context.getVariableAllocator(), projectionsFromPartialAgg.build(), variablesForPartialAggResult.build());
        }

        return new AggregationNode(
                node.getSourceLocation(),
                node.getId(),
                partial,
                finalAggregationBuilder.build(),
                node.getGroupingSets(),
                // preGroupedSymbols reflect properties of the input. Splitting the aggregation and pushing partial aggregation
                // through the exchange may or may not preserve these properties. Hence, it is safest to drop preGroupedSymbols here.
                ImmutableList.of(),
                FINAL,
                node.getHashVariable(),
                node.getGroupIdVariable());
    }

    private boolean partialAggregationNotUseful(AggregationNode aggregationNode, ExchangeNode exchangeNode, Context context)
    {
        StatsProvider stats = context.getStatsProvider();
        PlanNodeStatsEstimate exchangeStats = stats.getStats(exchangeNode);
        PlanNodeStatsEstimate aggregationStats = stats.getStats(aggregationNode);
        double inputBytes = exchangeStats.getOutputSizeInBytes(exchangeNode);
        double outputBytes = aggregationStats.getOutputSizeInBytes(aggregationNode);
        double byteReductionThreshold = getPartialAggregationByteReductionThreshold(context.getSession());

        return exchangeStats.isConfident() && outputBytes > inputBytes * byteReductionThreshold;
    }

    public static RowExpression ifThenElse(RowExpression... arguments)
    {
        return specialForm(SpecialFormExpression.Form.IF, arguments[1].getType(), arguments);
    }
}
