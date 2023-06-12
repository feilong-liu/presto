package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.common.plan.PlanCanonicalizationStrategy;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.CanonicalPlanGenerator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.SystemSessionProperties.isRewriteUnionWithCommonSourceUnderProjectEnabled;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.expressions.LogicalRowExpressions.or;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.SWITCH;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.WHEN;
import static com.facebook.presto.sql.planner.PlannerUtils.duplicateOutputWithUnnest;
import static com.facebook.presto.sql.planner.PlannerUtils.equalityPredicate;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class RewriteUnionWithCommonSource
        implements PlanOptimizer
{
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final FunctionAndTypeManager functionAndTypeManager;

    public RewriteUnionWithCommonSource(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = functionAndTypeManager;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        if (isRewriteUnionWithCommonSourceUnderProjectEnabled(session)) {
            return SimplePlanRewriter.rewriteWith(new RewriteUnionWithCommonSource.Rewriter(functionAndTypeManager, variableAllocator, idAllocator, session), plan);
        }
        return plan;
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final FunctionAndTypeManager functionAndTypeManager;
        private final VariableAllocator variableAllocator;
        private final PlanNodeIdAllocator idAllocator;
        private final Session session;

        public Rewriter(FunctionAndTypeManager functionAndTypeManager, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, Session session)
        {
            this.functionAndTypeManager = functionAndTypeManager;
            this.variableAllocator = variableAllocator;
            this.idAllocator = idAllocator;
            this.session = session;
        }

        @Override
        public PlanNode visitUnion(UnionNode node, RewriteContext<Void> context)
        {
            List<PlanNode> sources = node.getSources().stream().map(x -> context.rewrite(x)).collect(Collectors.toList());
            if (!(sources.stream().allMatch(x -> x instanceof ProjectNode))) {
                return node.replaceChildren(sources);
            }

            List<ProjectNode> projects = sources.stream().map(x -> (ProjectNode) x).collect(Collectors.toList());
            List<PlanNode> commonSourceList;
            boolean hasFilterNode = false;
            if (projects.stream().allMatch(x -> x.getSource() instanceof FilterNode)) {
                hasFilterNode = true;
                commonSourceList = projects.stream().map(x -> ((FilterNode) x.getSource()).getSource()).collect(Collectors.toList());
            }
            else {
                commonSourceList = projects.stream().map(x -> x.getSource()).collect(Collectors.toList());
            }

            List<Optional<PlanNode>> cannoicalCommonSources = commonSourceList.stream().
                    map(x -> x.accept(new CanonicalPlanGenerator(PlanCanonicalizationStrategy.DEFAULT, objectMapper, session), new CanonicalPlanGenerator.Context()))
                    .collect(toImmutableList());
            if (cannoicalCommonSources.stream().noneMatch(Optional::isPresent) || cannoicalCommonSources.stream().map(Optional::get).distinct().count() > 1) {
                return node.replaceChildren(sources);
            }

            PlanNode commonSource = commonSourceList.get(0);
            VariableReferenceExpression unnestVariable = variableAllocator.newVariable("field", INTEGER);
            PlanNode unnestNode = duplicateOutputWithUnnest(commonSource, projects.size(), unnestVariable, idAllocator, variableAllocator, functionAndTypeManager);

            ImmutableList.Builder<Map<VariableReferenceExpression, VariableReferenceExpression>> rewriteMapListBuilder = ImmutableList.builder();
            for (PlanNode anotherSource : commonSourceList) {
                Map<VariableReferenceExpression, VariableReferenceExpression> rewriteMap = IntStream.range(0, commonSource.getOutputVariables().size()).boxed().
                        collect(toImmutableMap(idx -> anotherSource.getOutputVariables().get(idx), idx -> commonSource.getOutputVariables().get(idx)));
                rewriteMapListBuilder.add(rewriteMap);
            }
            List<Map<VariableReferenceExpression, VariableReferenceExpression>> rewriteMapList = rewriteMapListBuilder.build();

            if (hasFilterNode) {
                ImmutableList.Builder<RowExpression> newPredicates = ImmutableList.builder();
                List<FilterNode> filterNodes = projects.stream().map(x -> (FilterNode) x.getSource()).collect(Collectors.toList());
                for (int i = 0; i < filterNodes.size(); ++i) {
                    FilterNode filterNode = filterNodes.get(i);
                    RowExpression predicate = filterNode.getPredicate();
                    if (i != 0) {
                        predicate = predicate.accept(new RewriteUnionWithCommonSource.ExpressionRewriter(rewriteMapList.get(i)), null);
                    }
                    newPredicates.add(and(predicate, equalityPredicate(new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver()), unnestVariable, constant((long) i + 1, INTEGER))));
                }
                unnestNode = new FilterNode(node.getSourceLocation(), idAllocator.getNextId(), unnestNode, or(newPredicates.build()));
            }

            ImmutableMap.Builder<VariableReferenceExpression, RowExpression> outputMapping = ImmutableMap.builder();
            ImmutableMap.Builder<VariableReferenceExpression, RowExpression> conditionAssignments = ImmutableMap.builder();
            for (VariableReferenceExpression outputVariable : node.getOutputVariables()) {
                List<VariableReferenceExpression> inputVariables = node.getVariableMapping().get(outputVariable);
                VariableReferenceExpression conditionOutput = variableAllocator.newVariable(outputVariable);
                outputMapping.put(outputVariable, conditionOutput);

                ImmutableList.Builder<RowExpression> rewrittenExpression = ImmutableList.builder();
                ImmutableList.Builder<RowExpression> whenExpression = ImmutableList.builder();
                whenExpression.add(unnestVariable);
                for (int i = 0; i < projects.size(); ++i) {
                    RowExpression rowExpression = projects.get(i).getAssignments().getMap().get(inputVariables.get(i));
                    if (i != 0) {
                        rowExpression = rowExpression.accept(new RewriteUnionWithCommonSource.ExpressionRewriter(rewriteMapList.get(i)), null);
                    }
                    rewrittenExpression.add(rowExpression);
                    whenExpression.add(new SpecialFormExpression(WHEN, rowExpression.getType(), constant((long) i + 1, INTEGER), rowExpression));
                }
                if (rewrittenExpression.build().stream().distinct().count() == 1) {
                    conditionAssignments.put(conditionOutput, rewrittenExpression.build().get(0));
                }
                else {
                    SpecialFormExpression conditionOutputExpression = new SpecialFormExpression(SWITCH, conditionOutput.getType(), whenExpression.build());
                    conditionAssignments.put(conditionOutput, conditionOutputExpression);
                }
            }

            ProjectNode conditionProject = new ProjectNode(idAllocator.getNextId(), unnestNode, Assignments.copyOf(conditionAssignments.build()));
            return new ProjectNode(idAllocator.getNextId(), conditionProject, Assignments.copyOf(outputMapping.build()));
        }
    }

    private static class ExpressionRewriter
            implements RowExpressionVisitor<RowExpression, Void>
    {
        private final Map<VariableReferenceExpression, VariableReferenceExpression> expressionMap;

        public ExpressionRewriter(Map<VariableReferenceExpression, VariableReferenceExpression> expressionMap)
        {
            this.expressionMap = ImmutableMap.copyOf(expressionMap);
        }

        @Override
        public RowExpression visitCall(CallExpression call, Void context)
        {
            return new CallExpression(
                    call.getSourceLocation(),
                    call.getDisplayName(),
                    call.getFunctionHandle(),
                    call.getType(),
                    call.getArguments().stream().map(argument -> argument.accept(this, null)).collect(toImmutableList()));
        }

        @Override
        public RowExpression visitInputReference(InputReferenceExpression reference, Void context)
        {
            return reference;
        }

        @Override
        public RowExpression visitConstant(ConstantExpression literal, Void context)
        {
            return literal;
        }

        @Override
        public RowExpression visitLambda(LambdaDefinitionExpression lambda, Void context)
        {
            return lambda;
        }

        @Override
        public RowExpression visitVariableReference(VariableReferenceExpression reference, Void context)
        {
            return expressionMap.getOrDefault(reference, reference);
        }

        @Override
        public RowExpression visitSpecialForm(SpecialFormExpression specialForm, Void context)
        {
            return new SpecialFormExpression(
                    specialForm.getForm(),
                    specialForm.getType(),
                    specialForm.getArguments().stream().map(argument -> argument.accept(this, null)).collect(toImmutableList()));
        }
    }
}
