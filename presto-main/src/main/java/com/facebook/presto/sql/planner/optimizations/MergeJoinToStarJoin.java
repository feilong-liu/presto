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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Logger;

import static com.facebook.presto.SystemSessionProperties.isMergeStarJoinEnabled;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RowType.anonymous;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.constantNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class MergeJoinToStarJoin
        implements PlanOptimizer
{
    private static final Logger log = Logger.getLogger(MergeJoinToStarJoin.class.getName());
    private final FunctionAndTypeManager functionAndTypeManager;

    public MergeJoinToStarJoin(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = functionAndTypeManager;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        if (isMergeStarJoinEnabled(session)) {
            return SimplePlanRewriter.rewriteWith(new Rewriter(variableAllocator, idAllocator, functionAndTypeManager), plan);
        }
        return plan;
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final VariableAllocator variableAllocator;
        private final PlanNodeIdAllocator planNodeIdAllocator;
        private final FunctionAndTypeManager functionAndTypeManager;

        public Rewriter(VariableAllocator variableAllocator, PlanNodeIdAllocator planNodeIdAllocator, FunctionAndTypeManager functionAndTypeManager)
        {
            this.variableAllocator = variableAllocator;
            this.planNodeIdAllocator = planNodeIdAllocator;
            this.functionAndTypeManager = functionAndTypeManager;
        }

        private static boolean eligibleJoinNode(JoinNode joinNode)
        {
            return joinNode.getCriteria().size() == 1 && joinNode.getType() == LEFT;
        }

        @Override
        public PlanNode visitJoin(JoinNode joinNode, RewriteContext<Void> context)
        {
            if (!eligibleJoinNode(joinNode)) {
                return context.defaultRewrite(joinNode, context.get());
            }
            VariableReferenceExpression leftJoinKey = joinNode.getCriteria().get(0).getLeft();
            Optional<JoinNode.DistributionType> distributionType = joinNode.getDistributionType();

            ImmutableList.Builder<PlanNode> rightChildNodes = ImmutableList.builder();
            rightChildNodes.add(joinNode.getRight());

            ImmutableList.Builder<JoinNode.EquiJoinClause> equiJoinClauseList = ImmutableList.builder();
            equiJoinClauseList.add(joinNode.getCriteria().get(0));

            List<VariableReferenceExpression> rightJoinKey = new ArrayList<>();
            rightJoinKey.add(joinNode.getCriteria().get(0).getRight());

            ImmutableList.Builder<Optional<RowExpression>> filterExpressionList = ImmutableList.builder();
            filterExpressionList.add(joinNode.getFilter());

            ImmutableList.Builder<Optional<VariableReferenceExpression>> rightHashVariableList = ImmutableList.builder();
            rightHashVariableList.add(joinNode.getRightHashVariable());

            // Currently only deal with multiple build with one probe
            PlanNode child = joinNode.getLeft();
            while (child instanceof JoinNode && ((JoinNode) child).getCriteria().size() == 1 && ((JoinNode) child).getCriteria().get(0).getLeft().equals(leftJoinKey)
                    && ((JoinNode) child).getType().equals(LEFT) && ((JoinNode) child).getDistributionType().equals(distributionType)) {
                JoinNode childJoin = (JoinNode) child;
                rightChildNodes.add(childJoin.getRight());
                equiJoinClauseList.add(childJoin.getCriteria().get(0));
                rightJoinKey.add(childJoin.getCriteria().get(0).getRight());
                checkState(!childJoin.getFilter().isPresent());
                filterExpressionList.add(childJoin.getFilter());
                checkState(!childJoin.getRightHashVariable().isPresent());
                rightHashVariableList.add(childJoin.getRightHashVariable());
                checkState(childJoin.getDynamicFilters().isEmpty());
                child = childJoin.getLeft();
            }
            if (rightChildNodes.build().size() <= 1) {
                return context.defaultRewrite(joinNode, context.get());
            }
            PlanNode leftChild = context.rewrite(child, context.get());
            ImmutableList<PlanNode> rightChildren = rightChildNodes.build().stream().map(x -> context.rewrite(x, context.get())).collect(toImmutableList());

            Map<VariableReferenceExpression, Long> rightOutputIndexMap = new HashMap<>();

            List<RowExpression> rowExpressions = new ArrayList<>();
            for (PlanNode rightChild : rightChildren) {
                ImmutableList.Builder<Type> rightOutputTypes = ImmutableList.builder();
                ImmutableList.Builder<RowExpression> rightOutputVariables = ImmutableList.builder();
                int idx = 0;
                for (VariableReferenceExpression variableReferenceExpression : rightChild.getOutputVariables()) {
                    if (rightJoinKey.contains(variableReferenceExpression)) {
                        continue;
                    }
                    rightOutputTypes.add(variableReferenceExpression.getType());
                    rightOutputVariables.add(variableReferenceExpression);
                    rightOutputIndexMap.put(variableReferenceExpression, (long) idx);
                    idx++;
                }
                rowExpressions.add(new SpecialFormExpression(rightChild.getSourceLocation(), SpecialFormExpression.Form.ROW_CONSTRUCTOR, anonymous(rightOutputTypes.build()),
                        rightOutputVariables.build()));
            }

            List<PlanNode> projectNodes = new ArrayList<>();
            for (int i = 0; i < rightChildren.size(); ++i) {
                Assignments.Builder builder = Assignments.builder();
                builder.put(rightJoinKey.get(i), rightJoinKey.get(i));
                for (int j = 0; j < rightChildren.size(); ++j) {
                    VariableReferenceExpression newProjectVariable = variableAllocator.newVariable(rowExpressions.get(j));
                    if (i == j) {
                        builder.put(newProjectVariable, rowExpressions.get(j));
                    }
                    else {
                        builder.put(newProjectVariable, constantNull(rowExpressions.get(j).getType()));
                    }
                }
                projectNodes.add(new ProjectNode(planNodeIdAllocator.getNextId(), rightChildren.get(i), builder.build()));
            }

            Map<VariableReferenceExpression, List<VariableReferenceExpression>> unionNodeMapping = new HashMap<>();
            for (int i = 0; i < projectNodes.get(0).getOutputVariables().size(); ++i) {
                VariableReferenceExpression key = projectNodes.get(0).getOutputVariables().get(i);
                List<VariableReferenceExpression> sourceList = new ArrayList<>();
                for (int j = 0; j < projectNodes.size(); ++j) {
                    sourceList.add(projectNodes.get(j).getOutputVariables().get(i));
                }
                unionNodeMapping.put(key, sourceList);
            }
            PlanNode unionNode = new UnionNode(joinNode.getSourceLocation(), planNodeIdAllocator.getNextId(), projectNodes, projectNodes.get(0).getOutputVariables(), unionNodeMapping);

            List<CallExpression> aggregationCall = new ArrayList<>();
            // output variables at pos 0 is aggregation key
            for (int i = 1; i < unionNode.getOutputVariables().size(); ++i) {
                aggregationCall.add(call(functionAndTypeManager, "arbitrary", unionNode.getOutputVariables().get(i).getType(), unionNode.getOutputVariables().get(i)));
            }
            Map<VariableReferenceExpression, AggregationNode.Aggregation> aggregations = new HashMap<>();
            List<VariableReferenceExpression> aggregationOutputVariables = new ArrayList<>();
            for (int i = 0; i < aggregationCall.size(); i++) {
                VariableReferenceExpression newVariable = variableAllocator.newVariable(aggregationCall.get(i));
                aggregationOutputVariables.add(newVariable);
                aggregations.put(newVariable, new AggregationNode.Aggregation(aggregationCall.get(i), Optional.empty(), Optional.empty(), false, Optional.empty()));
            }
            AggregationNode aggregationNode = new AggregationNode(joinNode.getSourceLocation(), planNodeIdAllocator.getNextId(), unionNode, aggregations,
                    AggregationNode.singleGroupingSet(ImmutableList.of(unionNode.getOutputVariables().get(0))),
                    ImmutableList.of(), AggregationNode.Step.SINGLE, Optional.empty(), Optional.empty());
            Map<VariableReferenceExpression, RowExpression> rightOutputVariableMap = new HashMap<>();
            for (int i = 0; i < rightChildren.size(); ++i) {
                VariableReferenceExpression rowVariable = aggregationOutputVariables.get(i);
                PlanNode rightChild = rightChildren.get(i);
                for (VariableReferenceExpression variableReferenceExpression : rightChild.getOutputVariables()) {
                    if (rightOutputIndexMap.containsKey(variableReferenceExpression)) {
                        RowExpression dereferenceExpression = new SpecialFormExpression(rightChild.getSourceLocation(), SpecialFormExpression.Form.DEREFERENCE,
                                variableReferenceExpression.getType(), ImmutableList.of(rowVariable, constant(rightOutputIndexMap.get(variableReferenceExpression), INTEGER)));
                        rightOutputVariableMap.put(variableReferenceExpression, dereferenceExpression);
                    }
                    else {
                        rightOutputVariableMap.put(variableReferenceExpression, unionNode.getOutputVariables().get(0));
                    }
                }
            }

            ImmutableList.Builder<VariableReferenceExpression> outputBuilder = ImmutableList.builder();
            JoinNode newJoinNode = new JoinNode(
                    joinNode.getSourceLocation(),
                    planNodeIdAllocator.getNextId(),
                    joinNode.getType(),
                    leftChild,
                    aggregationNode,
                    ImmutableList.of(new JoinNode.EquiJoinClause(joinNode.getCriteria().get(0).getLeft(), aggregationNode.getGroupingKeys().get(0))),
                    outputBuilder.addAll(leftChild.getOutputVariables()).addAll(aggregationNode.getOutputVariables()).build(), Optional.empty(), Optional.empty(), Optional.empty(),
                    Optional.empty(), ImmutableMap.of());

            Assignments.Builder assignments = Assignments.builder();
            for (VariableReferenceExpression joinOutputVariable : joinNode.getOutputVariables()) {
                if (rightOutputVariableMap.containsKey(joinOutputVariable)) {
                    assignments.put(joinOutputVariable, rightOutputVariableMap.get(joinOutputVariable));
                }
                else {
                    assignments.put(joinOutputVariable, joinOutputVariable);
                }
            }

            ProjectNode projectNode = new ProjectNode(planNodeIdAllocator.getNextId(), newJoinNode, assignments.build());

            return projectNode;
        }
    }
}
