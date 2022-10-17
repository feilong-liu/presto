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
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.plan.OffsetNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.isRemoveRedundantDistinctAggregationEnabled;
import static com.facebook.presto.spi.plan.AggregationNode.isDistinct;
import static com.facebook.presto.sql.planner.plan.ChildReplacer.replaceChildren;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

/**
 * Remove the redundant distinct if output is already distinct.
 * For example, for query select distinct k, sum(x) from table group by k
 * The plan will change
 * <p>
 * From:
 * <pre>
 * - Aggregation group by k, sum
 *   - Aggregation (sum <- AGG(x)) group by k
 * </pre>
 * To:
 * <pre>
 * - Aggregation (sum <- AGG(x)) group by k
 * </pre>
 * <p>
 */
public class RemoveRedundantDistinctAggregation
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, PlanVariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        if (isRemoveRedundantDistinctAggregationEnabled(session)) {
            PlanWithProperties result = new RemoveRedundantDistinctAggregation.Rewriter().accept(plan);
            return result.getNode();
        }
        return plan;
    }

    private static class PlanWithProperties
    {
        private final PlanNode node;
        // Variables in each set combines to be distinct in the output of the plan node.
        private final List<Set<VariableReferenceExpression>> distinctVariableSet;

        public PlanWithProperties(PlanNode node, List<Set<VariableReferenceExpression>> distinctVariableSet)
        {
            this.node = requireNonNull(node, "node is null");
            this.distinctVariableSet = requireNonNull(distinctVariableSet, "StreamProperties is null");
        }

        public PlanNode getNode()
        {
            return node;
        }

        public List<Set<VariableReferenceExpression>> getProperties()
        {
            return distinctVariableSet;
        }
    }

    private class Rewriter
            extends InternalPlanVisitor<PlanWithProperties, Void>
    {
        @Override
        public PlanWithProperties visitPlan(PlanNode node, Void context)
        {
            // For nodes such as join, unnest etc. the distinct properties may be violated, hence pass empty list for these cases.
            return planAndRecplace(node, false);
        }

        @Override
        public PlanWithProperties visitAggregation(AggregationNode node, Void context)
        {
            PlanWithProperties child = accept(node.getSource());
            if (isDistinct(node)) {
                if (child.getProperties().stream().anyMatch(node.getGroupingKeys()::containsAll)) {
                    return child;
                }
            }
            if (node.getGroupingSetCount() == 1 && !node.getGroupingKeys().isEmpty()) {
                ImmutableList.Builder<Set<VariableReferenceExpression>> properties = ImmutableList.builder();
                properties.addAll(child.getProperties());
                properties.add(node.getGroupingKeys().stream().collect(toImmutableSet()));
                return new PlanWithProperties(node, properties.build());
            }
            return new PlanWithProperties(node, child.getProperties());
        }

        @Override
        public PlanWithProperties visitProject(ProjectNode node, Void context)
        {
            return planAndRecplace(node, true);
        }

        @Override
        public PlanWithProperties visitOffset(OffsetNode node, Void context)
        {
            return planAndRecplace(node, true);
        }

        @Override
        public PlanWithProperties visitSort(SortNode node, Void context)
        {
            return planAndRecplace(node, true);
        }

        private PlanWithProperties accept(PlanNode node)
        {
            PlanWithProperties result = node.accept(this, null);
            return new PlanWithProperties(
                    result.getNode().assignStatsEquivalentPlanNode(node.getStatsEquivalentPlanNode()),
                    result.getProperties());
        }

        private PlanWithProperties planAndRecplace(PlanNode node, boolean passProperties)
        {
            List<PlanWithProperties> children = node.getSources().stream().map(this::accept).collect(toImmutableList());
            PlanNode result = replaceChildren(node, children.stream().map(PlanWithProperties::getNode).collect(toImmutableList()));
            ImmutableList.Builder<Set<VariableReferenceExpression>> properties = ImmutableList.builder();
            children.stream().map(PlanWithProperties::getProperties).forEach(properties::addAll);
            if (!passProperties) {
                return new PlanWithProperties(result, ImmutableList.of());
            }
            return new PlanWithProperties(result, properties.build());
        }
    }
}
