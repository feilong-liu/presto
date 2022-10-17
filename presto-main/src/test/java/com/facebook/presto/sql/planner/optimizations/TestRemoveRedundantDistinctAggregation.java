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

import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.groupingSet;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;

public class TestRemoveRedundantDistinctAggregation
        extends BasePlanTest
{
    @Test
    public void testDistinctOverSingleGroupBy()
    {
        assertPlan("SELECT DISTINCT orderpriority, SUM(totalprice) FROM orders GROUP BY orderpriority",
                output(
                        project(
                                aggregation(
                                        ImmutableMap.of("finalsum", functionCall("sum", ImmutableList.of("paritialsum"))),
                                        FINAL,
                                        anyTree(
                                                aggregation(
                                                        ImmutableMap.of("paritialsum", functionCall("sum", ImmutableList.of("totalprice"))),
                                                        PARTIAL,
                                                        project(
                                                                ImmutableMap.of(),
                                                                tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "orderpriority", "orderpriority")))))))));
    }

    @Test
    public void testDistinctOverSingleGroupingSet()
    {
        assertPlan("SELECT DISTINCT orderpriority, SUM(totalprice) FROM orders GROUP BY GROUPING SETS ((orderpriority))",
                output(
                        project(
                                aggregation(
                                        ImmutableMap.of("finalsum", functionCall("sum", ImmutableList.of("paritialsum"))),
                                        FINAL,
                                        anyTree(
                                                aggregation(
                                                        ImmutableMap.of("paritialsum", functionCall("sum", ImmutableList.of("totalprice"))),
                                                        PARTIAL,
                                                        project(
                                                                ImmutableMap.of(),
                                                                tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "orderpriority", "orderpriority")))))))));
    }

    // Should not trigger
    @Test
    public void testDistinctOverMultipleGroupingSet()
    {
        assertPlan("SELECT DISTINCT orderpriority, orderstatus, SUM(totalprice) FROM orders GROUP BY GROUPING SETS ((orderpriority), (orderstatus))",
                output(
                        anyTree(
                                aggregation(
                                        ImmutableMap.of(),
                                        anyTree(
                                                aggregation(
                                                        ImmutableMap.of("finalsum", functionCall("sum", ImmutableList.of("paritialsum"))),
                                                        FINAL,
                                                        anyTree(
                                                                aggregation(
                                                                        ImmutableMap.of("paritialsum", functionCall("sum", ImmutableList.of("totalprice"))),
                                                                        PARTIAL,
                                                                        project(
                                                                                ImmutableMap.of(),
                                                                                groupingSet(
                                                                                        ImmutableList.of(ImmutableList.of("orderpriority"), ImmutableList.of("orderstatus")),
                                                                                        ImmutableMap.of("totalprice", "totalprice"),
                                                                                        "groupid",
                                                                                        tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "orderpriority", "orderpriority", "orderstatus", "orderstatus"))))))))))));
    }
}
