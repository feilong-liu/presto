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
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.JOIN_PREFILTER_BUILD_SIDE;
import static com.facebook.presto.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;

public class TestJoinPrefilter
        extends BasePlanTest
{
    private Session getSessionAlwaysEnabled()
    {
        return Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(JOIN_PREFILTER_BUILD_SIDE, "true")
                .setSystemProperty(JOIN_REORDERING_STRATEGY, "NONE")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                .build();
    }

    @Test
    public void testInnerJoin()
    {
        assertPlan("SELECT * FROM orders JOIN lineitem ON orders.orderkey = lineitem.orderkey",
                getSessionAlwaysEnabled(),
                anyTree(
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("leftCol", "rightCol")),
                                anyTree(
                                        tableScan("orders", ImmutableMap.of("leftCol", "orderkey"))),
                                anyTree(
                                        filter(
                                                semiJoin(
                                                        "rightCol",
                                                        "leftCol",
                                                        "semiOutput",
                                                        anyTree(
                                                                tableScan("lineitem", ImmutableMap.of("rightCol", "orderkey"))),
                                                        anyTree(
                                                                aggregation(
                                                                        ImmutableMap.of(),
                                                                        tableScan("orders", ImmutableMap.of("leftCol", "orderkey"))))))))),
                false);
    }
}
