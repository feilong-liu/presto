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

import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.unnest;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestCrossJoinWithOrFilterToInnerJoin
        extends BaseRuleTest
{
    @Test
    public void testTriggerForBigInt()
    {
        tester().assertThat(new CrossJoinWithOrFilterToInnerJoin(getMetadata().getFunctionAndTypeManager()))
                .on(p ->
                {
                    p.variable("left_k1", BIGINT);
                    p.variable("left_k2", BIGINT);
                    p.variable("right_k1", BIGINT);
                    p.variable("right_k2", BIGINT);
                    return p.filter(
                            p.rowExpression("left_k1 = right_k1 or left_k2 = right_k2"),
                            p.join(JoinNode.Type.INNER,
                                    p.values(p.variable("left_k1"), p.variable("left_k2")),
                                    p.values(p.variable("right_k1"), p.variable("right_k2"))));
                })
                .matches(
                        project(
                                filter(
                                        "case left_idx when 1 then left_k1 = right_k1 when 2 then not(coalesce(left_k1 = right_k1, false)) and left_k2 = right_k2 else null end",
                                        join(
                                                JoinNode.Type.INNER,
                                                ImmutableList.of(equiJoinClause("expr", "expr_2"), equiJoinClause("left_idx", "right_idx")),
                                                project(
                                                        ImmutableMap.of("expr", expression("case left_idx when 1 then left_k1 when 2 then left_k2 else null end")),
                                                        unnest(
                                                                ImmutableMap.of("left_array", ImmutableList.of("left_idx")),
                                                                project(
                                                                        ImmutableMap.of("left_array", expression("array[1, 2]")),
                                                                        values("left_k1", "left_k2")))),
                                                project(
                                                        ImmutableMap.of("expr_2", expression("case right_idx when 1 then right_k1 when 2 then right_k2 else null end")),
                                                        unnest(
                                                                ImmutableMap.of("right_array", ImmutableList.of("right_idx")),
                                                                project(
                                                                        ImmutableMap.of("right_array", expression("array[1, 2]")),
                                                                        values("right_k1", "right_k2"))))))));
    }

    @Test
    public void testNotTriggerForDouble()
    {
        tester().assertThat(new CrossJoinWithOrFilterToInnerJoin(getMetadata().getFunctionAndTypeManager()))
                .on(p ->
                {
                    p.variable("left_k1", DOUBLE);
                    p.variable("left_k2", DOUBLE);
                    p.variable("right_k1", DOUBLE);
                    p.variable("right_k2", DOUBLE);
                    return p.filter(
                            p.rowExpression("left_k1 = right_k1 or left_k2 = right_k2"),
                            p.join(JoinNode.Type.INNER,
                                    p.values(p.variable("left_k1", DOUBLE), p.variable("left_k2", DOUBLE)),
                                    p.values(p.variable("right_k1", DOUBLE), p.variable("right_k2", DOUBLE))));
                }).doesNotFire();
    }

    @Test
    public void testOrWithCast()
    {
        tester().assertThat(new CrossJoinWithOrFilterToInnerJoin(getMetadata().getFunctionAndTypeManager()))
                .on(p ->
                {
                    p.variable("left_k1", BIGINT);
                    p.variable("left_k2", BIGINT);
                    p.variable("right_k1", BIGINT);
                    p.variable("right_k2", VARCHAR);
                    return p.filter(
                            p.rowExpression("left_k1 = right_k1 or left_k2 = CAST(right_k2 AS BIGINT)"),
                            p.join(JoinNode.Type.INNER,
                                    p.values(p.variable("left_k1"), p.variable("left_k2")),
                                    p.values(p.variable("right_k1"), p.variable("right_k2", VARCHAR))));
                })
                .matches(
                        project(
                                filter(
                                        "case left_idx when 1 then left_k1 = right_k1 when 2 then not(coalesce(left_k1 = right_k1, false)) and left_k2 = cast_0 else null end",
                                        join(
                                                JoinNode.Type.INNER,
                                                ImmutableList.of(equiJoinClause("expr", "expr_2"), equiJoinClause("left_idx", "right_idx")),
                                                project(
                                                        ImmutableMap.of("expr", expression("case left_idx when 1 then left_k1 when 2 then left_k2 else null end")),
                                                        unnest(
                                                                ImmutableMap.of("left_array", ImmutableList.of("left_idx")),
                                                                project(
                                                                        ImmutableMap.of("left_array", expression("array[1, 2]")),
                                                                        values("left_k1", "left_k2")))),
                                                project(
                                                        ImmutableMap.of("expr_2", expression("case right_idx when 1 then right_k1 when 2 then cast_0 else null end")),
                                                        unnest(
                                                                ImmutableMap.of("right_array", ImmutableList.of("right_idx")),
                                                                project(
                                                                        ImmutableMap.of("right_array", expression("array[1, 2]")),
                                                                        project(
                                                                                ImmutableMap.of("cast_0", expression("CAST(right_k2 AS bigint)")),
                                                                                values("right_k1", "right_k2")))))))));
    }

    @Test
    public void testOrWithCoalesce()
    {
        tester().assertThat(new CrossJoinWithOrFilterToInnerJoin(getMetadata().getFunctionAndTypeManager()))
                .on(p ->
                {
                    p.variable("left_k1", BIGINT);
                    p.variable("right_k1", BIGINT);
                    p.variable("right_k2", VARCHAR);
                    p.variable("right_k3", VARCHAR);
                    return p.filter(
                            p.rowExpression("left_k1 = right_k1 or CAST(left_k1 AS VARCHAR) = COALESCE(right_k2, right_k3)"),
                            p.join(JoinNode.Type.INNER,
                                    p.values(p.variable("left_k1")),
                                    p.values(p.variable("right_k1"), p.variable("right_k2", VARCHAR), p.variable("right_k3", VARCHAR))));
                })
                .matches(
                        project(
                                filter(
                                        "case left_idx when 1 then left_k1 = right_k1 when 2 then not(coalesce(left_k1 = right_k1, false)) and cast_5 = expr else null end",
                                        join(
                                                JoinNode.Type.INNER,
                                                ImmutableList.of(equiJoinClause("expr_3", "expr_2"), equiJoinClause("left_idx", "right_idx")),
                                                project(
                                                        ImmutableMap.of("expr_3", expression("case left_idx when 1 then cast_0 when 2 then cast_1 else null end")),
                                                        unnest(
                                                                ImmutableMap.of("left_array", ImmutableList.of("left_idx")),
                                                                project(
                                                                        ImmutableMap.of("left_array", expression("array[1, 2]"), "cast_0", expression("CAST(left_k1 AS varchar)"), "cast_1", expression("CAST(cast_5 AS varchar)")),
                                                                        project(
                                                                                ImmutableMap.of("cast_5", expression("cast(left_k1 AS varchar)")),
                                                                                values("left_k1"))))),
                                                project(
                                                        ImmutableMap.of("expr_2", expression("case right_idx when 1 then cast_3 when 2 then cast_4 else null end")),
                                                        unnest(
                                                                ImmutableMap.of("right_array", ImmutableList.of("right_idx")),
                                                                project(
                                                                        ImmutableMap.of("right_array", expression("array[1, 2]"), "cast_4", expression("cast(expr as varchar)"), "cast_3", expression("cast(right_k1 as varchar)")),
                                                                        project(
                                                                                ImmutableMap.of("expr", expression("COALESCE(right_k2, right_k3)")),
                                                                                values("right_k1", "right_k2", "right_k3")))))))));
    }

    @Test
    public void testConditionWithAnd()
    {
        tester().assertThat(new CrossJoinWithOrFilterToInnerJoin(getMetadata().getFunctionAndTypeManager()))
                .on(p ->
                {
                    p.variable("left_k1", BIGINT);
                    p.variable("left_k2", BIGINT);
                    p.variable("right_k1", BIGINT);
                    p.variable("right_k2", BIGINT);
                    return p.filter(
                            p.rowExpression("(left_k1 = right_k1 or left_k2 = right_k2) and left_k1+right_k2 > 10"),
                            p.join(JoinNode.Type.INNER,
                                    p.values(p.variable("left_k1"), p.variable("left_k2")),
                                    p.values(p.variable("right_k1"), p.variable("right_k2"))));
                })
                .matches(
                        project(
                                filter(
                                        "left_k1+right_k2 > 10",
                                        filter(
                                                "case left_idx when 1 then left_k1 = right_k1 when 2 then not(coalesce(left_k1 = right_k1, false)) and left_k2 = right_k2 else null end",
                                                join(
                                                        JoinNode.Type.INNER,
                                                        ImmutableList.of(equiJoinClause("expr", "expr_2"), equiJoinClause("left_idx", "right_idx")),
                                                        project(
                                                                ImmutableMap.of("expr", expression("case left_idx when 1 then left_k1 when 2 then left_k2 else null end")),
                                                                unnest(
                                                                        ImmutableMap.of("left_array", ImmutableList.of("left_idx")),
                                                                        project(
                                                                                ImmutableMap.of("left_array", expression("array[1, 2]")),
                                                                                values("left_k1", "left_k2")))),
                                                        project(
                                                                ImmutableMap.of("expr_2", expression("case right_idx when 1 then right_k1 when 2 then right_k2 else null end")),
                                                                unnest(
                                                                        ImmutableMap.of("right_array", ImmutableList.of("right_idx")),
                                                                        project(
                                                                                ImmutableMap.of("right_array", expression("array[1, 2]")),
                                                                                values("right_k1", "right_k2")))))))));
    }

    @Test
    public void testNonMatchingCondition()
    {
        tester().assertThat(new CrossJoinWithOrFilterToInnerJoin(getMetadata().getFunctionAndTypeManager()))
                .on(p ->
                {
                    p.variable("left_k1", BIGINT);
                    p.variable("left_k2", BIGINT);
                    p.variable("right_k1", BIGINT);
                    p.variable("right_k2", BIGINT);
                    return p.filter(
                            p.rowExpression("(left_k1 = right_k1 or left_k2 = right_k2) or left_k1+right_k2 > 10"),
                            p.join(JoinNode.Type.INNER,
                                    p.values(p.variable("left_k1"), p.variable("left_k2")),
                                    p.values(p.variable("right_k1"), p.variable("right_k2"))));
                })
                .doesNotFire();
    }

    @Test
    public void testNonMatchingCondition2()
    {
        tester().assertThat(new CrossJoinWithOrFilterToInnerJoin(getMetadata().getFunctionAndTypeManager()))
                .on(p ->
                {
                    p.variable("left_k1", BIGINT);
                    p.variable("left_k2", BIGINT);
                    p.variable("right_k1", BIGINT);
                    p.variable("right_k2", BIGINT);
                    return p.filter(
                            p.rowExpression("left_k1 = right_k1 or left_k2 > right_k2"),
                            p.join(JoinNode.Type.INNER,
                                    p.values(p.variable("left_k1"), p.variable("left_k2")),
                                    p.values(p.variable("right_k1"), p.variable("right_k2"))));
                })
                .doesNotFire();
    }
}
