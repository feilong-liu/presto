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
package com.facebook.presto.benchmark;

import com.facebook.presto.Session;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpcds.TpcdsConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpcds.TpcdsMetadata.TINY_SCHEMA_NAME;
import static com.google.common.io.Resources.getResource;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

public class SqlRewriteConstantVariableBenchmarks
{
    private static final List<String> TPCDS_QUERY_LIST = ImmutableList.of("q04", "q11", "q14_2", "q31", "q39_1", "q39_2", "q42", "q52", "q64", "q66", "q74", "q75", "q78");

    private SqlRewriteConstantVariableBenchmarks() {}

    private static String getQuery(String query)
    {
        String sql = query.replaceAll("\\s+;\\s+$", "")
                .replace("${database}.${schema}.", "")
                .replace("\"${database}\".\"${schema}\".\"${prefix}", "\"");
        return sql;
    }

    private static String read(String query)
    {
        try {
            String resource = format("/tpcds/%s.sql", query);
            return Resources.toString(getResource(SqlRewriteConstantVariableBenchmarks.class, resource), UTF_8);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static LocalQueryRunner createLocalQueryRunner(Map<String, String> extraSessionProperties)
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog("tpcds")
                .setSchema(TINY_SCHEMA_NAME);

        extraSessionProperties.forEach(sessionBuilder::setSystemProperty);

        Session session = sessionBuilder.build();
        LocalQueryRunner localQueryRunner = new LocalQueryRunner(session);

        // add tpcds
        localQueryRunner.createCatalog("tpcds", new TpcdsConnectorFactory(1), ImmutableMap.of());

        return localQueryRunner;
    }

    public static void main(String[] args)
    {
        for (String query : TPCDS_QUERY_LIST) {
            String sql = getQuery(read(query));
            System.out.println(format("%s Without optimization", query));
            new SqlRewriteConstantVariableBenchmark(createLocalQueryRunner(
                    ImmutableMap.of("rewrite_expression_with_constant_expression", "false")), sql).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
            System.out.println(format("%s With optimization", query));
            new SqlRewriteConstantVariableBenchmark(createLocalQueryRunner(
                    ImmutableMap.of("rewrite_expression_with_constant_expression", "true")), sql).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
        }
    }

    public static class SqlRewriteConstantVariableBenchmark
            extends AbstractSqlBenchmark
    {
        public SqlRewriteConstantVariableBenchmark(LocalQueryRunner localQueryRunner, String sql)
        {
            super(localQueryRunner, "sql_rewrite_const_variable", 0, 1, sql);
        }
    }
}
