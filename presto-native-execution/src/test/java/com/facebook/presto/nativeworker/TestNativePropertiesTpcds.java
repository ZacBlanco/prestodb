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
package com.facebook.presto.nativeworker;

import com.facebook.presto.Session;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_EXECUTION_TIME;
import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_RUN_TIME;

public class TestNativePropertiesTpcds
        extends AbstractTestNativeTpcdsQueries
{
    private String testName = "";

    @BeforeMethod(alwaysRun = true)
    public void setCustomTestcaseName(Method method, Object[] testData)
    {
        this.testName = method.getName();
        if (testData != null && testData.length > 0 && testData[0] instanceof String) {
            this.testName = method.getName() + "_" + testData[0];
        }
    }

    @Override
    public String getTestName()
    {
        return testName;
    }

    @DataProvider(name = "sessionProvider")
    public Object[][] sessionConfigProvider()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        Map<String, List<Object>> permutations = ImmutableMap.<String, List<Object>>builder()
                // For each key, provide a list of (<default value>, <tested value>)
                .put("spatial_join", ImmutableList.of(true, false))
                .put("spatial_partitioning_table_name", ImmutableList.of(true, false))
                .put("optimize_metadata_queries", ImmutableList.of(true, false))
                .put("optimize_metadata_queries_ignore_stats", ImmutableList.of(false, true))
                .put("optimize_metadata_queries_call_threshold", ImmutableList.of(100, 100))
                .put("grouped_execution", ImmutableList.of(true, false))
                .put("recoverable_grouped_execution", ImmutableList.of(false, true))
                .put("prefer_merge_join_for_sorted_inputs", ImmutableList.of(false, true))
                .put("distributed_index_join", ImmutableList.of(false, true))
                .put("hash_partition_count", ImmutableList.of(100, 1000))
                .put("partitioning_provider_catalog", ImmutableList.of("hive", "hive"))
                .put("exchange_materialization_strategy", ImmutableList.of("NONE", "NONE"))
                .put("use_stream_exchange_for_mark_distinct", ImmutableList.of(false, true))
                .put("redistribute_writes", ImmutableList.of(true, false))
                .put("push_table_write_through_union", ImmutableList.of(true, false))
                .put("plan_with_table_node_partitioning", ImmutableList.of(true, false))
                .put("reorder_joins", ImmutableList.of(true, false))
                .put("join_reordering_strategy", ImmutableList.of(FeaturesConfig.JoinReorderingStrategy.AUTOMATIC.name(), FeaturesConfig.JoinReorderingStrategy.NONE.name()))
                .put("partial_merge_pushdown_strategy", ImmutableList.of("NONE", "PUSH_THROUGH_LOW_MEMORY_OPERATORS"))
                .put("max_reordered_joins", ImmutableList.of(9, 1000))
                .put("fast_inequality_joins", ImmutableList.of(true, false))
                .put("optimize_mixed_distinct_aggregations", ImmutableList.of(false, true))
                .put("legacy_row_field_ordinal_access", ImmutableList.of(false, true))
                .put("do_not_use_legacy_map_subscript", ImmutableList.of(false, true))
                .put("iterative_optimizer_enabled", ImmutableList.of(true, false))
                .put("iterative_optimizer_timeout", ImmutableList.of("3m", "20s"))
                .put("query_analyzer_timeout", ImmutableList.of("3m", "20s"))
                .put("runtime_optimizer_enabled", ImmutableList.of(false, true))
                .put("enable_intermediate_aggregations", ImmutableList.of(false, true))
                .put("push_aggregation_through_join", ImmutableList.of(true, false))
                .put("push_partial_aggregation_through_join", ImmutableList.of(false, true))
                .put("distributed_sort", ImmutableList.of(true, false))
                .build();
        Session.SessionBuilder defaultSessionBuilder = Session.builder(queryRunner.getDefaultSession())
                .setSchema("tpcds")
                .setSystemProperty(QUERY_MAX_RUN_TIME, "2m")
                .setSystemProperty(QUERY_MAX_EXECUTION_TIME, "2m");
        permutations.forEach((property, values) -> {
            defaultSessionBuilder.setSystemProperty(property, values.get(0).toString());
        });
        Session defaultSession = defaultSessionBuilder.build();

        List<Object[]> combinations = permutations.entrySet().stream()
                // for every key, create a session with the switch flipped
                .map(entry -> {
                    Session.SessionBuilder session = Session.builder(defaultSession);
                    session.setSystemProperty(entry.getKey(), entry.getValue().get(1).toString());
                    String propChange = String.format("%s->%s", entry.getKey(), entry.getValue().get(1).toString());
                    return new Object[] {propChange, session.build()};
                })
                .collect(Collectors.toList());
        Object[][] matrix = new Object[combinations.size()][];
        matrix = combinations.toArray(matrix);
        return matrix;
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.createNativeQueryRunner(true, "PARQUET");
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        this.storageFormat = "PARQUET";
        return PrestoNativeQueryRunnerUtils.createJavaQueryRunner("PARQUET");
    }
}
