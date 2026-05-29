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
package com.facebook.presto.protocol.v2.adapter;

import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.execution.StageExecutionInfo;
import com.facebook.presto.execution.StageExecutionState;
import com.facebook.presto.execution.StageExecutionStats;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TestQueryInfoAdapters
{
    @Test
    public void testBasicQueryInfoToProtocol()
    {
        BasicQueryInfo queryInfo = new BasicQueryInfo(createQueryInfo(Optional.empty()));

        com.facebook.presto.protocol.v2.BasicQueryInfo protocol = new BasicQueryInfoAdapter().toProtocol(queryInfo);

        assertEquals(protocol.getQueryId(), queryInfo.getQueryId().toString());
        assertEquals(protocol.getState(), queryInfo.getState().name());
        assertEquals(protocol.getQuery(), queryInfo.getQuery());
        assertEquals(protocol.getQueryStats().getCreateTimeMillis(), queryInfo.getQueryStats().getCreateTimeInMillis());
        assertEquals(protocol.getQueryStats().getEndTimeMillis(), queryInfo.getQueryStats().getEndTimeInMillis());
        assertEquals(protocol.getQueryStats().getElapsedTimeMillis(), queryInfo.getQueryStats().getElapsedTime().roundTo(MILLISECONDS));
    }

    @Test
    public void testQueryInfoToProtocol()
    {
        StageInfo outputStage = createStageInfo();
        QueryInfo queryInfo = createQueryInfo(Optional.of(outputStage));

        com.facebook.presto.protocol.v2.QueryInfo protocol = new QueryInfoAdapter().toProtocol(queryInfo);

        assertEquals(protocol.getQueryId(), queryInfo.getQueryId().toString());
        assertEquals(protocol.getState(), queryInfo.getState().name());
        assertEquals(protocol.getQuery(), queryInfo.getQuery());
        assertEquals(protocol.getQueryStats().getCreateTimeMillis(), queryInfo.getQueryStats().getCreateTimeInMillis());
        assertEquals(protocol.getQueryStats().getEndTimeMillis(), queryInfo.getQueryStats().getEndTimeInMillis());
        assertEquals(protocol.getQueryStats().getElapsedTimeMillis(), queryInfo.getQueryStats().getElapsedTime().roundTo(MILLISECONDS));
        assertEquals(protocol.getQueryStats().getTotalCpuTimeMillis(), queryInfo.getQueryStats().getTotalCpuTime().roundTo(MILLISECONDS));
        assertTrue(protocol.hasOutputStage());
        assertEquals(protocol.getOutputStage().getStageId(), outputStage.getStageId().toString());
        assertEquals(protocol.getOutputStage().getState(), outputStage.getLatestAttemptExecutionInfo().getState().name());
        assertEquals(protocol.getOutputStage().getSubStagesCount(), 1);
        assertEquals(protocol.getOutputStage().getSubStages(0).getStageId(), outputStage.getSubStages().get(0).getStageId().toString());
        assertEquals(protocol.getOutputStage().getSubStages(0).getState(), outputStage.getSubStages().get(0).getLatestAttemptExecutionInfo().getState().name());
    }

    @Test
    public void testQueryInfoToProtocolWithoutOutputStage()
    {
        QueryInfo queryInfo = createQueryInfo(Optional.empty());

        com.facebook.presto.protocol.v2.QueryInfo protocol = new QueryInfoAdapter().toProtocol(queryInfo);

        assertFalse(protocol.hasOutputStage());
    }

    @Test
    public void testQueryAdaptersAreServerToClientProjectionOnly()
    {
        assertThrows(UnsupportedOperationException.class, () -> new BasicQueryInfoAdapter().fromProtocol(com.facebook.presto.protocol.v2.BasicQueryInfo.getDefaultInstance()));
        assertThrows(UnsupportedOperationException.class, () -> new QueryInfoAdapter().fromProtocol(com.facebook.presto.protocol.v2.QueryInfo.getDefaultInstance()));
        assertThrows(UnsupportedOperationException.class, () -> new StageInfoAdapter().fromProtocol(com.facebook.presto.protocol.v2.StageInfo.getDefaultInstance()));
        assertThrows(UnsupportedOperationException.class, () -> new StageExecutionInfoAdapter().fromProtocol(com.facebook.presto.protocol.v2.StageExecutionInfo.getDefaultInstance()));
    }

    private static QueryInfo createQueryInfo(Optional<StageInfo> outputStage)
    {
        return new QueryInfo(
                new QueryId("query"),
                TEST_SESSION.toSessionRepresentation(),
                RUNNING,
                new MemoryPoolId("general"),
                false,
                URI.create("http://localhost/query"),
                ImmutableList.of("col"),
                "SELECT 1",
                Optional.empty(),
                Optional.empty(),
                QueryStats.immediateFailureQueryStats(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                Optional.empty(),
                false,
                null,
                outputStage,
                null,
                null,
                ImmutableList.of(),
                ImmutableSet.of(),
                Optional.empty(),
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                StatsAndCosts.empty(),
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableSet.of(),
                ImmutableSet.of(),
                ImmutableSet.of(),
                ImmutableList.of(),
                ImmutableMap.of(),
                Optional.empty());
    }

    private static StageInfo createStageInfo()
    {
        StageInfo subStage = new StageInfo(
                StageId.valueOf("query.1"),
                URI.create("http://localhost/stage/1"),
                Optional.empty(),
                new StageExecutionInfo(StageExecutionState.FINISHED, StageExecutionStats.zero(1), ImmutableList.of(), Optional.empty()),
                ImmutableList.of(),
                ImmutableList.of(),
                false);
        return new StageInfo(
                StageId.valueOf("query.0"),
                URI.create("http://localhost/stage/0"),
                Optional.empty(),
                new StageExecutionInfo(StageExecutionState.RUNNING, StageExecutionStats.zero(0), ImmutableList.of(), Optional.empty()),
                ImmutableList.of(),
                ImmutableList.of(subStage),
                false);
    }
}
