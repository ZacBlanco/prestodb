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

import com.facebook.presto.client.Column;
import com.facebook.presto.client.QueryError;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StageStats;
import com.facebook.presto.client.StatementStats;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.WarningCode;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.net.URI;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestStatementAdapters
{
    @Test
    public void testQueryResultsRoundTrip()
    {
        QueryResults queryResults = createQueryResults();

        QueryResults roundTrip = new QueryResultsAdapter().fromProtocol(new QueryResultsAdapter().toProtocol(queryResults));

        assertEquals(roundTrip.getId(), queryResults.getId());
        assertEquals(roundTrip.getInfoUri(), queryResults.getInfoUri());
        assertEquals(roundTrip.getPartialCancelUri(), queryResults.getPartialCancelUri());
        assertEquals(roundTrip.getNextUri(), queryResults.getNextUri());
        assertEquals(roundTrip.getColumns().size(), 1);
        assertEquals(roundTrip.getColumns().get(0).getName(), "count");
        assertEquals(roundTrip.getColumns().get(0).getType(), "bigint");
        assertEquals(ImmutableList.copyOf(roundTrip.getBinaryData()), ImmutableList.of("encoded-page"));
        assertEquals(roundTrip.getStats().getState(), "RUNNING");
        assertEquals(roundTrip.getStats().getRootStage().getStageId(), "0");
        assertEquals(roundTrip.getStats().getRootStage().getSubStages().get(0).getStageId(), "1");
        assertEquals(roundTrip.getError().getMessage(), "boom");
        assertEquals(roundTrip.getError().getSqlState(), "XX000");
        assertEquals(roundTrip.getError().getErrorCode(), 1234);
        assertTrue(roundTrip.getError().isRetriable());
        assertEquals(roundTrip.getWarnings().size(), 1);
        assertEquals(roundTrip.getWarnings().get(0).getWarningCode().getCode(), 55);
        assertEquals(roundTrip.getWarnings().get(0).getWarningCode().getName(), "TEST_WARNING");
        assertEquals(roundTrip.getWarnings().get(0).getMessage(), "careful");
        assertEquals(roundTrip.getUpdateType(), "INSERT");
        assertEquals(roundTrip.getUpdateCount(), Long.valueOf(7));
    }

    @Test
    public void testQueryResultsToProtocolOmitsAbsentOptionalFields()
    {
        QueryResults queryResults = new QueryResults(
                "query",
                URI.create("http://localhost/query"),
                null,
                null,
                null,
                null,
                null,
                new StatementStats("FINISHED", false, false, true, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, null, null),
                null,
                ImmutableList.of(),
                null,
                null);

        com.facebook.presto.protocol.v2.QueryResults protocol = new QueryResultsAdapter().toProtocol(queryResults);

        assertTrue(protocol.hasInfoUri());
        assertFalse(protocol.hasPartialCancelUri());
        assertFalse(protocol.hasNextUri());
        assertEquals(protocol.getColumnsCount(), 0);
        assertEquals(protocol.getBinaryDataCount(), 0);
        assertFalse(protocol.hasError());
        assertEquals(protocol.getWarningsCount(), 0);
        assertFalse(protocol.hasUpdateType());
        assertFalse(protocol.hasUpdateCount());
    }

    private static QueryResults createQueryResults()
    {
        return new QueryResults(
                "query",
                URI.create("http://localhost/query"),
                URI.create("http://localhost/cancel"),
                URI.create("http://localhost/next"),
                ImmutableList.of(new Column("count", "bigint", null)),
                null,
                ImmutableList.of("encoded-page"),
                createStatementStats(),
                new QueryError("boom", "XX000", 1234, "TEST_ERROR", "INTERNAL_ERROR", true, null, null),
                ImmutableList.of(new PrestoWarning(new WarningCode(55, "TEST_WARNING"), "careful")),
                "INSERT",
                7L);
    }

    private static StatementStats createStatementStats()
    {
        return new StatementStats(
                "RUNNING",
                false,
                false,
                true,
                4,
                10,
                1,
                2,
                7,
                11,
                12,
                13,
                14,
                15,
                16,
                17,
                18,
                19,
                20,
                21,
                new StageStats("0", "RUNNING", false, 2, 5, 1, 2, 2, 31, 32, 33, 34, ImmutableList.of(
                        new StageStats("1", "FINISHED", true, 1, 5, 0, 0, 5, 41, 42, 43, 44, ImmutableList.of()))),
                null);
    }
}
