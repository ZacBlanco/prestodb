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
package com.facebook.presto.server;

import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.protocol.v2.BasicQueryInfoList;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static jakarta.ws.rs.core.Response.Status.BAD_REQUEST;
import static jakarta.ws.rs.core.Response.Status.GONE;
import static jakarta.ws.rs.core.Response.Status.NOT_IMPLEMENTED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;

public class TestQueryResourceV2
{
    @Test
    public void testGetAllQueryInfo()
    {
        QueryResourceV2 resource = createResource(
                true,
                false,
                ImmutableList.of(
                        createBasicQueryInfo("query_1", RUNNING, "SELECT 1"),
                        createBasicQueryInfo("query_2", FAILED, "SELECT 2")),
                createQueryInfo("query_1", "SELECT 1"));

        Response response = resource.getAllQueryInfo(null, null);

        assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
        BasicQueryInfoList queryInfoList = (BasicQueryInfoList) response.getEntity();
        assertEquals(queryInfoList.getQueriesCount(), 2);
        assertEquals(queryInfoList.getQueries(0).getQueryId(), "query_1");
        assertEquals(queryInfoList.getQueries(0).getState(), RUNNING.name());
        assertEquals(queryInfoList.getQueries(0).getQuery(), "SELECT 1");
        assertEquals(queryInfoList.getQueries(1).getQueryId(), "query_2");
    }

    @Test
    public void testGetAllQueryInfoWithStateAndLimitFilters()
    {
        QueryResourceV2 resource = createResource(
                true,
                false,
                ImmutableList.of(
                        createBasicQueryInfo("query_1", RUNNING, "SELECT 1"),
                        createBasicQueryInfo("query_2", RUNNING, "SELECT 2"),
                        createBasicQueryInfo("query_3", FAILED, "SELECT 3")),
                createQueryInfo("query_1", "SELECT 1"));

        Response response = resource.getAllQueryInfo("running", 1);

        assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
        BasicQueryInfoList queryInfoList = (BasicQueryInfoList) response.getEntity();
        assertEquals(queryInfoList.getQueriesCount(), 1);
        assertEquals(queryInfoList.getQueries(0).getQueryId(), "query_1");
        assertEquals(queryInfoList.getQueries(0).getState(), RUNNING.name());
    }

    @Test
    public void testGetQueryInfo()
    {
        QueryInfo queryInfo = createQueryInfo("query_1", "SELECT 1");
        QueryResourceV2 resource = createResource(true, false, ImmutableList.of(), queryInfo);

        Response response = resource.getQueryInfo(new QueryId("query_1"));

        assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
        com.facebook.presto.protocol.v2.QueryInfo protocol = (com.facebook.presto.protocol.v2.QueryInfo) response.getEntity();
        assertEquals(protocol.getQueryId(), "query_1");
        assertEquals(protocol.getState(), RUNNING.name());
        assertEquals(protocol.getQuery(), "SELECT 1");
        assertFalse(protocol.hasOutputStage());
    }

    @Test
    public void testMissingQueryReturnsGone()
    {
        QueryResourceV2 resource = new QueryResourceV2(
                true,
                false,
                ImmutableList::of,
                queryId -> {
                    throw new NoSuchElementException();
                },
                new com.facebook.presto.protocol.v2.adapter.BasicQueryInfoAdapter(),
                new com.facebook.presto.protocol.v2.adapter.QueryInfoAdapter());

        assertEquals(resource.getQueryInfo(new QueryId("missing")).getStatus(), GONE.getStatusCode());
    }

    @Test
    public void testDisabledResourceIsNotFound()
    {
        QueryResourceV2 resource = createResource(false, false, ImmutableList.of(), createQueryInfo("query_1", "SELECT 1"));

        assertThrows(NotFoundException.class, () -> resource.getAllQueryInfo(null, null));
        assertThrows(NotFoundException.class, () -> resource.getQueryInfo(new QueryId("query_1")));
    }

    @Test
    public void testInvalidLimitReturnsBadRequest()
    {
        QueryResourceV2 resource = createResource(true, false, ImmutableList.of(), createQueryInfo("query_1", "SELECT 1"));

        WebApplicationException exception = expectThrows(WebApplicationException.class, () -> resource.getAllQueryInfo(null, 0));
        assertEquals(exception.getResponse().getStatus(), BAD_REQUEST.getStatusCode());
    }

    @Test
    public void testResourceManagerModeIsNotImplementedForV2QueryProxying()
    {
        QueryResourceV2 resource = createResource(true, true, ImmutableList.of(), createQueryInfo("query_1", "SELECT 1"));

        assertEquals(resource.getAllQueryInfo(null, null).getStatus(), NOT_IMPLEMENTED.getStatusCode());
        assertEquals(resource.getQueryInfo(new QueryId("query_1")).getStatus(), NOT_IMPLEMENTED.getStatusCode());
    }

    private static QueryResourceV2 createResource(boolean enabled, boolean resourceManagerEnabled, List<BasicQueryInfo> queries, QueryInfo queryInfo)
    {
        return new QueryResourceV2(
                enabled,
                resourceManagerEnabled,
                () -> queries,
                queryId -> queryInfo,
                new com.facebook.presto.protocol.v2.adapter.BasicQueryInfoAdapter(),
                new com.facebook.presto.protocol.v2.adapter.QueryInfoAdapter());
    }

    private static BasicQueryInfo createBasicQueryInfo(String queryId, com.facebook.presto.execution.QueryState state, String query)
    {
        return new BasicQueryInfo(
                new QueryId(queryId),
                TEST_SESSION.toSessionRepresentation(),
                Optional.empty(),
                state,
                new MemoryPoolId("general"),
                false,
                URI.create("http://localhost/query/" + queryId),
                query,
                new BasicQueryStats(QueryStats.immediateFailureQueryStats()),
                null,
                Optional.empty(),
                ImmutableList.of(),
                Optional.empty());
    }

    private static QueryInfo createQueryInfo(String queryId, String query)
    {
        return new QueryInfo(
                new QueryId(queryId),
                TEST_SESSION.toSessionRepresentation(),
                RUNNING,
                new MemoryPoolId("general"),
                false,
                URI.create("http://localhost/query/" + queryId),
                ImmutableList.of("col"),
                query,
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
                Optional.empty(),
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

    private static <T extends Throwable> T expectThrows(Class<T> expectedType, Runnable runnable)
    {
        try {
            runnable.run();
        }
        catch (Throwable actual) {
            if (expectedType.isInstance(actual)) {
                return expectedType.cast(actual);
            }
            if (actual instanceof RuntimeException) {
                throw (RuntimeException) actual;
            }
            if (actual instanceof Error) {
                throw (Error) actual;
            }
            throw new RuntimeException(actual);
        }
        throw new AssertionError("Expected " + expectedType.getName() + " to be thrown");
    }
}
