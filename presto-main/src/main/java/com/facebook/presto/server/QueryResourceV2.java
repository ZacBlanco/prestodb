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

import com.facebook.presto.dispatcher.DispatchManager;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.protocol.v2.BasicQueryInfoList;
import com.facebook.presto.protocol.v2.adapter.BasicQueryInfoAdapter;
import com.facebook.presto.protocol.v2.adapter.QueryInfoAdapter;
import com.facebook.presto.spi.QueryId;
import com.google.common.collect.ImmutableList;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.facebook.presto.PrestoMediaTypes.APPLICATION_PRESTO_PROTOBUF;
import static com.facebook.presto.server.security.RoleType.ADMIN;
import static com.facebook.presto.server.security.RoleType.USER;
import static com.google.common.base.MoreObjects.firstNonNull;
import static jakarta.ws.rs.core.Response.Status.BAD_REQUEST;
import static jakarta.ws.rs.core.Response.Status.GONE;
import static jakarta.ws.rs.core.Response.Status.NOT_IMPLEMENTED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Protocol v2 query metadata endpoint surface. This is intentionally parallel to {@link QueryResource}
 * so the existing /v1 JSON resource remains unchanged.
 */
@Path("/v2/query")
@RolesAllowed({USER, ADMIN})
@Produces(APPLICATION_PRESTO_PROTOBUF)
public class QueryResourceV2
{
    private final boolean enabled;
    private final boolean resourceManagerEnabled;
    private final Supplier<List<BasicQueryInfo>> queriesSupplier;
    private final Function<QueryId, QueryInfo> queryInfoSupplier;
    private final BasicQueryInfoAdapter basicQueryInfoAdapter;
    private final QueryInfoAdapter queryInfoAdapter;

    @Inject
    public QueryResourceV2(
            ServerConfig serverConfig,
            DispatchManager dispatchManager,
            QueryManager queryManager)
    {
        this(
                requireNonNull(serverConfig, "serverConfig is null").isProtocolV2Enabled(),
                serverConfig.isResourceManagerEnabled(),
                requireNonNull(dispatchManager, "dispatchManager is null")::getQueries,
                requireNonNull(queryManager, "queryManager is null")::getFullQueryInfo,
                new BasicQueryInfoAdapter(),
                new QueryInfoAdapter());
    }

    QueryResourceV2(
            boolean enabled,
            boolean resourceManagerEnabled,
            Supplier<List<BasicQueryInfo>> queriesSupplier,
            Function<QueryId, QueryInfo> queryInfoSupplier,
            BasicQueryInfoAdapter basicQueryInfoAdapter,
            QueryInfoAdapter queryInfoAdapter)
    {
        this.enabled = enabled;
        this.resourceManagerEnabled = resourceManagerEnabled;
        this.queriesSupplier = requireNonNull(queriesSupplier, "queriesSupplier is null");
        this.queryInfoSupplier = requireNonNull(queryInfoSupplier, "queryInfoSupplier is null");
        this.basicQueryInfoAdapter = requireNonNull(basicQueryInfoAdapter, "basicQueryInfoAdapter is null");
        this.queryInfoAdapter = requireNonNull(queryInfoAdapter, "queryInfoAdapter is null");
    }

    @GET
    public Response getAllQueryInfo(
            @QueryParam("state") String stateFilter,
            @QueryParam("limit") Integer limitFilter)
    {
        checkEnabled();
        if (resourceManagerEnabled) {
            return Response.status(NOT_IMPLEMENTED).build();
        }

        int limit = firstNonNull(limitFilter, Integer.MAX_VALUE);
        if (limit <= 0) {
            throw new WebApplicationException(Response
                    .status(BAD_REQUEST)
                    .type(MediaType.TEXT_PLAIN)
                    .entity(format("Parameter 'limit' for getAllQueryInfo must be positive. Got %d.", limit))
                    .build());
        }

        List<BasicQueryInfo> queries = new ArrayList<>(queriesSupplier.get());
        if (stateFilter != null) {
            QueryState expectedState = QueryState.valueOf(stateFilter.toUpperCase(Locale.ENGLISH));
            queries.removeIf(item -> item.getState() != expectedState);
        }
        if (limit < queries.size()) {
            queries.sort(QueryResource.QUERIES_ORDERING);
        }
        else {
            limit = queries.size();
        }

        BasicQueryInfoList.Builder builder = BasicQueryInfoList.newBuilder();
        ImmutableList.copyOf(queries.subList(0, limit)).stream()
                .map(basicQueryInfoAdapter::toProtocol)
                .forEach(builder::addQueries);
        return Response.ok(builder.build()).build();
    }

    @GET
    @Path("{queryId}")
    public Response getQueryInfo(@PathParam("queryId") QueryId queryId)
    {
        checkEnabled();
        requireNonNull(queryId, "queryId is null");
        if (resourceManagerEnabled) {
            return Response.status(NOT_IMPLEMENTED).build();
        }

        try {
            QueryInfo queryInfo = queryInfoSupplier.apply(queryId);
            return Response.ok(queryInfoAdapter.toProtocol(queryInfo)).build();
        }
        catch (NoSuchElementException e) {
            return Response.status(GONE).build();
        }
    }

    private void checkEnabled()
    {
        if (!enabled) {
            throw new NotFoundException();
        }
    }
}
