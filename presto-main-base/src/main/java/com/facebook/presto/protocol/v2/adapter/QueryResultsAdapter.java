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

import com.facebook.presto.client.QueryResults;
import com.google.common.collect.ImmutableList;

import java.net.URI;

import static java.util.Objects.requireNonNull;

public class QueryResultsAdapter
        implements ProtocolAdapter<QueryResults, com.facebook.presto.protocol.v2.QueryResults>
{
    private final ColumnAdapter columnAdapter;
    private final StatementStatsAdapter statementStatsAdapter;
    private final QueryErrorAdapter queryErrorAdapter;
    private final ClientWarningAdapter clientWarningAdapter;

    public QueryResultsAdapter()
    {
        this(new ColumnAdapter(), new StatementStatsAdapter(), new QueryErrorAdapter(), new ClientWarningAdapter());
    }

    public QueryResultsAdapter(
            ColumnAdapter columnAdapter,
            StatementStatsAdapter statementStatsAdapter,
            QueryErrorAdapter queryErrorAdapter,
            ClientWarningAdapter clientWarningAdapter)
    {
        this.columnAdapter = requireNonNull(columnAdapter, "columnAdapter is null");
        this.statementStatsAdapter = requireNonNull(statementStatsAdapter, "statementStatsAdapter is null");
        this.queryErrorAdapter = requireNonNull(queryErrorAdapter, "queryErrorAdapter is null");
        this.clientWarningAdapter = requireNonNull(clientWarningAdapter, "clientWarningAdapter is null");
    }

    @Override
    public com.facebook.presto.protocol.v2.QueryResults toProtocol(QueryResults value)
    {
        requireNonNull(value, "value is null");
        com.facebook.presto.protocol.v2.QueryResults.Builder builder = com.facebook.presto.protocol.v2.QueryResults.newBuilder()
                .setId(value.getId())
                .setStats(statementStatsAdapter.toProtocol(value.getStats()));
        if (value.getInfoUri() != null) {
            builder.setInfoUri(value.getInfoUri().toString());
        }
        if (value.getPartialCancelUri() != null) {
            builder.setPartialCancelUri(value.getPartialCancelUri().toString());
        }
        if (value.getNextUri() != null) {
            builder.setNextUri(value.getNextUri().toString());
        }
        if (value.getColumns() != null) {
            value.getColumns().stream()
                    .map(columnAdapter::toProtocol)
                    .forEach(builder::addColumns);
        }
        if (value.getBinaryData() != null) {
            ImmutableList.copyOf(value.getBinaryData()).forEach(builder::addBinaryData);
        }
        if (value.getError() != null) {
            builder.setError(queryErrorAdapter.toProtocol(value.getError()));
        }
        value.getWarnings().stream()
                .map(clientWarningAdapter::toProtocol)
                .forEach(builder::addWarnings);
        if (value.getUpdateType() != null) {
            builder.setUpdateType(value.getUpdateType());
        }
        if (value.getUpdateCount() != null) {
            builder.setUpdateCount(value.getUpdateCount());
        }
        return builder.build();
    }

    @Override
    public QueryResults fromProtocol(com.facebook.presto.protocol.v2.QueryResults value)
    {
        requireNonNull(value, "value is null");
        return new QueryResults(
                value.getId(),
                value.hasInfoUri() ? URI.create(value.getInfoUri()) : null,
                value.hasPartialCancelUri() ? URI.create(value.getPartialCancelUri()) : null,
                value.hasNextUri() ? URI.create(value.getNextUri()) : null,
                value.getColumnsList().stream()
                        .map(columnAdapter::fromProtocol)
                        .collect(ImmutableList.toImmutableList()),
                null,
                value.getBinaryDataCount() == 0 ? null : value.getBinaryDataList(),
                statementStatsAdapter.fromProtocol(value.getStats()),
                value.hasError() ? queryErrorAdapter.fromProtocol(value.getError()) : null,
                value.getWarningsList().stream()
                        .map(clientWarningAdapter::fromProtocol)
                        .collect(ImmutableList.toImmutableList()),
                value.hasUpdateType() ? value.getUpdateType() : null,
                value.hasUpdateCount() ? value.getUpdateCount() : null);
    }
}
