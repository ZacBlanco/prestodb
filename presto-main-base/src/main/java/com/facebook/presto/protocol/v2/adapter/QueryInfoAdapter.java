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

import com.facebook.presto.execution.QueryInfo;

import static java.util.Objects.requireNonNull;

public class QueryInfoAdapter
        implements ProtocolAdapter<QueryInfo, com.facebook.presto.protocol.v2.QueryInfo>
{
    private final QueryStatsAdapter queryStatsAdapter;
    private final StageInfoAdapter stageInfoAdapter;

    public QueryInfoAdapter()
    {
        this(new QueryStatsAdapter(), new StageInfoAdapter());
    }

    public QueryInfoAdapter(QueryStatsAdapter queryStatsAdapter, StageInfoAdapter stageInfoAdapter)
    {
        this.queryStatsAdapter = requireNonNull(queryStatsAdapter, "queryStatsAdapter is null");
        this.stageInfoAdapter = requireNonNull(stageInfoAdapter, "stageInfoAdapter is null");
    }

    @Override
    public com.facebook.presto.protocol.v2.QueryInfo toProtocol(QueryInfo value)
    {
        requireNonNull(value, "value is null");
        com.facebook.presto.protocol.v2.QueryInfo.Builder builder = com.facebook.presto.protocol.v2.QueryInfo.newBuilder()
                .setQueryId(value.getQueryId().toString())
                .setState(value.getState().name())
                .setQuery(value.getQuery())
                .setQueryStats(queryStatsAdapter.toProtocol(value.getQueryStats()));
        value.getOutputStage()
                .map(stageInfoAdapter::toProtocol)
                .ifPresent(builder::setOutputStage);
        return builder.build();
    }

    @Override
    public QueryInfo fromProtocol(com.facebook.presto.protocol.v2.QueryInfo value)
    {
        requireNonNull(value, "value is null");
        throw new UnsupportedOperationException("QueryInfo protobuf is currently a server-to-client projection and cannot reconstruct the full Java DTO");
    }
}
