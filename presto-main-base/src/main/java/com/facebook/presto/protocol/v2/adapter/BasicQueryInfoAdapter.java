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

import com.facebook.presto.server.BasicQueryInfo;

import static java.util.Objects.requireNonNull;

public class BasicQueryInfoAdapter
        implements ProtocolAdapter<BasicQueryInfo, com.facebook.presto.protocol.v2.BasicQueryInfo>
{
    private final BasicQueryStatsAdapter basicQueryStatsAdapter;

    public BasicQueryInfoAdapter()
    {
        this(new BasicQueryStatsAdapter());
    }

    public BasicQueryInfoAdapter(BasicQueryStatsAdapter basicQueryStatsAdapter)
    {
        this.basicQueryStatsAdapter = requireNonNull(basicQueryStatsAdapter, "basicQueryStatsAdapter is null");
    }

    @Override
    public com.facebook.presto.protocol.v2.BasicQueryInfo toProtocol(BasicQueryInfo value)
    {
        requireNonNull(value, "value is null");
        return com.facebook.presto.protocol.v2.BasicQueryInfo.newBuilder()
                .setQueryId(value.getQueryId().toString())
                .setState(value.getState().name())
                .setQuery(value.getQuery())
                .setQueryStats(basicQueryStatsAdapter.toProtocol(value.getQueryStats()))
                .build();
    }

    @Override
    public BasicQueryInfo fromProtocol(com.facebook.presto.protocol.v2.BasicQueryInfo value)
    {
        requireNonNull(value, "value is null");
        throw new UnsupportedOperationException("BasicQueryInfo protobuf is currently a server-to-client projection and cannot reconstruct the full Java DTO");
    }
}
