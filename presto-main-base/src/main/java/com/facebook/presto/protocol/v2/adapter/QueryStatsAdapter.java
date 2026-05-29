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

import com.facebook.presto.execution.QueryStats;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class QueryStatsAdapter
        implements ProtocolAdapter<QueryStats, com.facebook.presto.protocol.v2.QueryStats>
{
    @Override
    public com.facebook.presto.protocol.v2.QueryStats toProtocol(QueryStats value)
    {
        requireNonNull(value, "value is null");
        return com.facebook.presto.protocol.v2.QueryStats.newBuilder()
                .setCreateTimeMillis(value.getCreateTimeInMillis())
                .setEndTimeMillis(value.getEndTimeInMillis())
                .setElapsedTimeMillis(value.getElapsedTime().roundTo(MILLISECONDS))
                .setTotalCpuTimeMillis(value.getTotalCpuTime().roundTo(MILLISECONDS))
                .build();
    }

    @Override
    public QueryStats fromProtocol(com.facebook.presto.protocol.v2.QueryStats value)
    {
        requireNonNull(value, "value is null");
        throw new UnsupportedOperationException("QueryStats protobuf is currently a server-to-client projection and cannot reconstruct the full Java DTO");
    }
}
