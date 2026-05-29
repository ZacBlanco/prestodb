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

import com.facebook.presto.server.BasicQueryStats;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class BasicQueryStatsAdapter
        implements ProtocolAdapter<BasicQueryStats, com.facebook.presto.protocol.v2.BasicQueryStats>
{
    @Override
    public com.facebook.presto.protocol.v2.BasicQueryStats toProtocol(BasicQueryStats value)
    {
        requireNonNull(value, "value is null");
        return com.facebook.presto.protocol.v2.BasicQueryStats.newBuilder()
                .setCreateTimeMillis(value.getCreateTimeInMillis())
                .setEndTimeMillis(value.getEndTimeInMillis())
                .setElapsedTimeMillis(value.getElapsedTime().roundTo(MILLISECONDS))
                .build();
    }

    @Override
    public BasicQueryStats fromProtocol(com.facebook.presto.protocol.v2.BasicQueryStats value)
    {
        requireNonNull(value, "value is null");
        throw new UnsupportedOperationException("BasicQueryStats protobuf is currently a server-to-client projection and cannot reconstruct the full Java DTO");
    }
}
