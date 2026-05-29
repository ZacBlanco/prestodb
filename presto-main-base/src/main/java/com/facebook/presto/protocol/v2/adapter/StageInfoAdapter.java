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

import com.facebook.presto.execution.StageInfo;

import static java.util.Objects.requireNonNull;

public class StageInfoAdapter
        implements ProtocolAdapter<StageInfo, com.facebook.presto.protocol.v2.StageInfo>
{
    @Override
    public com.facebook.presto.protocol.v2.StageInfo toProtocol(StageInfo value)
    {
        requireNonNull(value, "value is null");
        com.facebook.presto.protocol.v2.StageInfo.Builder builder = com.facebook.presto.protocol.v2.StageInfo.newBuilder()
                .setStageId(value.getStageId().toString())
                .setState(value.getLatestAttemptExecutionInfo().getState().name());
        value.getSubStages().stream()
                .map(this::toProtocol)
                .forEach(builder::addSubStages);
        return builder.build();
    }

    @Override
    public StageInfo fromProtocol(com.facebook.presto.protocol.v2.StageInfo value)
    {
        requireNonNull(value, "value is null");
        throw new UnsupportedOperationException("StageInfo protobuf is currently a server-to-client projection and cannot reconstruct the full Java DTO");
    }
}
