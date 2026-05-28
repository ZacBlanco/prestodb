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

import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.buffer.OutputBuffers.OutputBufferId;
import com.facebook.presto.protocol.v2.BufferType;
import com.facebook.presto.protocol.v2.OutputBuffer;
import com.google.common.collect.ImmutableMap;

import static java.util.Objects.requireNonNull;

public class OutputBuffersAdapter
        implements ProtocolAdapter<OutputBuffers, com.facebook.presto.protocol.v2.OutputBuffers>
{
    @Override
    public com.facebook.presto.protocol.v2.OutputBuffers toProtocol(OutputBuffers value)
    {
        requireNonNull(value, "value is null");

        com.facebook.presto.protocol.v2.OutputBuffers.Builder builder = com.facebook.presto.protocol.v2.OutputBuffers.newBuilder()
                .setType(toProtocol(value.getType()))
                .setVersion(value.getVersion())
                .setNoMoreBufferIds(value.isNoMoreBufferIds());

        value.getBuffers().forEach((bufferId, partition) -> builder.addBuffers(OutputBuffer.newBuilder()
                .setId(bufferId.getId())
                .setPartition(partition)));

        return builder.build();
    }

    @Override
    public OutputBuffers fromProtocol(com.facebook.presto.protocol.v2.OutputBuffers value)
    {
        requireNonNull(value, "value is null");
        ImmutableMap.Builder<OutputBufferId, Integer> buffers = ImmutableMap.builder();
        for (OutputBuffer buffer : value.getBuffersList()) {
            buffers.put(new OutputBufferId(buffer.getId()), buffer.getPartition());
        }
        return new OutputBuffers(fromProtocol(value.getType()), value.getVersion(), value.getNoMoreBufferIds(), buffers.build());
    }

    private static BufferType toProtocol(OutputBuffers.BufferType value)
    {
        switch (value) {
            case PARTITIONED:
                return BufferType.BUFFER_TYPE_PARTITIONED;
            case BROADCAST:
                return BufferType.BUFFER_TYPE_BROADCAST;
            case ARBITRARY:
                return BufferType.BUFFER_TYPE_ARBITRARY;
            case DISCARDING:
                return BufferType.BUFFER_TYPE_DISCARDING;
            case SPOOLING:
                return BufferType.BUFFER_TYPE_SPOOLING;
        }
        return BufferType.BUFFER_TYPE_UNKNOWN;
    }

    private static OutputBuffers.BufferType fromProtocol(BufferType value)
    {
        switch (value) {
            case BUFFER_TYPE_PARTITIONED:
                return OutputBuffers.BufferType.PARTITIONED;
            case BUFFER_TYPE_BROADCAST:
                return OutputBuffers.BufferType.BROADCAST;
            case BUFFER_TYPE_ARBITRARY:
                return OutputBuffers.BufferType.ARBITRARY;
            case BUFFER_TYPE_DISCARDING:
                return OutputBuffers.BufferType.DISCARDING;
            case BUFFER_TYPE_SPOOLING:
                return OutputBuffers.BufferType.SPOOLING;
            case BUFFER_TYPE_UNKNOWN:
            case UNRECOGNIZED:
        }
        throw new IllegalArgumentException("Unsupported buffer type: " + value);
    }
}
