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

import com.facebook.presto.execution.buffer.BufferInfo;
import com.facebook.presto.execution.buffer.BufferState;
import com.facebook.presto.execution.buffer.OutputBufferInfo;
import com.facebook.presto.execution.buffer.OutputBuffers.OutputBufferId;
import com.facebook.presto.execution.buffer.PageBufferInfo;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class OutputBufferInfoAdapter
        implements ProtocolAdapter<OutputBufferInfo, com.facebook.presto.protocol.v2.OutputBufferInfo>
{
    @Override
    public com.facebook.presto.protocol.v2.OutputBufferInfo toProtocol(OutputBufferInfo value)
    {
        requireNonNull(value, "value is null");
        com.facebook.presto.protocol.v2.OutputBufferInfo.Builder builder = com.facebook.presto.protocol.v2.OutputBufferInfo.newBuilder()
                .setType(value.getType())
                .setState(toProtocol(value.getState()))
                .setCanAddBuffers(value.isCanAddBuffers())
                .setCanAddPages(value.isCanAddPages())
                .setTotalBufferedBytes(value.getTotalBufferedBytes())
                .setTotalBufferedPages(value.getTotalBufferedPages())
                .setTotalRowsSent(value.getTotalRowsSent())
                .setTotalPagesSent(value.getTotalPagesSent());
        value.getBuffers().stream()
                .map(OutputBufferInfoAdapter::toProtocol)
                .forEach(builder::addBuffers);
        return builder.build();
    }

    @Override
    public OutputBufferInfo fromProtocol(com.facebook.presto.protocol.v2.OutputBufferInfo value)
    {
        requireNonNull(value, "value is null");
        return new OutputBufferInfo(
                value.getType(),
                fromProtocol(value.getState()),
                value.getCanAddBuffers(),
                value.getCanAddPages(),
                value.getTotalBufferedBytes(),
                value.getTotalBufferedPages(),
                value.getTotalRowsSent(),
                value.getTotalPagesSent(),
                value.getBuffersList().stream()
                        .map(OutputBufferInfoAdapter::fromProtocol)
                        .collect(toImmutableList()));
    }

    private static com.facebook.presto.protocol.v2.BufferInfo toProtocol(BufferInfo value)
    {
        return com.facebook.presto.protocol.v2.BufferInfo.newBuilder()
                .setBufferId(value.getBufferId().getId())
                .setFinished(value.isFinished())
                .setBufferedPages(value.getBufferedPages())
                .setPagesSent(value.getPagesSent())
                .setPageBufferInfo(toProtocol(value.getPageBufferInfo()))
                .build();
    }

    private static BufferInfo fromProtocol(com.facebook.presto.protocol.v2.BufferInfo value)
    {
        return new BufferInfo(
                new OutputBufferId(value.getBufferId()),
                value.getFinished(),
                value.getBufferedPages(),
                value.getPagesSent(),
                fromProtocol(value.getPageBufferInfo()));
    }

    private static com.facebook.presto.protocol.v2.PageBufferInfo toProtocol(PageBufferInfo value)
    {
        return com.facebook.presto.protocol.v2.PageBufferInfo.newBuilder()
                .setPartition(value.getPartition())
                .setBufferedPages(value.getBufferedPages())
                .setBufferedBytes(value.getBufferedBytes())
                .setRowsAdded(value.getRowsAdded())
                .setPagesAdded(value.getPagesAdded())
                .build();
    }

    private static PageBufferInfo fromProtocol(com.facebook.presto.protocol.v2.PageBufferInfo value)
    {
        return new PageBufferInfo(
                value.getPartition(),
                value.getBufferedPages(),
                value.getBufferedBytes(),
                value.getRowsAdded(),
                value.getPagesAdded());
    }

    private static com.facebook.presto.protocol.v2.BufferState toProtocol(BufferState value)
    {
        switch (value) {
            case OPEN:
                return com.facebook.presto.protocol.v2.BufferState.BUFFER_STATE_OPEN;
            case NO_MORE_BUFFERS:
                return com.facebook.presto.protocol.v2.BufferState.BUFFER_STATE_NO_MORE_BUFFERS;
            case NO_MORE_PAGES:
                return com.facebook.presto.protocol.v2.BufferState.BUFFER_STATE_NO_MORE_PAGES;
            case FLUSHING:
                return com.facebook.presto.protocol.v2.BufferState.BUFFER_STATE_FLUSHING;
            case FINISHED:
                return com.facebook.presto.protocol.v2.BufferState.BUFFER_STATE_FINISHED;
            case FAILED:
                return com.facebook.presto.protocol.v2.BufferState.BUFFER_STATE_FAILED;
        }
        return com.facebook.presto.protocol.v2.BufferState.BUFFER_STATE_UNKNOWN;
    }

    private static BufferState fromProtocol(com.facebook.presto.protocol.v2.BufferState value)
    {
        switch (value) {
            case BUFFER_STATE_OPEN:
                return BufferState.OPEN;
            case BUFFER_STATE_NO_MORE_BUFFERS:
                return BufferState.NO_MORE_BUFFERS;
            case BUFFER_STATE_NO_MORE_PAGES:
                return BufferState.NO_MORE_PAGES;
            case BUFFER_STATE_FLUSHING:
                return BufferState.FLUSHING;
            case BUFFER_STATE_FINISHED:
                return BufferState.FINISHED;
            case BUFFER_STATE_FAILED:
                return BufferState.FAILED;
            case BUFFER_STATE_UNKNOWN:
            case UNRECOGNIZED:
        }
        throw new IllegalArgumentException("Unsupported buffer state: " + value);
    }
}
