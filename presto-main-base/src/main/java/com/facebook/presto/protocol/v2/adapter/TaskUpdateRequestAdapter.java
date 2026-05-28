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

import com.facebook.presto.server.TaskUpdateRequest;
import com.google.protobuf.ByteString;

import static java.util.Objects.requireNonNull;

public class TaskUpdateRequestAdapter
        implements ProtocolAdapter<TaskUpdateRequest, com.facebook.presto.protocol.v2.TaskUpdateRequest>
{
    private final SessionRepresentationAdapter sessionRepresentationAdapter = new SessionRepresentationAdapter();
    private final TaskSourceAdapter taskSourceAdapter = new TaskSourceAdapter();
    private final OutputBuffersAdapter outputBuffersAdapter = new OutputBuffersAdapter();
    private final TableWriteInfoAdapter tableWriteInfoAdapter = new TableWriteInfoAdapter();

    @Override
    public com.facebook.presto.protocol.v2.TaskUpdateRequest toProtocol(TaskUpdateRequest value)
    {
        requireNonNull(value, "value is null");

        com.facebook.presto.protocol.v2.TaskUpdateRequest.Builder builder = com.facebook.presto.protocol.v2.TaskUpdateRequest.newBuilder()
                .setSession(sessionRepresentationAdapter.toProtocol(value.getSession()))
                .putAllExtraCredentials(value.getExtraCredentials())
                .setOutputIds(outputBuffersAdapter.toProtocol(value.getOutputIds()));

        value.getFragment().ifPresent(fragment -> builder.setFragment(ByteString.copyFrom(fragment)));
        value.getSources().stream()
                .map(taskSourceAdapter::toProtocol)
                .forEach(builder::addSources);
        value.getTableWriteInfo().ifPresent(tableWriteInfo -> builder.setTableWriteInfo(tableWriteInfoAdapter.toProtocol(tableWriteInfo)));

        return builder.build();
    }

    @Override
    public TaskUpdateRequest fromProtocol(com.facebook.presto.protocol.v2.TaskUpdateRequest value)
    {
        throw new UnsupportedOperationException("TaskUpdateRequest proto-to-Java conversion is not implemented yet");
    }
}
