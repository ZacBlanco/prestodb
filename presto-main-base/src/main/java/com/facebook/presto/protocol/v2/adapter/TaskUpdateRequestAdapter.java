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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;

import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class TaskUpdateRequestAdapter
        implements ProtocolAdapter<TaskUpdateRequest, com.facebook.presto.protocol.v2.TaskUpdateRequest>
{
    private final SessionRepresentationAdapter sessionRepresentationAdapter;
    private final TaskSourceAdapter taskSourceAdapter;
    private final OutputBuffersAdapter outputBuffersAdapter;
    private final TableWriteInfoAdapter tableWriteInfoAdapter;

    public TaskUpdateRequestAdapter()
    {
        this(new ConnectorPayloadAdapter());
    }

    public TaskUpdateRequestAdapter(ObjectMapper objectMapper)
    {
        this(new ConnectorPayloadAdapter(objectMapper));
    }

    public TaskUpdateRequestAdapter(ConnectorPayloadAdapter connectorPayloadAdapter)
    {
        requireNonNull(connectorPayloadAdapter, "connectorPayloadAdapter is null");
        this.sessionRepresentationAdapter = new SessionRepresentationAdapter();
        this.taskSourceAdapter = new TaskSourceAdapter(new ScheduledSplitAdapter(connectorPayloadAdapter), new LifespanAdapter());
        this.outputBuffersAdapter = new OutputBuffersAdapter();
        this.tableWriteInfoAdapter = new TableWriteInfoAdapter(connectorPayloadAdapter);
    }

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
        requireNonNull(value, "value is null");
        return new TaskUpdateRequest(
                sessionRepresentationAdapter.fromProtocol(value.getSession()),
                value.getExtraCredentialsMap(),
                value.hasFragment() ? Optional.of(value.getFragment().toByteArray()) : Optional.empty(),
                value.getSourcesList().stream()
                        .map(taskSourceAdapter::fromProtocol)
                        .collect(toImmutableList()),
                outputBuffersAdapter.fromProtocol(value.getOutputIds()),
                value.hasTableWriteInfo() ? Optional.of(tableWriteInfoAdapter.fromProtocol(value.getTableWriteInfo())) : Optional.empty());
    }
}
