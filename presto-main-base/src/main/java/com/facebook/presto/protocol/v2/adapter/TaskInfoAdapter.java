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

import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.fasterxml.jackson.databind.ObjectMapper;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class TaskInfoAdapter
        implements ProtocolAdapter<TaskInfo, com.facebook.presto.protocol.v2.TaskInfo>
{
    private final TaskStatusAdapter taskStatusAdapter;
    private final OutputBufferInfoAdapter outputBufferInfoAdapter;
    private final TaskStatsAdapter taskStatsAdapter;

    public TaskInfoAdapter()
    {
        this(new TaskStatusAdapter(), new OutputBufferInfoAdapter(), new TaskStatsAdapter());
    }

    public TaskInfoAdapter(ObjectMapper objectMapper)
    {
        this(new TaskStatusAdapter(), new OutputBufferInfoAdapter(), new TaskStatsAdapter(objectMapper));
    }

    public TaskInfoAdapter(TaskStatusAdapter taskStatusAdapter, OutputBufferInfoAdapter outputBufferInfoAdapter, TaskStatsAdapter taskStatsAdapter)
    {
        this.taskStatusAdapter = requireNonNull(taskStatusAdapter, "taskStatusAdapter is null");
        this.outputBufferInfoAdapter = requireNonNull(outputBufferInfoAdapter, "outputBufferInfoAdapter is null");
        this.taskStatsAdapter = requireNonNull(taskStatsAdapter, "taskStatsAdapter is null");
    }

    @Override
    public com.facebook.presto.protocol.v2.TaskInfo toProtocol(TaskInfo value)
    {
        requireNonNull(value, "value is null");
        com.facebook.presto.protocol.v2.TaskInfo.Builder builder = com.facebook.presto.protocol.v2.TaskInfo.newBuilder()
                .setTaskId(value.getTaskId().toString())
                .setTaskStatus(taskStatusAdapter.toProtocol(value.getTaskStatus()))
                .setLastHeartbeatInMillis(value.getLastHeartbeatInMillis())
                .setOutputBuffers(outputBufferInfoAdapter.toProtocol(value.getOutputBuffers()))
                .setStats(taskStatsAdapter.toProtocol(value.getStats()))
                .setNeedsPlan(value.isNeedsPlan())
                .setNodeId(value.getNodeId());
        value.getNoMoreSplits().stream()
                .map(PlanNodeId::getId)
                .forEach(builder::addNoMoreSplits);
        return builder.build();
    }

    @Override
    public TaskInfo fromProtocol(com.facebook.presto.protocol.v2.TaskInfo value)
    {
        requireNonNull(value, "value is null");
        return new TaskInfo(
                TaskId.valueOf(value.getTaskId()),
                taskStatusAdapter.fromProtocol(value.getTaskStatus()),
                value.getLastHeartbeatInMillis(),
                outputBufferInfoAdapter.fromProtocol(value.getOutputBuffers()),
                value.getNoMoreSplitsList().stream()
                        .map(PlanNodeId::new)
                        .collect(toImmutableSet()),
                taskStatsAdapter.fromProtocol(value.getStats()),
                value.getNeedsPlan(),
                value.getNodeId());
    }
}
