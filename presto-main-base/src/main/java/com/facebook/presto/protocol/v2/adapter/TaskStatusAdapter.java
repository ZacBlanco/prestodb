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

import com.facebook.presto.execution.TaskState;
import com.facebook.presto.execution.TaskStatus;

import java.net.URI;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class TaskStatusAdapter
        implements ProtocolAdapter<TaskStatus, com.facebook.presto.protocol.v2.TaskStatus>
{
    private final LifespanAdapter lifespanAdapter;
    private final ExecutionFailureInfoAdapter executionFailureInfoAdapter;

    public TaskStatusAdapter()
    {
        this(new LifespanAdapter(), new ExecutionFailureInfoAdapter());
    }

    public TaskStatusAdapter(LifespanAdapter lifespanAdapter, ExecutionFailureInfoAdapter executionFailureInfoAdapter)
    {
        this.lifespanAdapter = requireNonNull(lifespanAdapter, "lifespanAdapter is null");
        this.executionFailureInfoAdapter = requireNonNull(executionFailureInfoAdapter, "executionFailureInfoAdapter is null");
    }

    @Override
    public com.facebook.presto.protocol.v2.TaskStatus toProtocol(TaskStatus value)
    {
        requireNonNull(value, "value is null");
        com.facebook.presto.protocol.v2.TaskStatus.Builder builder = com.facebook.presto.protocol.v2.TaskStatus.newBuilder()
                .setTaskInstanceIdLeastSignificantBits(value.getTaskInstanceIdLeastSignificantBits())
                .setTaskInstanceIdMostSignificantBits(value.getTaskInstanceIdMostSignificantBits())
                .setVersion(value.getVersion())
                .setState(toProtocol(value.getState()))
                .setSelf(value.getSelf().toString())
                .setQueuedPartitionedDrivers(value.getQueuedPartitionedDrivers())
                .setRunningPartitionedDrivers(value.getRunningPartitionedDrivers())
                .setOutputBufferUtilization(value.getOutputBufferUtilization())
                .setOutputBufferOverutilized(value.isOutputBufferOverutilized())
                .setPhysicalWrittenDataSizeInBytes(value.getPhysicalWrittenDataSizeInBytes())
                .setMemoryReservationInBytes(value.getMemoryReservationInBytes())
                .setSystemMemoryReservationInBytes(value.getSystemMemoryReservationInBytes())
                .setFullGcCount(value.getFullGcCount())
                .setFullGcTimeInMillis(value.getFullGcTimeInMillis())
                .setPeakNodeTotalMemoryReservationInBytes(value.getPeakNodeTotalMemoryReservationInBytes())
                .setTotalCpuTimeInNanos(value.getTotalCpuTimeInNanos())
                .setTaskAgeInMillis(value.getTaskAgeInMillis())
                .setQueuedPartitionedSplitsWeight(value.getQueuedPartitionedSplitsWeight())
                .setRunningPartitionedSplitsWeight(value.getRunningPartitionedSplitsWeight());
        value.getCompletedDriverGroups().stream()
                .map(lifespanAdapter::toProtocol)
                .forEach(builder::addCompletedDriverGroups);
        value.getFailures().stream()
                .map(executionFailureInfoAdapter::toProtocol)
                .forEach(builder::addFailures);
        return builder.build();
    }

    @Override
    public TaskStatus fromProtocol(com.facebook.presto.protocol.v2.TaskStatus value)
    {
        requireNonNull(value, "value is null");
        return new TaskStatus(
                value.getTaskInstanceIdLeastSignificantBits(),
                value.getTaskInstanceIdMostSignificantBits(),
                value.getVersion(),
                fromProtocol(value.getState()),
                URI.create(value.getSelf()),
                value.getCompletedDriverGroupsList().stream()
                        .map(lifespanAdapter::fromProtocol)
                        .collect(toImmutableSet()),
                value.getFailuresList().stream()
                        .map(executionFailureInfoAdapter::fromProtocol)
                        .collect(toImmutableList()),
                value.getQueuedPartitionedDrivers(),
                value.getRunningPartitionedDrivers(),
                value.getOutputBufferUtilization(),
                value.getOutputBufferOverutilized(),
                value.getPhysicalWrittenDataSizeInBytes(),
                value.getMemoryReservationInBytes(),
                value.getSystemMemoryReservationInBytes(),
                value.getPeakNodeTotalMemoryReservationInBytes(),
                value.getFullGcCount(),
                value.getFullGcTimeInMillis(),
                value.getTotalCpuTimeInNanos(),
                value.getTaskAgeInMillis(),
                value.getQueuedPartitionedSplitsWeight(),
                value.getRunningPartitionedSplitsWeight());
    }

    private static com.facebook.presto.protocol.v2.TaskState toProtocol(TaskState value)
    {
        switch (value) {
            case PLANNED:
                return com.facebook.presto.protocol.v2.TaskState.TASK_STATE_PLANNED;
            case RUNNING:
                return com.facebook.presto.protocol.v2.TaskState.TASK_STATE_RUNNING;
            case FINISHED:
                return com.facebook.presto.protocol.v2.TaskState.TASK_STATE_FINISHED;
            case CANCELED:
                return com.facebook.presto.protocol.v2.TaskState.TASK_STATE_CANCELED;
            case ABORTED:
                return com.facebook.presto.protocol.v2.TaskState.TASK_STATE_ABORTED;
            case FAILED:
                return com.facebook.presto.protocol.v2.TaskState.TASK_STATE_FAILED;
        }
        return com.facebook.presto.protocol.v2.TaskState.TASK_STATE_UNKNOWN;
    }

    private static TaskState fromProtocol(com.facebook.presto.protocol.v2.TaskState value)
    {
        switch (value) {
            case TASK_STATE_PLANNED:
                return TaskState.PLANNED;
            case TASK_STATE_RUNNING:
                return TaskState.RUNNING;
            case TASK_STATE_FINISHED:
                return TaskState.FINISHED;
            case TASK_STATE_CANCELED:
                return TaskState.CANCELED;
            case TASK_STATE_ABORTED:
                return TaskState.ABORTED;
            case TASK_STATE_FAILED:
                return TaskState.FAILED;
            case TASK_STATE_UNKNOWN:
            case UNRECOGNIZED:
        }
        throw new IllegalArgumentException("Unsupported task state: " + value);
    }
}
