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

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.operator.BlockedReason;
import com.facebook.presto.operator.PipelineStats;
import com.facebook.presto.operator.TaskStats;
import com.fasterxml.jackson.databind.ObjectMapper;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class TaskStatsAdapter
        implements ProtocolAdapter<TaskStats, com.facebook.presto.protocol.v2.TaskStats>
{
    private final ConnectorPayloadAdapter connectorPayloadAdapter;

    public TaskStatsAdapter()
    {
        this(new ConnectorPayloadAdapter());
    }

    public TaskStatsAdapter(ObjectMapper objectMapper)
    {
        this(new ConnectorPayloadAdapter(objectMapper));
    }

    public TaskStatsAdapter(ConnectorPayloadAdapter connectorPayloadAdapter)
    {
        this.connectorPayloadAdapter = requireNonNull(connectorPayloadAdapter, "connectorPayloadAdapter is null");
    }

    @Override
    public com.facebook.presto.protocol.v2.TaskStats toProtocol(TaskStats value)
    {
        requireNonNull(value, "value is null");
        com.facebook.presto.protocol.v2.TaskStats.Builder builder = com.facebook.presto.protocol.v2.TaskStats.newBuilder()
                .setCreateTimeInMillis(value.getCreateTimeInMillis())
                .setFirstStartTimeInMillis(value.getFirstStartTimeInMillis())
                .setLastStartTimeInMillis(value.getLastStartTimeInMillis())
                .setLastEndTimeInMillis(value.getLastEndTimeInMillis())
                .setEndTimeInMillis(value.getEndTimeInMillis())
                .setElapsedTimeInNanos(value.getElapsedTimeInNanos())
                .setQueuedTimeInNanos(value.getQueuedTimeInNanos())
                .setTotalDrivers(value.getTotalDrivers())
                .setQueuedDrivers(value.getQueuedDrivers())
                .setRunningDrivers(value.getRunningDrivers())
                .setBlockedDrivers(value.getBlockedDrivers())
                .setCompletedDrivers(value.getCompletedDrivers())
                .setCumulativeUserMemory(value.getCumulativeUserMemory())
                .setCumulativeTotalMemory(value.getCumulativeTotalMemory())
                .setUserMemoryReservationInBytes(value.getUserMemoryReservationInBytes())
                .setRevocableMemoryReservationInBytes(value.getRevocableMemoryReservationInBytes())
                .setSystemMemoryReservationInBytes(value.getSystemMemoryReservationInBytes())
                .setPeakUserMemoryInBytes(value.getPeakUserMemoryInBytes())
                .setPeakTotalMemoryInBytes(value.getPeakTotalMemoryInBytes())
                .setPeakNodeTotalMemoryInBytes(value.getPeakNodeTotalMemoryInBytes())
                .setTotalScheduledTimeInNanos(value.getTotalScheduledTimeInNanos())
                .setTotalCpuTimeInNanos(value.getTotalCpuTimeInNanos())
                .setTotalBlockedTimeInNanos(value.getTotalBlockedTimeInNanos())
                .setFullyBlocked(value.isFullyBlocked())
                .setTotalAllocationInBytes(value.getTotalAllocationInBytes())
                .setRawInputDataSizeInBytes(value.getRawInputDataSizeInBytes())
                .setRawInputPositions(value.getRawInputPositions())
                .setProcessedInputDataSizeInBytes(value.getProcessedInputDataSizeInBytes())
                .setProcessedInputPositions(value.getProcessedInputPositions())
                .setOutputDataSizeInBytes(value.getOutputDataSizeInBytes())
                .setOutputPositions(value.getOutputPositions())
                .setPhysicalWrittenDataSizeInBytes(value.getPhysicalWrittenDataSizeInBytes())
                .setQueuedPartitionedDrivers(value.getQueuedPartitionedDrivers())
                .setQueuedPartitionedSplitsWeight(value.getQueuedPartitionedSplitsWeight())
                .setRunningPartitionedDrivers(value.getRunningPartitionedDrivers())
                .setRunningPartitionedSplitsWeight(value.getRunningPartitionedSplitsWeight())
                .setFullGcCount(value.getFullGcCount())
                .setFullGcTimeInMillis(value.getFullGcTimeInMillis())
                .setRuntimeStats(connectorPayloadAdapter.toConnectorPayload(value.getRuntimeStats()))
                .setTotalSplits(value.getTotalSplits())
                .setQueuedSplits(value.getQueuedSplits())
                .setRunningSplits(value.getRunningSplits())
                .setCompletedSplits(value.getCompletedSplits())
                .setTotalNewDrivers(value.getTotalNewDrivers())
                .setQueuedNewDrivers(value.getQueuedNewDrivers())
                .setRunningNewDrivers(value.getRunningNewDrivers())
                .setCompletedNewDrivers(value.getCompletedNewDrivers());
        value.getBlockedReasons().stream()
                .map(BlockedReason::name)
                .forEach(builder::addBlockedReasons);
        value.getPipelines().stream()
                .map(connectorPayloadAdapter::toConnectorPayload)
                .forEach(builder::addPipelines);
        return builder.build();
    }

    @Override
    public TaskStats fromProtocol(com.facebook.presto.protocol.v2.TaskStats value)
    {
        requireNonNull(value, "value is null");
        return new TaskStats(
                value.getCreateTimeInMillis(),
                value.getFirstStartTimeInMillis(),
                value.getLastStartTimeInMillis(),
                value.getLastEndTimeInMillis(),
                value.getEndTimeInMillis(),
                value.getElapsedTimeInNanos(),
                value.getQueuedTimeInNanos(),
                value.getTotalDrivers(),
                value.getQueuedDrivers(),
                value.getQueuedPartitionedDrivers(),
                value.getQueuedPartitionedSplitsWeight(),
                value.getRunningDrivers(),
                value.getRunningPartitionedDrivers(),
                value.getRunningPartitionedSplitsWeight(),
                value.getBlockedDrivers(),
                value.getCompletedDrivers(),
                value.getTotalNewDrivers(),
                value.getQueuedNewDrivers(),
                value.getRunningNewDrivers(),
                value.getCompletedNewDrivers(),
                value.getTotalSplits(),
                value.getQueuedSplits(),
                value.getRunningSplits(),
                value.getCompletedSplits(),
                value.getCumulativeUserMemory(),
                value.getCumulativeTotalMemory(),
                value.getUserMemoryReservationInBytes(),
                value.getRevocableMemoryReservationInBytes(),
                value.getSystemMemoryReservationInBytes(),
                value.getPeakTotalMemoryInBytes(),
                value.getPeakUserMemoryInBytes(),
                value.getPeakNodeTotalMemoryInBytes(),
                value.getTotalScheduledTimeInNanos(),
                value.getTotalCpuTimeInNanos(),
                value.getTotalBlockedTimeInNanos(),
                value.getFullyBlocked(),
                value.getBlockedReasonsList().stream()
                        .map(BlockedReason::valueOf)
                        .collect(toImmutableSet()),
                value.getTotalAllocationInBytes(),
                value.getRawInputDataSizeInBytes(),
                value.getRawInputPositions(),
                value.getProcessedInputDataSizeInBytes(),
                value.getProcessedInputPositions(),
                value.getOutputDataSizeInBytes(),
                value.getOutputPositions(),
                value.getPhysicalWrittenDataSizeInBytes(),
                value.getFullGcCount(),
                value.getFullGcTimeInMillis(),
                value.getPipelinesList().stream()
                        .map(payload -> connectorPayloadAdapter.fromConnectorPayload(payload, PipelineStats.class))
                        .collect(toImmutableList()),
                value.hasRuntimeStats() ? connectorPayloadAdapter.fromConnectorPayload(value.getRuntimeStats(), RuntimeStats.class) : new RuntimeStats());
    }
}
