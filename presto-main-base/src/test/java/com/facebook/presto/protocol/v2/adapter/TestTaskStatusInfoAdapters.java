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

import com.facebook.presto.execution.ExecutionFailureInfo;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.execution.buffer.BufferInfo;
import com.facebook.presto.execution.buffer.OutputBufferInfo;
import com.facebook.presto.execution.buffer.OutputBuffers.OutputBufferId;
import com.facebook.presto.execution.buffer.PageBufferInfo;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.net.URI;

import static com.facebook.presto.execution.buffer.BufferState.OPEN;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestTaskStatusInfoAdapters
{
    @Test
    public void testTaskStatusRoundTrip()
    {
        ExecutionFailureInfo failure = new ExecutionFailureInfo(
                "java.lang.RuntimeException",
                "boom",
                null,
                ImmutableList.of(),
                ImmutableList.of("com.example.Test.method(Test.java:1)"),
                null,
                null,
                null,
                null);
        TaskStatus taskStatus = new TaskStatus(
                11,
                22,
                33,
                TaskState.RUNNING,
                URI.create("http://localhost/task"),
                ImmutableSet.of(Lifespan.driverGroup(7)),
                ImmutableList.of(failure),
                2,
                3,
                0.5,
                true,
                400,
                500,
                600,
                700,
                8,
                9,
                10,
                11,
                12,
                13);

        TaskStatus roundTrip = new TaskStatusAdapter().fromProtocol(new TaskStatusAdapter().toProtocol(taskStatus));

        assertEquals(roundTrip.getTaskInstanceIdLeastSignificantBits(), taskStatus.getTaskInstanceIdLeastSignificantBits());
        assertEquals(roundTrip.getTaskInstanceIdMostSignificantBits(), taskStatus.getTaskInstanceIdMostSignificantBits());
        assertEquals(roundTrip.getVersion(), taskStatus.getVersion());
        assertEquals(roundTrip.getState(), taskStatus.getState());
        assertEquals(roundTrip.getSelf(), taskStatus.getSelf());
        assertEquals(roundTrip.getCompletedDriverGroups(), taskStatus.getCompletedDriverGroups());
        assertEquals(roundTrip.getFailures().size(), 1);
        assertEquals(roundTrip.getFailures().get(0).getType(), failure.getType());
        assertEquals(roundTrip.getFailures().get(0).getMessage(), failure.getMessage());
        assertEquals(roundTrip.getQueuedPartitionedDrivers(), taskStatus.getQueuedPartitionedDrivers());
        assertEquals(roundTrip.getRunningPartitionedDrivers(), taskStatus.getRunningPartitionedDrivers());
        assertEquals(roundTrip.getOutputBufferUtilization(), taskStatus.getOutputBufferUtilization());
        assertEquals(roundTrip.isOutputBufferOverutilized(), taskStatus.isOutputBufferOverutilized());
        assertEquals(roundTrip.getPhysicalWrittenDataSizeInBytes(), taskStatus.getPhysicalWrittenDataSizeInBytes());
        assertEquals(roundTrip.getMemoryReservationInBytes(), taskStatus.getMemoryReservationInBytes());
        assertEquals(roundTrip.getSystemMemoryReservationInBytes(), taskStatus.getSystemMemoryReservationInBytes());
        assertEquals(roundTrip.getPeakNodeTotalMemoryReservationInBytes(), taskStatus.getPeakNodeTotalMemoryReservationInBytes());
        assertEquals(roundTrip.getFullGcCount(), taskStatus.getFullGcCount());
        assertEquals(roundTrip.getFullGcTimeInMillis(), taskStatus.getFullGcTimeInMillis());
        assertEquals(roundTrip.getTotalCpuTimeInNanos(), taskStatus.getTotalCpuTimeInNanos());
        assertEquals(roundTrip.getTaskAgeInMillis(), taskStatus.getTaskAgeInMillis());
        assertEquals(roundTrip.getQueuedPartitionedSplitsWeight(), taskStatus.getQueuedPartitionedSplitsWeight());
        assertEquals(roundTrip.getRunningPartitionedSplitsWeight(), taskStatus.getRunningPartitionedSplitsWeight());
    }

    @Test
    public void testTaskInfoRoundTrip()
    {
        OutputBufferInfo outputBufferInfo = new OutputBufferInfo(
                "PARTITIONED",
                OPEN,
                true,
                true,
                100,
                5,
                200,
                6,
                ImmutableList.of(new BufferInfo(new OutputBufferId(0), false, 2, 3, new PageBufferInfo(0, 2, 100, 200, 6))));
        TaskStats taskStats = new TaskStats(1, 2);
        TaskInfo taskInfo = new TaskInfo(
                new TaskId("query", 1, 2, 3, 4),
                TaskStatus.initialTaskStatus(URI.create("http://localhost/task")),
                1234,
                outputBufferInfo,
                ImmutableSet.of(new PlanNodeId("source")),
                taskStats,
                false,
                "node");

        TaskInfo roundTrip = new TaskInfoAdapter().fromProtocol(new TaskInfoAdapter().toProtocol(taskInfo));

        assertEquals(roundTrip.getTaskId(), taskInfo.getTaskId());
        assertEquals(roundTrip.getTaskStatus().getState(), taskInfo.getTaskStatus().getState());
        assertEquals(roundTrip.getLastHeartbeatInMillis(), taskInfo.getLastHeartbeatInMillis());
        assertEquals(roundTrip.getOutputBuffers(), taskInfo.getOutputBuffers());
        assertEquals(roundTrip.getNoMoreSplits(), taskInfo.getNoMoreSplits());
        assertEquals(roundTrip.getStats().getCreateTimeInMillis(), taskStats.getCreateTimeInMillis());
        assertEquals(roundTrip.getStats().getEndTimeInMillis(), taskStats.getEndTimeInMillis());
        assertFalse(roundTrip.isNeedsPlan());
        assertEquals(roundTrip.getNodeId(), taskInfo.getNodeId());
    }

    @Test
    public void testTaskStatsRoundTrip()
    {
        TaskStats taskStats = new TaskStats(10, 20);

        TaskStats roundTrip = new TaskStatsAdapter().fromProtocol(new TaskStatsAdapter().toProtocol(taskStats));

        assertEquals(roundTrip.getCreateTimeInMillis(), taskStats.getCreateTimeInMillis());
        assertEquals(roundTrip.getEndTimeInMillis(), taskStats.getEndTimeInMillis());
        assertEquals(roundTrip.getPipelines(), taskStats.getPipelines());
        assertTrue(roundTrip.getRuntimeStats().getMetrics().isEmpty());
    }
}
