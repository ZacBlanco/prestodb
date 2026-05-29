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

import com.facebook.presto.client.StatementStats;

import static java.util.Objects.requireNonNull;

public class StatementStatsAdapter
        implements ProtocolAdapter<StatementStats, com.facebook.presto.protocol.v2.StatementStats>
{
    private final StageStatsAdapter stageStatsAdapter;

    public StatementStatsAdapter()
    {
        this(new StageStatsAdapter());
    }

    public StatementStatsAdapter(StageStatsAdapter stageStatsAdapter)
    {
        this.stageStatsAdapter = requireNonNull(stageStatsAdapter, "stageStatsAdapter is null");
    }

    @Override
    public com.facebook.presto.protocol.v2.StatementStats toProtocol(StatementStats value)
    {
        requireNonNull(value, "value is null");
        com.facebook.presto.protocol.v2.StatementStats.Builder builder = com.facebook.presto.protocol.v2.StatementStats.newBuilder()
                .setState(value.getState())
                .setWaitingForPrerequisites(value.isWaitingForPrerequisites())
                .setQueued(value.isQueued())
                .setScheduled(value.isScheduled())
                .setNodes(value.getNodes())
                .setTotalSplits(value.getTotalSplits())
                .setQueuedSplits(value.getQueuedSplits())
                .setRunningSplits(value.getRunningSplits())
                .setCompletedSplits(value.getCompletedSplits())
                .setCpuTimeMillis(value.getCpuTimeMillis())
                .setWallTimeMillis(value.getWallTimeMillis())
                .setWaitingForPrerequisitesTimeMillis(value.getWaitingForPrerequisitesTimeMillis())
                .setQueuedTimeMillis(value.getQueuedTimeMillis())
                .setElapsedTimeMillis(value.getElapsedTimeMillis())
                .setProcessedRows(value.getProcessedRows())
                .setProcessedBytes(value.getProcessedBytes())
                .setPeakMemoryBytes(value.getPeakMemoryBytes())
                .setPeakTotalMemoryBytes(value.getPeakTotalMemoryBytes())
                .setPeakTaskTotalMemoryBytes(value.getPeakTaskTotalMemoryBytes())
                .setSpilledBytes(value.getSpilledBytes());
        if (value.getRootStage() != null) {
            builder.setRootStage(stageStatsAdapter.toProtocol(value.getRootStage()));
        }
        return builder.build();
    }

    @Override
    public StatementStats fromProtocol(com.facebook.presto.protocol.v2.StatementStats value)
    {
        requireNonNull(value, "value is null");
        return new StatementStats(
                value.getState(),
                value.getWaitingForPrerequisites(),
                value.getQueued(),
                value.getScheduled(),
                value.getNodes(),
                value.getTotalSplits(),
                value.getQueuedSplits(),
                value.getRunningSplits(),
                value.getCompletedSplits(),
                value.getCpuTimeMillis(),
                value.getWallTimeMillis(),
                value.getWaitingForPrerequisitesTimeMillis(),
                value.getQueuedTimeMillis(),
                value.getElapsedTimeMillis(),
                value.getProcessedRows(),
                value.getProcessedBytes(),
                value.getPeakMemoryBytes(),
                value.getPeakTotalMemoryBytes(),
                value.getPeakTaskTotalMemoryBytes(),
                value.getSpilledBytes(),
                value.hasRootStage() ? stageStatsAdapter.fromProtocol(value.getRootStage()) : null,
                null);
    }
}
