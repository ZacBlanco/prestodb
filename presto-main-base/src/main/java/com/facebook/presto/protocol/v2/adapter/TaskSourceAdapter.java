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

import com.facebook.presto.execution.TaskSource;

import static java.util.Objects.requireNonNull;

public class TaskSourceAdapter
        implements ProtocolAdapter<TaskSource, com.facebook.presto.protocol.v2.TaskSource>
{
    private final ScheduledSplitAdapter scheduledSplitAdapter = new ScheduledSplitAdapter();
    private final LifespanAdapter lifespanAdapter = new LifespanAdapter();

    @Override
    public com.facebook.presto.protocol.v2.TaskSource toProtocol(TaskSource value)
    {
        requireNonNull(value, "value is null");
        com.facebook.presto.protocol.v2.TaskSource.Builder builder = com.facebook.presto.protocol.v2.TaskSource.newBuilder()
                .setPlanNodeId(value.getPlanNodeId().getId())
                .setNoMoreSplits(value.isNoMoreSplits());

        value.getSplits().stream()
                .map(scheduledSplitAdapter::toProtocol)
                .forEach(builder::addSplits);
        value.getNoMoreSplitsForLifespan().stream()
                .map(lifespanAdapter::toProtocol)
                .forEach(builder::addNoMoreSplitsForLifespan);

        return builder.build();
    }

    @Override
    public TaskSource fromProtocol(com.facebook.presto.protocol.v2.TaskSource value)
    {
        throw new UnsupportedOperationException("TaskSource proto-to-Java conversion requires split payload decoding");
    }
}
