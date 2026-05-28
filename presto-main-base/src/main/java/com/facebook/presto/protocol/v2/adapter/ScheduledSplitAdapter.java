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

import com.facebook.presto.execution.ScheduledSplit;

import static com.facebook.presto.protocol.v2.adapter.ConnectorPayloadAdapter.toConnectorPayload;
import static java.util.Objects.requireNonNull;

public class ScheduledSplitAdapter
        implements ProtocolAdapter<ScheduledSplit, com.facebook.presto.protocol.v2.ScheduledSplit>
{
    @Override
    public com.facebook.presto.protocol.v2.ScheduledSplit toProtocol(ScheduledSplit value)
    {
        requireNonNull(value, "value is null");
        return com.facebook.presto.protocol.v2.ScheduledSplit.newBuilder()
                .setSequenceId(value.getSequenceId())
                .setPlanNodeId(value.getPlanNodeId().getId())
                .setSplit(toConnectorPayload(value.getSplit()))
                .build();
    }

    @Override
    public ScheduledSplit fromProtocol(com.facebook.presto.protocol.v2.ScheduledSplit value)
    {
        throw new UnsupportedOperationException("ScheduledSplit proto-to-Java conversion requires connector payload decoding");
    }
}
