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
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.buffer.OutputBuffers.OutputBufferId;
import com.facebook.presto.server.TaskUpdateRequest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.execution.TaskTestUtils.SPLIT;
import static com.facebook.presto.execution.TaskTestUtils.TABLE_SCAN_NODE_ID;
import static com.facebook.presto.execution.buffer.OutputBuffers.BufferType.PARTITIONED;
import static com.facebook.presto.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestTaskUpdateRequestAdapter
{
    @Test
    public void testTaskUpdateRequestToProtocol()
    {
        OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(PARTITIONED)
                .withBuffer(new OutputBufferId(0), 0)
                .withNoMoreBufferIds();
        TaskSource source = new TaskSource(TABLE_SCAN_NODE_ID, ImmutableSet.of(SPLIT), true);
        TaskUpdateRequest request = new TaskUpdateRequest(
                TEST_SESSION.toSessionRepresentation(),
                ImmutableMap.of("token", "secret"),
                Optional.of("fragment".getBytes(StandardCharsets.UTF_8)),
                ImmutableList.of(source),
                outputBuffers,
                Optional.empty());

        com.facebook.presto.protocol.v2.TaskUpdateRequest protocol = new TaskUpdateRequestAdapter().toProtocol(request);

        assertEquals(protocol.getSession().getQueryId(), TEST_SESSION.getQueryId().toString());
        assertEquals(protocol.getExtraCredentialsMap(), ImmutableMap.of("token", "secret"));
        assertEquals(protocol.getFragment().toString(StandardCharsets.UTF_8), "fragment");
        assertEquals(protocol.getSourcesCount(), 1);
        assertEquals(protocol.getSources(0).getPlanNodeId(), TABLE_SCAN_NODE_ID.getId());
        assertEquals(protocol.getSources(0).getSplitsCount(), 1);
        assertEquals(protocol.getSources(0).getSplits(0).getSplit().getEncoding(), "java-to-string");
        assertEquals(protocol.getOutputIds().getType(), com.facebook.presto.protocol.v2.BufferType.BUFFER_TYPE_PARTITIONED);
        assertTrue(protocol.getOutputIds().getNoMoreBufferIds());
        assertEquals(protocol.getOutputIds().getBuffersCount(), 1);
    }

    @Test
    public void testOutputBuffersRoundTrip()
    {
        OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(PARTITIONED)
                .withBuffer(new OutputBufferId(3), 7)
                .withNoMoreBufferIds();

        OutputBuffersAdapter adapter = new OutputBuffersAdapter();
        assertEquals(adapter.fromProtocol(adapter.toProtocol(outputBuffers)), outputBuffers);
    }
}
