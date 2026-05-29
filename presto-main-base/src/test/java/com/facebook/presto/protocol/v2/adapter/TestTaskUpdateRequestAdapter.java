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

import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.buffer.OutputBuffers.OutputBufferId;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.metadata.SplitJacksonModule;
import com.facebook.presto.metadata.TransactionHandleJacksonModule;
import com.facebook.presto.server.TaskUpdateRequest;
import com.facebook.presto.spi.ConnectorCodec;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.testing.TestingHandleResolver;
import com.facebook.presto.testing.TestingSplit;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import static com.facebook.presto.protocol.v2.adapter.ConnectorPayloadAdapter.JSON_JAVA_CLASS_ENCODING;
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

        com.facebook.presto.protocol.v2.TaskUpdateRequest protocol = new TaskUpdateRequestAdapter(createTestingObjectMapper()).toProtocol(request);

        assertEquals(protocol.getSession().getQueryId(), TEST_SESSION.getQueryId().toString());
        assertEquals(protocol.getExtraCredentialsMap(), ImmutableMap.of("token", "secret"));
        assertEquals(protocol.getFragment().toString(StandardCharsets.UTF_8), "fragment");
        assertEquals(protocol.getSourcesCount(), 1);
        assertEquals(protocol.getSources(0).getPlanNodeId(), TABLE_SCAN_NODE_ID.getId());
        assertEquals(protocol.getSources(0).getSplitsCount(), 1);
        assertEquals(protocol.getSources(0).getSplits(0).getSplit().getEncoding(), JSON_JAVA_CLASS_ENCODING);
        assertTrue(protocol.getSources(0).getSplits(0).getSplit().getData().toString(StandardCharsets.UTF_8).contains("\"@type\":\"com.facebook.presto.metadata.Split\""));
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

    @Test
    public void testTaskUpdateRequestFromProtocolWithoutConnectorPayloads()
    {
        OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(PARTITIONED)
                .withNoMoreBufferIds();
        TaskUpdateRequest request = new TaskUpdateRequest(
                TEST_SESSION.toSessionRepresentation(),
                ImmutableMap.of("token", "secret"),
                Optional.of("fragment".getBytes(StandardCharsets.UTF_8)),
                ImmutableList.of(new TaskSource(TABLE_SCAN_NODE_ID, ImmutableSet.of(), true)),
                outputBuffers,
                Optional.empty());

        TaskUpdateRequest roundTrip = new TaskUpdateRequestAdapter().fromProtocol(new TaskUpdateRequestAdapter().toProtocol(request));

        assertEquals(roundTrip.getSession().getQueryId(), request.getSession().getQueryId());
        assertEquals(roundTrip.getExtraCredentials(), request.getExtraCredentials());
        assertEquals(roundTrip.getFragment().map(bytes -> new String(bytes, StandardCharsets.UTF_8)), Optional.of("fragment"));
        assertEquals(roundTrip.getSources().size(), 1);
        assertEquals(roundTrip.getSources().get(0).getPlanNodeId(), TABLE_SCAN_NODE_ID);
        assertEquals(roundTrip.getSources().get(0).getSplits(), ImmutableSet.of());
        assertEquals(roundTrip.getSources().get(0).getNoMoreSplitsForLifespan(), ImmutableSet.of());
        assertTrue(roundTrip.getSources().get(0).isNoMoreSplits());
        assertEquals(roundTrip.getOutputIds(), request.getOutputIds());
        assertEquals(roundTrip.getTableWriteInfo(), request.getTableWriteInfo());
    }

    @Test
    public void testTaskUpdateRequestRoundTripWithSplitPayload()
    {
        ObjectMapper objectMapper = createTestingObjectMapper();
        TaskUpdateRequestAdapter adapter = new TaskUpdateRequestAdapter(objectMapper);
        OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(PARTITIONED)
                .withNoMoreBufferIds();
        TaskUpdateRequest request = new TaskUpdateRequest(
                TEST_SESSION.toSessionRepresentation(),
                ImmutableMap.of(),
                Optional.empty(),
                ImmutableList.of(new TaskSource(TABLE_SCAN_NODE_ID, ImmutableSet.of(SPLIT), false)),
                outputBuffers,
                Optional.empty());

        TaskUpdateRequest roundTrip = adapter.fromProtocol(adapter.toProtocol(request));

        assertEquals(roundTrip.getSources().size(), 1);
        assertEquals(roundTrip.getSources().get(0).getSplits().size(), 1);
        assertEquals(roundTrip.getSources().get(0).getSplits().iterator().next().getSequenceId(), SPLIT.getSequenceId());
        assertSplitEquals(roundTrip.getSources().get(0).getSplits().iterator().next().getSplit(), SPLIT.getSplit());
    }

    @Test
    public void testTaskUpdateRequestRoundTripWithNoMoreSplitsForLifespan()
    {
        TaskUpdateRequestAdapter adapter = new TaskUpdateRequestAdapter(createTestingObjectMapper());
        OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(PARTITIONED)
                .withBuffer(new OutputBufferId(0), 0)
                .withNoMoreBufferIds();
        TaskSource taskSource = new TaskSource(
                TABLE_SCAN_NODE_ID,
                ImmutableSet.of(),
                ImmutableSet.of(Lifespan.driverGroup(7)),
                false);
        TaskUpdateRequest request = new TaskUpdateRequest(
                TEST_SESSION.toSessionRepresentation(),
                ImmutableMap.of("token", "secret"),
                Optional.empty(),
                ImmutableList.of(taskSource),
                outputBuffers,
                Optional.empty());

        TaskUpdateRequest roundTrip = adapter.fromProtocol(adapter.toProtocol(request));

        assertEquals(roundTrip.getSources().size(), 1);
        assertEquals(roundTrip.getSources().get(0).getNoMoreSplitsForLifespan(), ImmutableSet.of(Lifespan.driverGroup(7)));
        assertEquals(roundTrip.getSources().get(0).isNoMoreSplits(), false);
        assertEquals(roundTrip.getOutputIds(), outputBuffers);
    }

    private static void assertSplitEquals(Split actual, Split expected)
    {
        assertEquals(actual.getConnectorId(), expected.getConnectorId());
        assertEquals(actual.getTransactionHandle(), expected.getTransactionHandle());
        assertEquals(actual.getLifespan(), expected.getLifespan());
        assertEquals(actual.getSplitContext().isCacheable(), expected.getSplitContext().isCacheable());
        assertEquals(actual.getConnectorSplit().getClass(), expected.getConnectorSplit().getClass());

        TestingSplit actualConnectorSplit = (TestingSplit) actual.getConnectorSplit();
        TestingSplit expectedConnectorSplit = (TestingSplit) expected.getConnectorSplit();
        assertEquals(actualConnectorSplit.getNodeSelectionStrategy(), expectedConnectorSplit.getNodeSelectionStrategy());
        assertEquals(actualConnectorSplit.getAddresses(), expectedConnectorSplit.getAddresses());
    }

    private static ObjectMapper createTestingObjectMapper()
    {
        HandleResolver handleResolver = new HandleResolver();
        handleResolver.addConnectorName("test", new TestingHandleResolver());
        FeaturesConfig featuresConfig = new FeaturesConfig();
        ObjectMapper objectMapper = new JsonObjectMapperProvider().get();
        objectMapper.registerModule(new SplitJacksonModule(
                handleResolver,
                featuresConfig,
                (ConnectorId connectorId) -> Optional.<ConnectorCodec<ConnectorSplit>>empty()));
        objectMapper.registerModule(new TransactionHandleJacksonModule(
                handleResolver,
                featuresConfig,
                (ConnectorId connectorId) -> Optional.<ConnectorCodec<ConnectorTransactionHandle>>empty()));
        return objectMapper;
    }
}
