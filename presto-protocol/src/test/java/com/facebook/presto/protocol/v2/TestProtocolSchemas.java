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
package com.facebook.presto.protocol.v2;

import com.google.protobuf.ByteString;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestProtocolSchemas
{
    @Test
    public void testTaskUpdateRequestRoundTrip()
            throws Exception
    {
        SessionRepresentation session = SessionRepresentation.newBuilder()
                .setQueryId("20260527_000000_00000_test")
                .setClientTransactionSupport(true)
                .setUser("test-user")
                .setSource("test-source")
                .setTimeZoneKey(0)
                .setLocale("en_US")
                .setStartTime(123)
                .putSystemProperties("task_writer_count", "4")
                .build();

        TaskUpdateRequest request = TaskUpdateRequest.newBuilder()
                .setSession(session)
                .putExtraCredentials("credential", "value")
                .setFragment(ByteString.copyFromUtf8("serialized-plan-fragment"))
                .addSources(TaskSource.newBuilder()
                        .setPlanNodeId("0")
                        .addSplits(ScheduledSplit.newBuilder()
                                .setSequenceId(1)
                                .setPlanNodeId("0")
                                .setSplit(ConnectorPayload.newBuilder()
                                        .setEncoding("json")
                                        .setData(ByteString.copyFromUtf8("serialized-split"))))
                        .addNoMoreSplitsForLifespan(Lifespan.newBuilder()
                                .setGrouped(true)
                                .setGroupId(7))
                        .setNoMoreSplits(true))
                .setOutputIds(OutputBuffers.newBuilder()
                        .setType(BufferType.BUFFER_TYPE_PARTITIONED)
                        .setVersion(2)
                        .setNoMoreBufferIds(true)
                        .addBuffers(OutputBuffer.newBuilder()
                                .setId(0)
                                .setPartition(0)))
                .build();

        assertEquals(TaskUpdateRequest.parseFrom(request.toByteArray()), request);
    }
}
