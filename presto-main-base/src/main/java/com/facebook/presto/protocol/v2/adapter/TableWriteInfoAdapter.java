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

import com.facebook.presto.execution.scheduler.ExecutionWriterTarget;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.metadata.AnalyzeTableHandle;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class TableWriteInfoAdapter
        implements ProtocolAdapter<TableWriteInfo, com.facebook.presto.protocol.v2.TableWriteInfo>
{
    private final ConnectorPayloadAdapter connectorPayloadAdapter;

    public TableWriteInfoAdapter()
    {
        this(new ConnectorPayloadAdapter());
    }

    public TableWriteInfoAdapter(ConnectorPayloadAdapter connectorPayloadAdapter)
    {
        this.connectorPayloadAdapter = requireNonNull(connectorPayloadAdapter, "connectorPayloadAdapter is null");
    }

    @Override
    public com.facebook.presto.protocol.v2.TableWriteInfo toProtocol(TableWriteInfo value)
    {
        requireNonNull(value, "value is null");
        com.facebook.presto.protocol.v2.TableWriteInfo.Builder builder = com.facebook.presto.protocol.v2.TableWriteInfo.newBuilder();
        value.getWriterTarget().ifPresent(writerTarget -> builder.setWriterTarget(connectorPayloadAdapter.toConnectorPayload(writerTarget)));
        value.getAnalyzeTableHandle().ifPresent(analyzeTableHandle -> builder.setAnalyzeTableHandle(connectorPayloadAdapter.toConnectorPayload(analyzeTableHandle)));
        return builder.build();
    }

    @Override
    public TableWriteInfo fromProtocol(com.facebook.presto.protocol.v2.TableWriteInfo value)
    {
        requireNonNull(value, "value is null");
        return new TableWriteInfo(
                value.hasWriterTarget() ? Optional.of(connectorPayloadAdapter.fromConnectorPayload(value.getWriterTarget(), ExecutionWriterTarget.class)) : Optional.empty(),
                value.hasAnalyzeTableHandle() ? Optional.of(connectorPayloadAdapter.fromConnectorPayload(value.getAnalyzeTableHandle(), AnalyzeTableHandle.class)) : Optional.empty());
    }
}
