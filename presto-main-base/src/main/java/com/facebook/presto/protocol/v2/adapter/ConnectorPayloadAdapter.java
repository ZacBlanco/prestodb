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
import com.facebook.presto.protocol.v2.ConnectorPayload;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public final class ConnectorPayloadAdapter
{
    public static final String JSON_JAVA_CLASS_ENCODING = "json+java-class";
    private static final String TYPE_PROPERTY = "@type";
    private static final String PAYLOAD_PROPERTY = "payload";

    private final ObjectMapper objectMapper;

    public ConnectorPayloadAdapter()
    {
        this(new JsonObjectMapperProvider().get());
    }

    public ConnectorPayloadAdapter(ObjectMapper objectMapper)
    {
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
    }

    public ConnectorPayload toConnectorPayload(Object value)
    {
        requireNonNull(value, "value is null");
        try {
            ObjectNode wrapper = objectMapper.createObjectNode();
            wrapper.put(TYPE_PROPERTY, value.getClass().getName());
            wrapper.set(PAYLOAD_PROPERTY, objectMapper.valueToTree(value));
            return ConnectorPayload.newBuilder()
                    .setEncoding(JSON_JAVA_CLASS_ENCODING)
                    .setData(ByteString.copyFrom(objectMapper.writeValueAsBytes(wrapper)))
                    .build();
        }
        catch (IllegalArgumentException | IOException e) {
            throw new IllegalArgumentException("Unable to serialize connector payload of type " + value.getClass().getName(), e);
        }
    }

    public <T> T fromConnectorPayload(ConnectorPayload payload, Class<T> expectedType)
    {
        requireNonNull(payload, "payload is null");
        requireNonNull(expectedType, "expectedType is null");
        if (!JSON_JAVA_CLASS_ENCODING.equals(payload.getEncoding())) {
            throw new IllegalArgumentException("Unsupported connector payload encoding: " + payload.getEncoding());
        }

        try {
            JsonNode wrapper = objectMapper.readTree(payload.getData().toByteArray());
            JsonNode typeNode = wrapper.get(TYPE_PROPERTY);
            JsonNode payloadNode = wrapper.get(PAYLOAD_PROPERTY);
            if (typeNode == null || payloadNode == null) {
                throw new IllegalArgumentException("Connector payload must contain " + TYPE_PROPERTY + " and " + PAYLOAD_PROPERTY);
            }

            Class<?> actualType = Class.forName(typeNode.asText());
            if (!expectedType.isAssignableFrom(actualType)) {
                throw new IllegalArgumentException("Connector payload type " + actualType.getName() + " is not assignable to " + expectedType.getName());
            }

            return expectedType.cast(objectMapper.treeToValue(payloadNode, actualType));
        }
        catch (ClassNotFoundException | IOException e) {
            throw new IllegalArgumentException("Unable to deserialize connector payload", e);
        }
    }
}
