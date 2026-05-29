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
package com.facebook.presto.server.protocol.v2;

import com.facebook.airlift.http.client.HeaderName;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.Response;
import com.facebook.airlift.http.client.ResponseHandler;
import com.facebook.presto.server.smile.BaseResponse;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.function.Function;

import static com.facebook.airlift.http.client.ResponseHandlerUtils.propagate;
import static com.google.common.io.ByteStreams.toByteArray;
import static java.util.Objects.requireNonNull;

public class ProtobufResponseHandler<J, P extends Message>
        implements ResponseHandler<BaseResponse<J>, RuntimeException>
{
    private final Parser<P> parser;
    private final Function<P, J> adapter;

    public ProtobufResponseHandler(Parser<P> parser, Function<P, J> adapter)
    {
        this.parser = requireNonNull(parser, "parser is null");
        this.adapter = requireNonNull(adapter, "adapter is null");
    }

    @Override
    public BaseResponse<J> handleException(Request request, Exception exception)
            throws RuntimeException
    {
        throw propagate(request, exception);
    }

    @Override
    public BaseResponse<J> handle(Request request, Response response)
            throws RuntimeException
    {
        byte[] bytes;
        try {
            bytes = toByteArray(response.getInputStream());
        }
        catch (IOException e) {
            throw new UncheckedIOException("Error reading response from server", e);
        }
        return new ProtobufResponse<>(response.getStatusCode(), response.getHeaders(), bytes, parser, adapter);
    }

    private static class ProtobufResponse<J, P extends Message>
            implements BaseResponse<J>
    {
        private final int statusCode;
        private final ListMultimap<HeaderName, String> headers;
        private final byte[] responseBytes;
        private final J value;
        private final IllegalArgumentException exception;

        private ProtobufResponse(int statusCode, ListMultimap<HeaderName, String> headers, byte[] responseBytes, Parser<P> parser, Function<P, J> adapter)
        {
            this.statusCode = statusCode;
            this.headers = ImmutableListMultimap.copyOf(headers);
            this.responseBytes = responseBytes.clone();

            J value = null;
            IllegalArgumentException exception = null;
            try {
                value = adapter.apply(parser.parseFrom(responseBytes));
            }
            catch (InvalidProtocolBufferException | RuntimeException e) {
                exception = new IllegalArgumentException("Unable to decode protobuf response", e);
            }
            this.value = value;
            this.exception = exception;
        }

        @Override
        public int getStatusCode()
        {
            return statusCode;
        }

        @Override
        public String getHeader(String name)
        {
            List<String> values = getHeaders().get(HeaderName.of(name));
            if (values.isEmpty()) {
                return null;
            }
            return values.get(0);
        }

        @Override
        public List<String> getHeaders(String name)
        {
            return headers.get(HeaderName.of(name));
        }

        @Override
        public ListMultimap<HeaderName, String> getHeaders()
        {
            return headers;
        }

        @Override
        public boolean hasValue()
        {
            return exception == null;
        }

        @Override
        public J getValue()
        {
            if (!hasValue()) {
                throw new IllegalStateException("Response does not contain a protobuf value", exception);
            }
            return value;
        }

        @Override
        public int getResponseSize()
        {
            return responseBytes.length;
        }

        @Override
        public byte[] getResponseBytes()
        {
            return responseBytes.clone();
        }

        @Override
        public Exception getException()
        {
            return exception;
        }
    }
}
