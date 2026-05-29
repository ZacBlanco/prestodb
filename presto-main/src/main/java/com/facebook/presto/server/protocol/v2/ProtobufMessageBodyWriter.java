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

import com.google.protobuf.Message;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.ext.MessageBodyWriter;
import jakarta.ws.rs.ext.Provider;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

import static com.facebook.presto.PrestoMediaTypes.APPLICATION_PRESTO_PROTOBUF;

@Provider
@Produces(APPLICATION_PRESTO_PROTOBUF)
public class ProtobufMessageBodyWriter
        implements MessageBodyWriter<Message>
{
    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType)
    {
        return Message.class.isAssignableFrom(type) && MediaType.valueOf(APPLICATION_PRESTO_PROTOBUF).isCompatible(mediaType);
    }

    @Override
    public long getSize(Message message, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType)
    {
        return message.getSerializedSize();
    }

    @Override
    public void writeTo(Message message, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream)
            throws IOException, WebApplicationException
    {
        message.writeTo(entityStream);
    }
}
