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

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.ext.MessageBodyReader;
import jakarta.ws.rs.ext.Provider;

import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;

import static com.facebook.presto.PrestoMediaTypes.APPLICATION_PRESTO_PROTOBUF;

@Provider
@Consumes(APPLICATION_PRESTO_PROTOBUF)
public class ProtobufMessageBodyReader
        implements MessageBodyReader<Message>
{
    @Override
    public boolean isReadable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType)
    {
        return Message.class.isAssignableFrom(type) && MediaType.valueOf(APPLICATION_PRESTO_PROTOBUF).isCompatible(mediaType);
    }

    @Override
    public Message readFrom(Class<Message> type, Type genericType, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, String> httpHeaders, InputStream entityStream)
            throws IOException, WebApplicationException
    {
        try {
            Method method = type.getMethod("getDefaultInstance");
            Message defaultInstance = (Message) method.invoke(null);
            return defaultInstance.getParserForType().parseFrom(entityStream);
        }
        catch (InvalidProtocolBufferException e) {
            throw new BadRequestException("Malformed protobuf request body", e);
        }
        catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new WebApplicationException("Unable to parse protobuf request body", e);
        }
    }
}
