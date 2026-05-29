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

import com.facebook.presto.client.ErrorLocation;
import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.common.ErrorType;
import com.facebook.presto.execution.ExecutionFailureInfo;
import com.facebook.presto.spi.ErrorCause;
import com.facebook.presto.spi.HostAddress;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class ExecutionFailureInfoAdapter
        implements ProtocolAdapter<ExecutionFailureInfo, com.facebook.presto.protocol.v2.ExecutionFailureInfo>
{
    @Override
    public com.facebook.presto.protocol.v2.ExecutionFailureInfo toProtocol(ExecutionFailureInfo value)
    {
        requireNonNull(value, "value is null");
        com.facebook.presto.protocol.v2.ExecutionFailureInfo.Builder builder = com.facebook.presto.protocol.v2.ExecutionFailureInfo.newBuilder()
                .setType(value.getType())
                .addAllStack(value.getStack());

        if (value.getMessage() != null) {
            builder.setMessage(value.getMessage());
        }
        if (value.getCause() != null) {
            builder.setCause(toProtocol(value.getCause()));
        }
        value.getSuppressed().stream()
                .map(this::toProtocol)
                .forEach(builder::addSuppressed);
        if (value.getErrorLocation() != null) {
            builder.setErrorLocation(toProtocol(value.getErrorLocation()));
        }
        if (value.getErrorCode() != null) {
            builder.setErrorCode(toProtocol(value.getErrorCode()));
        }
        if (value.getRemoteHost() != null) {
            builder.setRemoteHost(value.getRemoteHost().toString());
        }
        if (value.getErrorCause() != null) {
            builder.setErrorCause(value.getErrorCause().name());
        }
        return builder.build();
    }

    @Override
    public ExecutionFailureInfo fromProtocol(com.facebook.presto.protocol.v2.ExecutionFailureInfo value)
    {
        requireNonNull(value, "value is null");
        return new ExecutionFailureInfo(
                value.getType(),
                value.hasMessage() ? value.getMessage() : null,
                value.hasCause() ? fromProtocol(value.getCause()) : null,
                value.getSuppressedList().stream()
                        .map(this::fromProtocol)
                        .collect(toImmutableList()),
                value.getStackList(),
                value.hasErrorLocation() ? fromProtocol(value.getErrorLocation()) : null,
                value.hasErrorCode() ? fromProtocol(value.getErrorCode()) : null,
                value.hasRemoteHost() ? HostAddress.fromString(value.getRemoteHost()) : null,
                value.hasErrorCause() ? ErrorCause.valueOf(value.getErrorCause()) : null);
    }

    private static com.facebook.presto.protocol.v2.ErrorLocation toProtocol(ErrorLocation value)
    {
        return com.facebook.presto.protocol.v2.ErrorLocation.newBuilder()
                .setLineNumber(value.getLineNumber())
                .setColumnNumber(value.getColumnNumber())
                .build();
    }

    private static ErrorLocation fromProtocol(com.facebook.presto.protocol.v2.ErrorLocation value)
    {
        return new ErrorLocation(value.getLineNumber(), value.getColumnNumber());
    }

    private static com.facebook.presto.protocol.v2.ErrorCode toProtocol(ErrorCode value)
    {
        return com.facebook.presto.protocol.v2.ErrorCode.newBuilder()
                .setCode(value.getCode())
                .setName(value.getName())
                .setType(value.getType().name())
                .setRetriable(value.isRetriable())
                .setCatchableByTry(value.isCatchableByTry())
                .build();
    }

    private static ErrorCode fromProtocol(com.facebook.presto.protocol.v2.ErrorCode value)
    {
        return new ErrorCode(
                value.getCode(),
                value.getName(),
                ErrorType.valueOf(value.getType()),
                value.getRetriable(),
                value.getCatchableByTry());
    }
}
