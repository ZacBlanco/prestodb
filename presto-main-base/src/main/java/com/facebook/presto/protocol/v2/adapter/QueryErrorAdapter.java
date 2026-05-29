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

import com.facebook.presto.client.QueryError;

import static java.util.Objects.requireNonNull;

public class QueryErrorAdapter
        implements ProtocolAdapter<QueryError, com.facebook.presto.protocol.v2.QueryError>
{
    @Override
    public com.facebook.presto.protocol.v2.QueryError toProtocol(QueryError value)
    {
        requireNonNull(value, "value is null");
        com.facebook.presto.protocol.v2.QueryError.Builder builder = com.facebook.presto.protocol.v2.QueryError.newBuilder()
                .setMessage(value.getMessage())
                .setErrorCode(value.getErrorCode())
                .setRetriable(value.isRetriable());
        if (value.getSqlState() != null) {
            builder.setSqlState(value.getSqlState());
        }
        if (value.getErrorName() != null) {
            builder.setErrorName(value.getErrorName());
        }
        if (value.getErrorType() != null) {
            builder.setErrorType(value.getErrorType());
        }
        return builder.build();
    }

    @Override
    public QueryError fromProtocol(com.facebook.presto.protocol.v2.QueryError value)
    {
        requireNonNull(value, "value is null");
        return new QueryError(
                value.getMessage(),
                value.hasSqlState() ? value.getSqlState() : null,
                value.getErrorCode(),
                value.hasErrorName() ? value.getErrorName() : null,
                value.hasErrorType() ? value.getErrorType() : null,
                value.getRetriable(),
                null,
                null);
    }
}
