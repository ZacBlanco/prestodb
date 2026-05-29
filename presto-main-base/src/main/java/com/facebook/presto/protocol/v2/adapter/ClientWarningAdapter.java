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

import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.WarningCode;

import static java.util.Objects.requireNonNull;

public class ClientWarningAdapter
        implements ProtocolAdapter<PrestoWarning, com.facebook.presto.protocol.v2.ClientWarning>
{
    @Override
    public com.facebook.presto.protocol.v2.ClientWarning toProtocol(PrestoWarning value)
    {
        requireNonNull(value, "value is null");
        return com.facebook.presto.protocol.v2.ClientWarning.newBuilder()
                .setCode(value.getWarningCode().getCode())
                .setName(value.getWarningCode().getName())
                .setMessage(value.getMessage())
                .build();
    }

    @Override
    public PrestoWarning fromProtocol(com.facebook.presto.protocol.v2.ClientWarning value)
    {
        requireNonNull(value, "value is null");
        return new PrestoWarning(new WarningCode(value.getCode(), value.getName()), value.getMessage());
    }
}
