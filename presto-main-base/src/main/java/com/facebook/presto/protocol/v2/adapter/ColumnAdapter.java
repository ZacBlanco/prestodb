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

import com.facebook.presto.client.Column;

import static java.util.Objects.requireNonNull;

public class ColumnAdapter
        implements ProtocolAdapter<Column, com.facebook.presto.protocol.v2.Column>
{
    @Override
    public com.facebook.presto.protocol.v2.Column toProtocol(Column value)
    {
        requireNonNull(value, "value is null");
        return com.facebook.presto.protocol.v2.Column.newBuilder()
                .setName(value.getName())
                .setType(value.getType())
                .build();
    }

    @Override
    public Column fromProtocol(com.facebook.presto.protocol.v2.Column value)
    {
        requireNonNull(value, "value is null");
        return new Column(value.getName(), value.getType(), null);
    }
}
