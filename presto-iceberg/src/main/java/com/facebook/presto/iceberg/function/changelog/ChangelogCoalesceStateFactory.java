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
package com.facebook.presto.iceberg.function.changelog;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.AccumulatorStateFactory;

import static java.util.Objects.requireNonNull;

public class ChangelogCoalesceStateFactory
        implements AccumulatorStateFactory<ChangelogCoalesceState>
{
    private final Type type;

    public ChangelogCoalesceStateFactory(Type type)
    {
        this.type = requireNonNull(type, "type is null");
    }

    @Override
    public ChangelogCoalesceState createSingleState()
    {
        return new ChangelogCoalesceState.Single(type);
    }

    @Override
    public Class<? extends ChangelogCoalesceState> getSingleStateClass()
    {
        return ChangelogCoalesceState.Single.class;
    }

    @Override
    public ChangelogCoalesceState.Grouped createGroupedState()
    {
        return new ChangelogCoalesceState.Grouped(type);
    }

    @Override
    public Class<? extends ChangelogCoalesceState> getGroupedStateClass()
    {
        return ChangelogCoalesceState.Grouped.class;
    }
}
