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
package com.facebook.presto.plugin.memory;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import java.util.List;

import static com.facebook.presto.spi.session.PropertyMetadata.integerProperty;

public class MemorySessionProperties
{
    private static final String MEMORY_SPLITS_PER_NODE = "memory_splits_per_node";
    private final List<PropertyMetadata<?>> propertyMetadata;

    @Inject
    public MemorySessionProperties(MemoryConfig memoryConfig)
    {
        this.propertyMetadata = ImmutableList.<PropertyMetadata<?>>builder()
                .add(integerProperty(MEMORY_SPLITS_PER_NODE,
                        "number of splits to generate per node",
                        memoryConfig.getSplitsPerNode(),
                        false))
                .build();
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return propertyMetadata;
    }

    public static int getSplitsPerNode(ConnectorSession session)
    {
        return session.getProperty(MEMORY_SPLITS_PER_NODE, Integer.class);
    }
}
