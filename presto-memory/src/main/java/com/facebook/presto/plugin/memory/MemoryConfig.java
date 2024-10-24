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

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import java.nio.file.Path;
import java.nio.file.Paths;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class MemoryConfig
{
    private int splitsPerNode = Runtime.getRuntime().availableProcessors();
    private Path spillDirectory;
    private DataSize maxDataPerNode = new DataSize(128, MEGABYTE);
    private DataSize maxMemoryPerTable = new DataSize(64, MEGABYTE);
    private DataSize maxSpillFileSize = new DataSize(1, GIGABYTE);

    @NotNull
    public int getSplitsPerNode()
    {
        return splitsPerNode;
    }

    @Config("memory.splits-per-node")
    public MemoryConfig setSplitsPerNode(int splitsPerNode)
    {
        this.splitsPerNode = splitsPerNode;
        return this;
    }

    @NotNull
    public DataSize getMaxDataPerNode()
    {
        return maxDataPerNode;
    }

    @Config("memory.max-memory-size")
    @ConfigDescription("max amount of memory that can be stored in all tables within in a single node.")
    public MemoryConfig setMaxDataPerNode(DataSize maxDataPerNode)
    {
        this.maxDataPerNode = maxDataPerNode;
        return this;
    }

    @NotNull
    public DataSize getMaxMemoryPerTable()
    {
        return maxMemoryPerTable;
    }

    @Config("memory.max-memory-per-table")
    public MemoryConfig setMaxMemoryPerTable(DataSize maxTableMemory)
    {
        this.maxMemoryPerTable = maxTableMemory;
        return this;
    }

    @NotNull
    public DataSize getMaxSpillFileSize()
    {
        return maxSpillFileSize;
    }

    @Config("memory.max-spill-file-size")
    public MemoryConfig setMaxSpillFileSize(DataSize maxSpillFileSize)
    {
        this.maxSpillFileSize = maxSpillFileSize;
        return this;
    }

    @Nullable
    public Path getSpillDirectory()
    {
        return spillDirectory;
    }

    @Config("memory.spill-directory")
    public MemoryConfig setSpillDirectory(String spillDirectory)
    {
        this.spillDirectory = Paths.get(spillDirectory);
        return this;
    }
}
