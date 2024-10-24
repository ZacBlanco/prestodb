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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.plugin.memory.tuplestore.SpillingTupleStore;
import com.facebook.presto.plugin.memory.tuplestore.TupleStore;
import com.facebook.presto.spi.PrestoException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import com.google.common.collect.Streams;
import io.airlift.units.DataSize;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.facebook.presto.plugin.memory.MemoryErrorCode.MISSING_DATA;
import static java.lang.String.format;

@ThreadSafe
public class MemoryPagesStore
        implements AutoCloseable
{
    private static final Logger log = Logger.get(MemoryPagesStore.class);
    private final long maxBytes;
    private final DataSize maxSpillSize;
    private final DataSize maxTableMemorySize;
    @Nullable
    private final Path spillDirectory;

    private Supplier<Long> currentBytes;

    private final Map<Long, TableData> tables = new HashMap<>();

    @Inject
    public MemoryPagesStore(MemoryConfig config)
    {
        this.maxBytes = config.getMaxDataPerNode().toBytes();
        this.maxSpillSize = config.getMaxSpillFileSize();
        this.maxTableMemorySize = config.getMaxMemoryPerTable();
        this.spillDirectory = config.getSpillDirectory();
        currentBytes = () -> tables.values().stream().mapToLong(TableData::getStoredBytes).sum();
    }

    @VisibleForTesting
    public long getCurrentSize()
    {
        return currentBytes.get();
    }

    public synchronized void initialize(long tableId)
    {
        if (!tables.containsKey(tableId)) {
            tables.put(tableId, new TableData(spillDirectory, maxTableMemorySize, maxSpillSize));
        }
    }

    public synchronized void add(Long tableId, Page page)
    {
        if (!contains(tableId)) {
            throw new PrestoException(MISSING_DATA, "Failed to find table on a worker.");
        }
        page.compact();

        TableData tableData = tables.get(tableId);
        tableData.add(page);
        if (currentBytes.get() > maxBytes) {
            CompletableFuture.allOf(tables.values().stream().map(TableData::spill).toArray(CompletableFuture<?>[]::new)).join();
        }
    }

    public synchronized Stream<Page> getPages(
            Long tableId,
            int partNumber,
            int totalParts,
            List<Integer> columnIndexes,
            long expectedRows)
    {
        if (!contains(tableId)) {
            throw new PrestoException(MISSING_DATA, "Failed to find table on a worker.");
        }
        TableData tableData = tables.get(tableId);
        if (tableData.getRows() < expectedRows) {
            throw new PrestoException(MISSING_DATA,
                    format("Expected to find [%s] rows on a worker, but found [%s].", expectedRows, tableData.getRows()));
        }
        // skip every N elements starting from partNumber

        return Streams.zip(
                IntStream.iterate(0, i -> i + 1).skip(partNumber).boxed(),
                Streams.stream(tableData.getPages()).skip(partNumber),
                (indx, page) -> {
                    if (((indx - partNumber) % totalParts == 0)) {
                        return page;
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .map(page -> getColumns(page, columnIndexes));
    }

    public synchronized boolean contains(Long tableId)
    {
        return tables.containsKey(tableId);
    }

    public synchronized void cleanUp(Set<Long> activeTableIds)
    {
        // We have to remember that there might be some race conditions when there are two tables created at once.
        // That can lead to a situation when MemoryPagesStore already knows about a newer second table on some worker
        // but cleanUp is triggered by insert from older first table, which MemoryTableHandle was created before
        // second table creation. Thus activeTableIds can have missing latest ids and we can only clean up tables
        // that:
        // - have smaller value then max(activeTableIds).
        // - are missing from activeTableIds set

        if (activeTableIds.isEmpty()) {
            // if activeTableIds is empty, we can not determine latestTableId...
            return;
        }
        long latestTableId = Collections.max(activeTableIds);

        for (Iterator<Map.Entry<Long, TableData>> tableDataIterator = tables.entrySet().iterator(); tableDataIterator.hasNext(); ) {
            Map.Entry<Long, TableData> tablePagesEntry = tableDataIterator.next();
            Long tableId = tablePagesEntry.getKey();
            if (tableId < latestTableId && !activeTableIds.contains(tableId)) {
                try {
                    tablePagesEntry.getValue().close();
                }
                catch (Exception e) {
                    log.warn(e, "Failed to close tuple store: %s", e.toString());
                }
                tableDataIterator.remove();
            }
        }
    }

    private static Page getColumns(Page page, List<Integer> columnIndexes)
    {
        Block[] outputBlocks = new Block[columnIndexes.size()];

        for (int i = 0; i < columnIndexes.size(); i++) {
            outputBlocks[i] = page.getBlock(columnIndexes.get(i));
        }

        return new Page(page.getPositionCount(), outputBlocks);
    }

    @Override
    public void close()
            throws Exception
    {
        for (TableData tableData : tables.values()) {
            tableData.close();
        }
    }

    private static final class TableData
            implements AutoCloseable
    {
        private final TupleStore pages;
        private long rows;

        public TableData(Path spillPath, DataSize maxTableMemory, DataSize maxSpillFileSize)
        {
            this.pages = new SpillingTupleStore(
                    Runtime.getRuntime().availableProcessors(),
                    spillPath,
                    maxTableMemory,
                    maxSpillFileSize);
        }

        public void add(Page page)
        {
            pages.addPages(Iterators.singletonIterator(page)).join();
            rows += page.getPositionCount();
        }

        public CompletableFuture<?> spill()
        {
            return pages.flush();
        }

        private Iterator<Page> getPages()
        {
            return pages.readPages();
        }

        private long getRows()
        {
            return rows;
        }

        private long getStoredBytes()
        {
            return pages.getMemoryUtilization();
        }

        @Override
        public void close()
                throws Exception
        {
            pages.close();
        }
    }
}
