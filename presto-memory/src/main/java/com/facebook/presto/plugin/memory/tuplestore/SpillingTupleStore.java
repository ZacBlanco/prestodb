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
package com.facebook.presto.plugin.memory.tuplestore;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.page.PagesSerdeUtil;
import com.facebook.presto.spi.page.SerializedPage;
import com.google.common.collect.Iterators;
import com.google.common.io.Closer;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.SliceOutput;
import io.airlift.units.DataSize;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Deque;
import java.util.Iterator;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.util.UUID.randomUUID;

/**
 * The {@link SpillingTupleStore} is a thread-safe object which can accept a set of iterators
 * over {@link Page}s. A small number of pages can be configured to be stored in-memory. Over that
 * amount, pages will spill to one or more files on disk (max file size configurable).
 * <br>
 * A reader can ask the tuple store for the set of pages submitted so far. It will return an
 * iterator which lazily iterates over all the tuples available in the tuple store.
 * <br>
 * A spilled set of pages is in a special page file format.
 */
@ThreadSafe
public class SpillingTupleStore
        implements TupleStore
{
    private static final Logger log = Logger.get(SpillingTupleStore.class);
    /**
     * Directory where spill files are created.
     */
    private final Path spillDirectory;
    /**
     * Total number of rows in the tuple store in bytes.
     */
    private final AtomicLong rowCount = new AtomicLong();
    /**
     * Total size of the tuple store in bytes
     */
    private final AtomicLong sizeInBytes = new AtomicLong();
    /**
     * Total pages committed
     */
    private final AtomicLong pageCount = new AtomicLong(0L);
    private final Closer closer = Closer.create();
    /**
     * Executor
     */
    private final ExecutorService executorService;
    /**
     * page serde
     */
    private final PagesSerde serde;
    /**
     * Total in-memory bytes retained by the {@code memoryPages} variable.
     */
    private final AtomicLong currentQueueSizeInBytes = new AtomicLong(0);
    /**
     * Current set of pages stored in memory
     */
    private final AtomicReference<Queue<Page>> memoryPages = new AtomicReference<>(new ConcurrentLinkedQueue<>());
    /**
     * Guards against spilling on multiple threads at the same time during
     * {@link SpillingTupleStore#flushQueueToDisk()}
     */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    @Nullable
    private OutputStreamSliceOutput output;
    /**
     * current spill file path
     */
    private Path currentPath;
    /**
     * All spills that have been written
     */
    private final Deque<Path> paths = new ConcurrentLinkedDeque<>();
    /**
     * The memory limit where spilling begins
     */
    private final DataSize maxMemory;
    /**
     * Executor used to run spilling tasks
     */
    private final ExecutorService serializedExec = Executors.newSingleThreadExecutor();
    /**
     * max spill file size on disk
     */
    private final DataSize maxSpillSize;

    public SpillingTupleStore(int spillingThreads, @Nullable Path spillDirectory, DataSize maxMemory, DataSize maxSpillSize)
    {
        executorService = Executors.newWorkStealingPool(spillingThreads);
        BlockEncodingManager blockEncodingManager = new BlockEncodingManager();
        this.serde = new PagesSerde(blockEncodingManager, Optional.empty(), Optional.empty(), Optional.empty());
        closer.register(executorService::shutdown);
        closer.register(serializedExec::shutdown);
        this.maxMemory = maxMemory;
        this.maxSpillSize = maxSpillSize;
        try {
            this.spillDirectory = spillDirectory == null ?
                    Files.createTempDirectory("presto-memory-spilling") :
                    spillDirectory;
            if (!Files.isWritable(this.spillDirectory)) {
                throw new IOException("Spill directory is not writable");
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to initialize tuple store: ", e);
        }
    }

    @Override
    public CompletableFuture<?> addPages(Iterator<Page> pages)
    {
        return addPagesWithSpill(pages);
    }

    /**
     * protocol:
     * <p>
     * 1. any number of concurrent readers may add pages to the in-memory set.
     * 2. Only a single writer can flush at a time
     * </p>
     * @param pages pages to add
     * @return future which is complete when all pages have been committed
     */
    private CompletableFuture<Void> addPagesWithSpill(Iterator<Page> pages)
    {
        while (pages.hasNext()) {
            Page page = pages.next();
            try {
                lock.readLock().lock();
                memoryPages.get().offer(page);
                rowCount.addAndGet(page.getPositionCount());
                sizeInBytes.addAndGet(page.getRetainedSizeInBytes());
                pageCount.incrementAndGet();
                if (currentQueueSizeInBytes.addAndGet(page.getRetainedSizeInBytes()) > maxMemory.toBytes()) {
                    return CompletableFuture.completedFuture(null)
                            .thenComposeAsync(unused -> this.flushQueueToDisk(false), executorService)
                            .thenComposeAsync(unused -> addPagesWithSpill(pages), executorService);
                }
            }
            finally {
                lock.readLock().unlock();
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<?> flushQueueToDisk(boolean force)
    {
        try {
            lock.writeLock().lock();
            if (currentQueueSizeInBytes.get() <= maxMemory.toBytes() && !force) {
                return CompletableFuture.completedFuture(null);
            }
            // make sure there is an existing slice output
            if (output == null || output.longSize() > maxSpillSize.toBytes()) {
                createNewSpillFile();
            }

            Queue<Page> pageQueue = memoryPages.get();
            final SliceOutput outputSlice = output;
            CompletableFuture<?> spillTask = CompletableFuture.runAsync(() -> {
                pageQueue.forEach(page -> {
                    SerializedPage serializedPage = serde.serialize(page);
                    PagesSerdeUtil.writeSerializedPage(outputSlice, serializedPage);
                });
                try {
                    // flushing so reading an in-progress file doesn't result in
                    // errors
                    outputSlice.flush();
                }
                catch (IOException e) {
                    log.warn("Failed to flush spill to disk", e);
                }
            }, serializedExec);

            memoryPages.set(new LinkedBlockingQueue<>());
            currentQueueSizeInBytes.set(0);
            return spillTask;
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to write pagefile", e);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public long getMemoryUtilization()
    {
        return currentQueueSizeInBytes.get();
    }

    private synchronized void createNewSpillFile()
            throws IOException
    {
        if (output != null) {
            output.close();
        }

        currentPath = spillDirectory.resolve(randomUUID() + ".pagefile");
        paths.add(currentPath);
        output = new OutputStreamSliceOutput(Files.newOutputStream(currentPath));
        final String closePath = currentPath.toString();
        closer.register(() -> Files.delete(Paths.get(closePath)));
    }

    @Override
    public Iterator<Page> readPages()
    {
        // we're allowed to get up to N pages at the time of writing since that many
        // pages have been committed
        long limit = pageCount.get();
        return Iterators.limit(Iterators.concat(memoryPages.get().iterator(),
                Iterators.concat(Iterators.transform(paths.iterator(), path -> {
                    try {
                        return PagesSerdeUtil.readPages(serde, new InputStreamSliceInput(Files.newInputStream(path)));
                    }
                    catch (IOException e) {
                        throw new RuntimeException("Failed to iterate all pages in tuple store on " + path, e);
                    }
                }))), (int) limit);
    }

    @Override
    public CompletableFuture<?> flush()
    {
        return CompletableFuture.completedFuture(null)
                .thenComposeAsync(unused -> flushQueueToDisk(true), executorService);
    }

    @Override
    public void close()
            throws Exception
    {
        closer.close();
    }
}
