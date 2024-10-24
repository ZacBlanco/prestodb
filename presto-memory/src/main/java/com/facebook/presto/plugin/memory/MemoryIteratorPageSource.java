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

import com.facebook.presto.common.Page;
import com.facebook.presto.spi.ConnectorPageSource;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class MemoryIteratorPageSource
        implements ConnectorPageSource
{
    private final Iterator<Page> pages;
    private long completedBytes;
    private long completedPositions;
    private long readTimeNanos;
    private boolean closed;

    public MemoryIteratorPageSource(Iterator<Page> pages)
    {
        this.pages = pages;
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getCompletedPositions()
    {
        return completedPositions;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return closed || !pages.hasNext();
    }

    @Override
    public Page getNextPage()
    {
        if (isFinished()) {
            return null;
        }
        long start = System.nanoTime();
        Page p;
        try {
            p = pages.next();
            completedBytes += p.getSizeInBytes();
            completedPositions += p.getPositionCount();
        }
        catch (NoSuchElementException e) {
            p = null;
        }
        long end = System.nanoTime();
        readTimeNanos += end - start;
        return p;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
            throws IOException
    {
        closed = true;
    }
}
