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

import com.facebook.presto.common.Page;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

public interface TupleStore
        extends AutoCloseable
{
    /**
     * Adds a set of pages to the tuple store.
     *
     * @param pages an iterator over the set of pages to add.
     * @return a future indicating when the set of pages has finished committing
     * to the tuplestore. All reads after this will include the submitted pages.
     */
    CompletableFuture<?> addPages(Iterator<Page> pages);

    /**
     * Retrieves an iterator over the tuple store which returns the entire set
     * of committed pages.
     *
     * @return an iterator over the set of available pages.
     */
    Iterator<Page> readPages();

    /**
     * Attempt to flush any in-memory data to disk if possible.
     *
     * @return a future which is completed when the tuple store is finished
     * flushing
     */
    CompletableFuture<?> flush();

    /**
     * @return the store's current memory utilization
     */
    long getMemoryUtilization();
}
