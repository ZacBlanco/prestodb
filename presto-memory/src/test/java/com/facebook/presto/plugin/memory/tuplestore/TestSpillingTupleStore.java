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
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.google.common.collect.Streams;
import io.airlift.units.DataSize;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static io.airlift.units.DataSize.succinctDataSize;

@Test(singleThreaded = true)
public class TestSpillingTupleStore
{
    SpillingTupleStore spillingTupleStore;

    private static final DataSize MEM_LIMIT = succinctDataSize(1, KILOBYTE);
    private static final DataSize SPILL_LIMIT = succinctDataSize(2, KILOBYTE);

    @BeforeMethod
    public void before()
    {
        spillingTupleStore = new SpillingTupleStore(
                4,
                null,
                MEM_LIMIT,
                SPILL_LIMIT);
    }

    @AfterMethod
    public void after()
            throws Exception
    {
        spillingTupleStore.close();
    }

    @DataProvider
    private Object[][] pageProvider()
    {
        return new Object[][] {
                // below memory limit
                {succinctDataSize((double) MEM_LIMIT.toBytes() / 2, BYTE), 1},
                {succinctDataSize((double) MEM_LIMIT.toBytes() / 2, BYTE), 10},
                // at memory limit
                {MEM_LIMIT, 1},
                {MEM_LIMIT, 10},
                // between memory and spill limit (requires reading in-progress spill file)
                {succinctBytes(MEM_LIMIT.toBytes() + (MEM_LIMIT.toBytes() / 2)), 1},
                {succinctBytes(MEM_LIMIT.toBytes() + (MEM_LIMIT.toBytes() / 2)), 2},
                {succinctBytes(MEM_LIMIT.toBytes() + (MEM_LIMIT.toBytes() / 2)), 10},
                // at spill limit
                {SPILL_LIMIT, 1},
                {SPILL_LIMIT, 2},
                {SPILL_LIMIT, 10},
                // above spill limit, may require reading memory, spill file
                {succinctBytes(SPILL_LIMIT.toBytes() + MEM_LIMIT.toBytes() / 2), 1},
                {succinctBytes(SPILL_LIMIT.toBytes() + MEM_LIMIT.toBytes() / 2), 2},
                {succinctBytes(SPILL_LIMIT.toBytes() + MEM_LIMIT.toBytes() / 2), 10},
                // above spill and memory limit, may require reading memory, spill file, and in-progress spill
                {succinctBytes(SPILL_LIMIT.toBytes() + MEM_LIMIT.toBytes() * 2), 1},
                {succinctBytes(SPILL_LIMIT.toBytes() + MEM_LIMIT.toBytes() * 2), 2},
                {succinctBytes(SPILL_LIMIT.toBytes() + MEM_LIMIT.toBytes() * 2), 10},
                // mutiple of spill limit, should have multiple spill files
                {succinctBytes(SPILL_LIMIT.toBytes() * 3), 1},
                {succinctBytes(SPILL_LIMIT.toBytes() * 3), 2},
                {succinctBytes(SPILL_LIMIT.toBytes() * 3), 10},
        };
    }

    @Test(dataProvider = "pageProvider")
    public void testPageCounts(DataSize size, int numPages)
    {
        List<Page> pages = createPages(size, numPages);
        spillingTupleStore.addPages(pages.iterator()).join();
        checkPagePositionsEqual(
                Streams.stream(spillingTupleStore.readPages()),
                pages.stream());
    }

    private void checkPagePositionsEqual(Stream<Page> actual, Stream<Page> expected)
    {
        assertEquals(
                actual.mapToLong(Page::getPositionCount).sum(),
                expected.mapToLong(Page::getPositionCount).sum());
    }

    /**
     * Create a page roughly of the requested size.
     *
     * @param size
     * @return
     */
    private static Page createPage(DataSize size)
    {
        int entries = (int) size.toBytes() / BIGINT.getFixedSize();
        BlockBuilder builder = BIGINT.createBlockBuilder(null, entries);
        LongStream.range(0, entries).forEach(builder::writeLong);
        return Page.wrapBlocksWithoutCopy(entries, new Block[] {builder.build()});
    }

    /**
     * Create a stream page roughly of the requested size.
     *
     * @param size
     * @return
     */
    private static List<Page> createPages(DataSize size, int numPages)
    {
        DataSize sizePerPage = succinctBytes(size.toBytes() / numPages);

        return Stream.concat(
                        Stream.of(createPage(succinctBytes(size.toBytes() % numPages)))
                                .filter(page -> page.getPositionCount() > 0),
                        IntStream.range(0, size.toBytes() % numPages == 0 ? numPages : numPages - 1)
                                .mapToObj(unused -> createPage(sizePerPage)))
                .collect(Collectors.toList());
    }
}
