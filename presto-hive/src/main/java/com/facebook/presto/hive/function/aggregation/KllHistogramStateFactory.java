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
package com.facebook.presto.hive.function.aggregation;

import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.AccumulatorStateMetadata;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.memory.Memory;

public class KllHistogramStateFactory
        implements AccumulatorStateFactory<KllHistogramStateFactory.StatisticalKllHistogram>
{
    @Override
    public StatisticalKllHistogram createSingleState()
    {
        return new StatisticalKllHistogram(KllDoublesSketch.newHeapInstance());
    }

    @Override
    public Class<? extends StatisticalKllHistogram> getSingleStateClass()
    {
        return StatisticalKllHistogram.class;
    }

    @Override
    public StatisticalKllHistogram createGroupedState()
    {
        return new StatisticalKllHistogram(KllDoublesSketch.newHeapInstance());
    }

    @Override
    public Class<? extends StatisticalKllHistogram> getGroupedStateClass()
    {
        return StatisticalKllHistogram.class;
    }

    @AccumulatorStateMetadata(stateSerializerClass = KllHistogramStateSerializer.class, stateFactoryClass = KllHistogramStateFactory.class)
    public static class StatisticalKllHistogram
            implements AccumulatorState
    {
        private final KllDoublesSketch sketch;

        public StatisticalKllHistogram(KllDoublesSketch sketch)
        {
            this.sketch = sketch;
        }

        public void add(double value)
        {
            sketch.update(value);
        }

        public void merge(StatisticalKllHistogram other)
        {
            sketch.merge(other.sketch);
        }

        public long getSize()
        {
            return sketch.getN();
        }

        public long estimatedMemorySizeInBytes()
        {
            return sketch.getSerializedSizeBytes();
        }

        public Slice serialize()
        {
            return Slices.wrappedBuffer(sketch.toByteArray());
        }

        public boolean isNull()
        {
            return sketch == null;
        }

        public static StatisticalKllHistogram fromSlice(Slice src)
        {
            return new StatisticalKllHistogram(KllDoublesSketch.wrap(Memory.wrap(src.getBytes())));
        }

        @Override
        public long getEstimatedSize()
        {
            return estimatedMemorySizeInBytes();
        }
    }
}
