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
package com.facebook.presto.hive.function;

import com.facebook.presto.hive.types.KllHistogramType;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.memory.Memory;

import static com.facebook.presto.common.type.StandardTypes.DOUBLE;

public class KllQuantileFunction
{
    private KllQuantileFunction() {}

    @ScalarFunction("kll_rank")
    @SqlType(DOUBLE)
    public static double kllQuantile(@SqlType(KllHistogramType.KLL_HISTOGRAM_TYPE) Slice histogram, @SqlType(DOUBLE) double rank)
    {
        KllDoublesSketch sketch = KllDoublesSketch.wrap(Memory.wrap(histogram.byteArray()));
        return sketch.getQuantile(rank);
    }

    @ScalarFunction("kll_quantile")
    @SqlType(DOUBLE)
    public static double kllrank(@SqlType(KllHistogramType.KLL_HISTOGRAM_TYPE) Slice histogram, @SqlType(DOUBLE) double quantile)
    {
        KllDoublesSketch sketch = KllDoublesSketch.wrap(Memory.wrap(histogram.byteArray()));
        return sketch.getRank(quantile);
    }
}
