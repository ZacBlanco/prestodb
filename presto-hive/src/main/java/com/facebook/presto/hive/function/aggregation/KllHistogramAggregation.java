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

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.hive.types.KllHistogramType;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;

import static com.facebook.presto.common.type.StandardTypes.BIGINT;
import static com.facebook.presto.common.type.StandardTypes.DOUBLE;
import static com.facebook.presto.common.type.StandardTypes.INTEGER;
import static com.facebook.presto.common.type.StandardTypes.REAL;
import static com.facebook.presto.common.type.StandardTypes.SMALLINT;
import static com.facebook.presto.common.type.StandardTypes.TINYINT;
import static com.facebook.presto.hive.types.KllHistogramType.KLL_HISTOGRAM;

@AggregationFunction("kll_histogram")
public class KllHistogramAggregation
{
    private KllHistogramAggregation() {}

    @InputFunction
    public static void input(@AggregationState KllHistogramStateFactory.StatisticalKllHistogram state, @SqlType(BIGINT) long value)
    {
        state.add(value);
    }

    @InputFunction
    public static void inputInt(@AggregationState KllHistogramStateFactory.StatisticalKllHistogram state, @SqlType(INTEGER) long value)
    {
        state.add(value);
    }

    @InputFunction
    public static void input(@AggregationState KllHistogramStateFactory.StatisticalKllHistogram state, @SqlType(SMALLINT) short value)
    {
        state.add(value);
    }

    @InputFunction
    public static void inputTiny(@AggregationState KllHistogramStateFactory.StatisticalKllHistogram state, @SqlType(TINYINT) short value)
    {
        state.add(value);
    }

    @InputFunction
    public static void inputReal(@AggregationState KllHistogramStateFactory.StatisticalKllHistogram state, @SqlType(REAL) float value)
    {
        state.add(value);
    }

    @InputFunction
    public static void inputDouble(@AggregationState KllHistogramStateFactory.StatisticalKllHistogram state, @SqlType(DOUBLE) double value)
    {
        state.add(value);
    }

    @CombineFunction
    public static void combine(@AggregationState KllHistogramStateFactory.StatisticalKllHistogram state, @AggregationState KllHistogramStateFactory.StatisticalKllHistogram otherState)
    {
        state.merge(otherState);
    }

    @OutputFunction(KllHistogramType.KLL_HISTOGRAM_TYPE)
    public static void output(@AggregationState KllHistogramStateFactory.StatisticalKllHistogram state, BlockBuilder out)
    {
        if (state.isNull()) {
            out.appendNull();
        }
        else {
            KLL_HISTOGRAM.writeSlice(out, state.serialize());
        }
    }
}
