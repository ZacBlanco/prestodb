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

package com.facebook.presto.cost;

import com.facebook.presto.spi.statistics.ConnectorHistogram;
import com.facebook.presto.spi.statistics.Estimate;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verify;
import static java.lang.Double.isInfinite;
import static java.lang.Double.isNaN;

public class UniformDistributionHistogram
        implements ConnectorHistogram
{
    private final double lowValue;
    private final double highValue;
    private final Estimate distinctValuesCount;

    @JsonCreator
    public UniformDistributionHistogram(
            @JsonProperty("lowValue") double lowValue,
            @JsonProperty("highValue") double highValue,
            @JsonProperty("distinctValuesCount") Estimate distinctValuesCount)
    {
        verify(isNaN(lowValue) || isNaN(highValue) || (lowValue <= highValue), "lowValue must be <= highValue");
        if (lowValue == highValue) {
            distinctValuesCount = Estimate.of(1.0);
        }
        this.lowValue = lowValue;
        this.highValue = highValue;
        this.distinctValuesCount = distinctValuesCount;
        // force equal range bounds to equal 1.0
    }

    public UniformDistributionHistogram(double lowValue, double highValue, double distinctValuesCount)
    {
        this(lowValue, highValue, Optional.of(distinctValuesCount)
                .filter(x -> !x.isNaN())
                .filter(x -> !x.isInfinite())
                .map(Estimate::of)
                .orElseGet(Estimate::unknown));
    }

    @JsonProperty
    public double getLowValue()
    {
        return lowValue;
    }

    @JsonProperty
    public double getHighValue()
    {
        return highValue;
    }

    @JsonProperty
    public Estimate getDistinctValuesCount()
    {
        return distinctValuesCount;
    }

    @Override
    public Estimate cumulativeProbability(double value)
    {
        if (isNaN(lowValue) ||
                isNaN(highValue) ||
                isNaN(value)) {
            return Estimate.unknown();
        }

        if (value >= highValue) {
            return Estimate.of(1.0);
        }

        if (value <= lowValue) {
            return Estimate.of(0.0);
        }

        if (isInfinite(lowValue) || isInfinite(highValue)) {
            return Estimate.unknown();
        }

        return Estimate.of((value - lowValue) / (highValue - lowValue));
    }

    @Override
    public Estimate inverseCumulativeProbability(double percentile)
    {
        verify(percentile >= 0.0 && percentile <= 1.0, "percentile must be in [0.0, 1.0]");
        if (isNaN(lowValue) ||
                isNaN(highValue)) {
            return Estimate.unknown();
        }

        if (percentile == 0.0 && !isInfinite(lowValue)) {
            return Estimate.of(lowValue);
        }

        if (percentile == 1.0 && !isInfinite(highValue)) {
            return Estimate.of(highValue);
        }

        if (isInfinite(lowValue) || isInfinite(highValue)) {
            return Estimate.unknown();
        }

        return Estimate.of(lowValue + (percentile * (highValue - lowValue)));
    }

    @Override
    public Estimate cumulativeDistinctValues(double percentile)
    {
        verify(percentile >= 0.0 && percentile <= 1.0, "percentile must be in [0.0, 1.0]");
        Estimate distinctEstimate = distinctValues();
        // case when distribution is a single value
        if (lowValue == highValue && percentile < 1.0) {
            return Estimate.zero();
        }
        return distinctEstimate.map(values -> percentile * values);
    }

    @Override
    public Estimate distinctValues()
    {
        return distinctValuesCount;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("lowValue", lowValue)
                .add("highValue", highValue)
                .add("distinctValuesCount", distinctValuesCount)
                .toString();
    }
}
