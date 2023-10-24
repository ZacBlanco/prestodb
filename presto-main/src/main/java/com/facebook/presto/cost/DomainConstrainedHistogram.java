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

import static com.facebook.presto.cost.HistogramCalculator.calculateRangeOverlap;
import static com.facebook.presto.util.MoreMath.max;
import static com.facebook.presto.util.MoreMath.min;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verify;
import static java.lang.Double.isFinite;
import static java.lang.Double.isNaN;

public class DomainConstrainedHistogram
        implements ConnectorHistogram
{
    private final StatisticRange range;
    private final ConnectorHistogram source;
    private final Estimate modifier;

    @JsonCreator
    public DomainConstrainedHistogram(
            @JsonProperty("domain") StatisticRange range,
            @JsonProperty("source") ConnectorHistogram source)
    {
        this.range = range;
        this.source = source;
        this.modifier = calculateRangeOverlap(range, source);
    }

    @Override
    public Estimate cumulativeProbability(double value)
    {
        if (isNaN(value)) {
            return Estimate.unknown();
        }

        if (range.length() == 0.0) {
            if (value >= range.getHigh()) {
                return Estimate.of(1.0);
            }
            else {
                return Estimate.of(0.0);
            }
        }
        else {
            return modifier.flatMap(mod ->
                            source.cumulativeProbability(value)
                                    .flatMap(est -> source.cumulativeProbability(range.getLow())
                                            .map(lowPercent -> est - lowPercent))
                                    .map(percent -> min(1.0, max(0.0, percent / mod))))
                    .or(() -> {
                        if (!isFinite(range.length())) {
                            return Estimate.unknown();
                        }
                        return Estimate.of(min(1.0, max(0.0, (value - range.getLow()) / range.length())));
                    });
        }
    }

    @Override
    public Estimate inverseCumulativeProbability(double percentile)
    {
        verify(percentile >= 0.0 && percentile <= 1.0, "percentile must be in [0.0, 1.0]");
        if (percentile == 0.0 && isFinite(range.getLow())) {
            return Estimate.of(range.getLow());
        }

        if (percentile == 1.0 && isFinite(range.getHigh())) {
            return Estimate.of(range.getHigh());
        }

        return modifier.flatMap(mod -> source.cumulativeProbability(range.getLow())
                        .flatMap(lowPercentile ->
                                source.inverseCumulativeProbability(min(1.0, max(0.0, lowPercentile + (percentile * mod))))))
                .or(() -> {
                    //in the case the source results in unknown values, then assume a uniform
                    // distribution in the domain range
                    if (!isFinite(range.length())) {
                        return Estimate.unknown();
                    }
                    return Estimate.of(range.getLow() + (percentile * range.length()));
                });
    }

    @Override
    public Estimate cumulativeDistinctValues(double percentile)
    {
        verify(percentile >= 0.0 && percentile <= 1.0, "percentile must be in [0.0, 1.0]");
        // distinct values of percentile - distinct values at low
        return modifier.flatMap(mod ->
                source.cumulativeProbability(range.getLow()).flatMap(lowPercent -> {
                    Estimate lowDistinctValue = source.cumulativeDistinctValues(lowPercent);
                    Estimate distinctValueSource = source.cumulativeDistinctValues(lowPercent + (percentile * mod));
                    return lowDistinctValue.flatMap(lowDistinct -> distinctValueSource.map(distinctValues -> distinctValues - lowDistinct));
                }));
    }

    @Override
    public Estimate distinctValues()
    {
        Estimate highDistinct = source.cumulativeProbability(range.getHigh())
                .flatMap(source::cumulativeDistinctValues);
        Estimate lowDistinct = source.cumulativeProbability(range.getLow())
                .flatMap(source::cumulativeDistinctValues);
        return highDistinct.flatMap(high -> lowDistinct.map(low -> high - low))
                .map(value -> min(range.getDistinctValuesCount(), max(1.0, value)))
                .or(() -> Estimate.of(range.getDistinctValuesCount()));
    }

    @JsonProperty
    public ConnectorHistogram getSource()
    {
        return source;
    }

    @JsonProperty
    public StatisticRange getRange()
    {
        return range;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("range", range)
                .add("source", source)
                .toString();
    }
}
