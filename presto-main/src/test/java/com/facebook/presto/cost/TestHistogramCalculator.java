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
import org.testng.annotations.Test;

import static com.facebook.presto.cost.HistogramCalculator.calculateFilterFactor;
import static com.facebook.presto.cost.HistogramCalculator.calculateRangeOverlap;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static org.testng.Assert.assertEquals;

public class TestHistogramCalculator
{
    @Test
    public void testCalculateFilterFactor()
    {
        StatisticRange zeroToTen = range(0, 10, 10);
        StatisticRange empty = StatisticRange.empty();

        // Equal ranges
        assertFilterFactor(zeroToTen, uniformHist(0, 10, 5), Estimate.of(1.0));
        assertFilterFactor(zeroToTen, uniformHist(0, 10, 20), Estimate.of(1.0));

        // Some overlap
        assertFilterFactor(range(5, 3000, 5), uniformHist(zeroToTen), Estimate.of(0.5));

        // Single value overlap
        assertFilterFactor(range(3, 3, 1), uniformHist(zeroToTen), Estimate.of(1.0 / zeroToTen.getDistinctValuesCount()));
        assertFilterFactor(range(10, 100, 357), uniformHist(zeroToTen), Estimate.of(1.0 / zeroToTen.getDistinctValuesCount()));

        // No overlap
        assertFilterFactor(range(20, 30, 10), uniformHist(zeroToTen), Estimate.zero());

        // Empty ranges
        assertFilterFactor(zeroToTen, uniformHist(empty), Estimate.zero());
        assertFilterFactor(empty, uniformHist(zeroToTen), Estimate.zero());

        // no test for (empty, empty) since any return value is correct
        assertFilterFactor(unboundedRange(10), uniformHist(empty), Estimate.zero());
        assertFilterFactor(empty, uniformHist(unboundedRange(10)), Estimate.zero());

        // Unbounded (infinite), NDV-based
        assertFilterFactor(unboundedRange(10), uniformHist(unboundedRange(20)), Estimate.of(0.5));
        assertFilterFactor(unboundedRange(20), uniformHist(unboundedRange(10)), Estimate.of(1.0));

        // NEW TESTS (TPC-H Q2)
        // unbounded ranges
        assertFilterFactor(unboundedRange(0.5), uniformHist(unboundedRange(NaN)), Estimate.of(.5));
        // unbounded ranges with limited distinct values
        assertFilterFactor(unboundedRange(1.0),
                domainConstrained(unboundedRange(5.0), uniformHist(unboundedRange(7.0))),
                Estimate.of(0.2));
    }

    @Test
    public void testCalculateOverlapPercent()
    {
        // left edge over hist range
        assertRangeOverlap(Estimate.of(0.0), range(-10, -5), uniformHist(range(0, 10)));
        assertRangeOverlap(Estimate.of(0.0), range(-10, 0), uniformHist(range(0, 10)));
        assertRangeOverlap(Estimate.of(0.1), range(-5, 1), uniformHist(range(0, 10)));
        assertRangeOverlap(Estimate.of(0.1), range(-4, 1), uniformHist(range(0, 10)));

        // right edge over hist range
        assertRangeOverlap(Estimate.of(0.0), range(11, 15), uniformHist(range(0, 10)));
        assertRangeOverlap(Estimate.of(0.0), range(10, 15), uniformHist(range(0, 10)));
        assertRangeOverlap(Estimate.of(0.1), range(9, 15), uniformHist(range(0, 10)));
        assertRangeOverlap(Estimate.of(0.1), range(9, 14), uniformHist(range(0, 10)));

        // single edge within, other edge on outer limit
        assertRangeOverlap(Estimate.of(0.5), range(5, 10), uniformHist(range(0, 10)));
        assertRangeOverlap(Estimate.of(0.5), range(0, 5), uniformHist(range(0, 10)));

        // infinite ranges
        assertRangeOverlap(Estimate.of(1.0), range(NEGATIVE_INFINITY, POSITIVE_INFINITY), uniformHist(range(0, 10)));
        assertRangeOverlap(Estimate.of(0.5), range(5, POSITIVE_INFINITY), uniformHist(range(0, 10)));
        assertRangeOverlap(Estimate.of(0.5), range(NEGATIVE_INFINITY, 5), uniformHist(range(0, 10)));

        // single value -- since it's not a range, the overlap should be 0%
        assertRangeOverlap(Estimate.of(0.0), range(5, 5), uniformHist(range(0, 10)));
        assertRangeOverlap(Estimate.of(0.0), range(0, 0), uniformHist(range(0, 10)));
        assertRangeOverlap(Estimate.of(0.0), range(10, 10), uniformHist(range(0, 10)));

        // "common" case
        assertRangeOverlap(Estimate.of(0.3), range(2, 5), uniformHist(range(0, 10)));

        // cases where histogram has unbounded range
        // -inf
        assertRangeOverlap(Estimate.unknown(), range(0, 10), uniformHist(range(NEGATIVE_INFINITY, 10)));
        assertRangeOverlap(Estimate.unknown(), range(9, 10), uniformHist(range(NEGATIVE_INFINITY, 10)));
        assertRangeOverlap(Estimate.of(0.0), range(10, 10), uniformHist(range(NEGATIVE_INFINITY, 10)));
        assertRangeOverlap(Estimate.of(0.0), range(10, 12), uniformHist(range(NEGATIVE_INFINITY, 10)));
        assertRangeOverlap(Estimate.of(0.0), range(11, 12), uniformHist(range(NEGATIVE_INFINITY, 10)));
        // +inf
        assertRangeOverlap(Estimate.unknown(), range(0, 10), uniformHist(range(0, POSITIVE_INFINITY)));
        assertRangeOverlap(Estimate.unknown(), range(0, 1), uniformHist(range(0, POSITIVE_INFINITY)));
        assertRangeOverlap(Estimate.of(0.0), range(-10, 0), uniformHist(range(0, POSITIVE_INFINITY)));
        assertRangeOverlap(Estimate.of(0.0), range(-12, -10), uniformHist(range(0, POSITIVE_INFINITY)));
        assertRangeOverlap(Estimate.of(0.0), range(-12, -11), uniformHist(range(0, POSITIVE_INFINITY)));
    }

    private static StatisticRange range(double low, double high, double distinctValues)
    {
        return new StatisticRange(low, high, distinctValues);
    }

    private static StatisticRange range(double low, double high)
    {
        return new StatisticRange(low, high, NaN);
    }

    private static StatisticRange unboundedRange(double distinctValues)
    {
        return new StatisticRange(NEGATIVE_INFINITY, POSITIVE_INFINITY, distinctValues);
    }

    private static void assertFilterFactor(StatisticRange range, ConnectorHistogram histogram, Estimate expected)
    {
        assertEquals(calculateFilterFactor(range, histogram), expected);
    }

    private static void assertRangeOverlap(Estimate expected, StatisticRange range, ConnectorHistogram histogram)
    {
        assertEquals(calculateRangeOverlap(range, histogram), expected);
    }

    private static ConnectorHistogram uniformHist(StatisticRange range)
    {
        return uniformHist(range.getLow(), range.getHigh(), range.getDistinctValuesCount());
    }

    private static ConnectorHistogram uniformHist(double low, double high, double distinct)
    {
        return new UniformDistributionHistogram(low, high, distinct);
    }

    private static ConnectorHistogram domainConstrained(StatisticRange range, ConnectorHistogram source)
    {
        return new DomainConstrainedHistogram(range, source);
    }
}
