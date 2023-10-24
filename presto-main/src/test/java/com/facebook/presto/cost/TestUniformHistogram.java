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
import com.google.common.base.VerifyException;
import org.apache.commons.math3.distribution.RealDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;
import org.testng.annotations.Test;

import static java.lang.Double.POSITIVE_INFINITY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TestUniformHistogram
        extends TestHistogram
{
    ConnectorHistogram createHistogram(double distinctValues)
    {
        return new UniformDistributionHistogram(0, 1, distinctValues);
    }

    RealDistribution getDistribution()
    {
        return new UniformRealDistribution();
    }

    @Test
    public void testInvalidConstruction()
    {
        assertThrows(VerifyException.class, () -> new UniformDistributionHistogram(2.0, 1.0, 2.0));
    }

    @Test
    public void testSingleValueRangeDistinct()
    {
        assertEquals(new UniformDistributionHistogram(1.0, 1.0, 2.0).getDistinctValuesCount().getValue(), 1.0);
    }

    @Test
    public void testNanRangeValues()
    {
        ConnectorHistogram hist = new UniformDistributionHistogram(Double.NaN, 2, 2);
        assertTrue(hist.inverseCumulativeProbability(0.5).isUnknown());
        assertEquals(hist.cumulativeDistinctValues(0.5).getValue(), 1.0);

        hist = new UniformDistributionHistogram(1.0, Double.NaN, 2);
        assertTrue(hist.inverseCumulativeProbability(0.5).isUnknown());
        assertEquals(hist.cumulativeDistinctValues(0.5).getValue(), 1.0);

        hist = new UniformDistributionHistogram(1.0, 2.0, Double.NaN);
        assertEquals(hist.inverseCumulativeProbability(0.5).getValue(), 1.5);
        assertTrue(hist.cumulativeDistinctValues(0.5).isUnknown());
    }

    @Test
    public void testInfiniteRangeValues()
    {
        // test low value as infinite
        ConnectorHistogram hist = new UniformDistributionHistogram(Double.NEGATIVE_INFINITY, 2, 2);

        assertTrue(hist.inverseCumulativeProbability(0.5).isUnknown());
        assertEquals(hist.inverseCumulativeProbability(0.0), Estimate.unknown());
        assertEquals(hist.inverseCumulativeProbability(1.0).getValue(), 2.0);

        assertEquals(hist.cumulativeProbability(0.0), Estimate.unknown());
        assertEquals(hist.cumulativeProbability(1.0), Estimate.unknown());
        assertEquals(hist.cumulativeProbability(2.0).getValue(), 1.0);
        assertEquals(hist.cumulativeProbability(2.5).getValue(), 1.0);

        assertEquals(hist.cumulativeDistinctValues(0.5).getValue(), 1.0);

        // test high value as infinite
        hist = new UniformDistributionHistogram(1.0, POSITIVE_INFINITY, 2);

        assertTrue(hist.inverseCumulativeProbability(0.5).isUnknown());
        assertEquals(hist.inverseCumulativeProbability(0.0).getValue(), 1.0);
        assertEquals(hist.inverseCumulativeProbability(1.0), Estimate.unknown());

        assertEquals(hist.cumulativeProbability(0.0).getValue(), 0.0);
        assertEquals(hist.cumulativeProbability(1.0).getValue(), 0.0);
        assertEquals(hist.cumulativeProbability(1.5), Estimate.unknown());

        assertEquals(hist.cumulativeDistinctValues(0.5).getValue(), 1.0);
        assertEquals(hist.cumulativeDistinctValues(0.0).getValue(), 0.0);
        assertEquals(hist.cumulativeDistinctValues(1.0).getValue(), 2.0);
    }

    @Test
    public void testSingleValueRange()
    {
        UniformDistributionHistogram hist = new UniformDistributionHistogram(1.0, 1.0, 1.0);

        assertEquals(hist.inverseCumulativeProbability(0.0).getValue(), 1.0);
        assertEquals(hist.inverseCumulativeProbability(1.0).getValue(), 1.0);
        assertEquals(hist.inverseCumulativeProbability(0.5).getValue(), 1.0);

        assertEquals(hist.cumulativeProbability(0.0).getValue(), 0.0);
        assertEquals(hist.cumulativeProbability(0.5).getValue(), 0.0);
        assertEquals(hist.cumulativeProbability(1.0).getValue(), 1.0);
        assertEquals(hist.cumulativeProbability(1.5).getValue(), 1.0);

        assertEquals(hist.distinctValues().getValue(), 1.0);
        assertEquals(hist.cumulativeDistinctValues(0.0).getValue(), 0.0);
        assertEquals(hist.cumulativeDistinctValues(0.5).getValue(), 0.0);
        assertEquals(hist.cumulativeDistinctValues(1.0).getValue(), 1.0);
    }
}
