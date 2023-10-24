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
import com.google.common.base.VerifyException;
import org.apache.commons.math3.distribution.RealDistribution;
import org.testng.annotations.Test;

import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public abstract class TestHistogram
{
    abstract ConnectorHistogram createHistogram(double distinctValues);

    abstract RealDistribution getDistribution();

    @Test
    public void testValueAtPercentile()
    {
        ConnectorHistogram hist = createHistogram(100);
        RealDistribution dist = getDistribution();
        assertThrows(VerifyException.class, () -> hist.inverseCumulativeProbability(Double.NaN));
        assertThrows(VerifyException.class, () -> hist.inverseCumulativeProbability(-1.0));
        assertThrows(VerifyException.class, () -> hist.inverseCumulativeProbability(2.0));
        assertEquals(hist.inverseCumulativeProbability(0.0).getValue(), dist.getSupportLowerBound(), .001);
        assertEquals(hist.inverseCumulativeProbability(0.25).getValue(), dist.inverseCumulativeProbability(0.25));
        assertEquals(hist.inverseCumulativeProbability(0.5).getValue(), dist.getNumericalMean(), .001);
        assertEquals(hist.inverseCumulativeProbability(1.0).getValue(), dist.getSupportUpperBound(), .001);
    }

    @Test
    public void testPercentileAtValue()
    {
        ConnectorHistogram hist = createHistogram(100);
        RealDistribution dist = getDistribution();

        assertTrue(hist.cumulativeProbability(Double.NaN).isUnknown());
        assertEquals(hist.cumulativeProbability(NEGATIVE_INFINITY).getValue(), 0.0, .001);
        assertEquals(hist.cumulativeProbability(POSITIVE_INFINITY).getValue(), 1.0, .001);

        assertEquals(hist.cumulativeProbability(dist.getSupportLowerBound() - 1).getValue(), 0.0, .001);
        assertEquals(hist.cumulativeProbability(dist.getSupportLowerBound()).getValue(), 0.0, .001);
        assertEquals(hist.cumulativeProbability(dist.getSupportUpperBound() + 1).getValue(), 1.0, .001);
        assertEquals(hist.cumulativeProbability(dist.getSupportUpperBound()).getValue(), 1.0, .001);
        assertEquals(hist.cumulativeProbability(dist.getNumericalMean()).getValue(), 0.5, .001);
        for (int i = 0; i < 10; i++) {
            assertEquals(hist.cumulativeProbability(dist.inverseCumulativeProbability(0.1 * i)).getValue(), dist.cumulativeProbability(0.1 * i), .001);
        }
    }
}
