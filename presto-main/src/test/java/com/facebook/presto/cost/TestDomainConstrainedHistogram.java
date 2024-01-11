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
import org.apache.commons.math3.distribution.RealDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;

public class TestDomainConstrainedHistogram
        extends TestHistogram
{
    @Override
    ConnectorHistogram createHistogram()
    {
        RealDistribution dist = getDistribution();
        return new DomainConstrainedHistogram(new StatisticRange(
                dist.inverseCumulativeProbability(0.0),
                dist.inverseCumulativeProbability(1.0),
                0),
                new UniformDistributionHistogram(
                        dist.inverseCumulativeProbability(0.0) - 1,
                        dist.inverseCumulativeProbability(1.0) + 1));
    }

    @Override
    RealDistribution getDistribution()
    {
        return new UniformRealDistribution();
    }

    /**
     * Domain constrained histogram doesn't support inclusive/exclusive arguments
     */
    @Override
    public void testInclusiveExclusive()
    {
    }
}
