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
import com.facebook.presto.spi.statistics.UnsupportedHistogramMergeException;
import com.facebook.presto.tdigest.TDigest;
import io.airlift.slice.Slice;

import java.text.DecimalFormat;

import static java.util.Objects.requireNonNull;

public class TDigestHistogram
        implements ConnectorHistogram
{
    private final TDigest digest;

    public TDigestHistogram(Slice rawDigest)
    {
        this.digest = TDigest.createTDigest(requireNonNull(rawDigest, "rawDigest is null"));
    }

    @Override
    public Estimate cumulativeProbability(double value, boolean inclusive)
    {
        return Estimate.of(digest.getCdf(value));
    }

    @Override
    public Estimate inverseCumulativeProbability(double percentile)
    {
        return Estimate.of(digest.getQuantile(percentile));
    }

    @Override
    public ConnectorHistogram merge(ConnectorHistogram other)
            throws UnsupportedHistogramMergeException
    {
        if (other instanceof TDigestHistogram) {
            TDigest copy = TDigest.createTDigest(digest.serialize());
            copy.merge(((TDigestHistogram) other).digest);
        }
        throw new UnsupportedHistogramMergeException();
    }

    private static final DecimalFormat fmt = new DecimalFormat("#.00");

    @Override
    public String toString()
    {
        return "{" +
                "min=" + fmt.format(digest.getMin()) +
                ",p25=" + fmt.format(digest.getQuantile(0.25)) +
                ",p50=" + fmt.format(digest.getQuantile(0.5)) +
                ",p90=" + fmt.format(digest.getQuantile(0.9)) +
                ",p99=" + fmt.format(digest.getQuantile(0.99)) +
                ",max=" + fmt.format(digest.getMax()) +
                "}";
    }
}
