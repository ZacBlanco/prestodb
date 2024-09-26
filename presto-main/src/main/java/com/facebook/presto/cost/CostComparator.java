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

import com.facebook.presto.Session;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Ordering;

import javax.inject.Inject;

import static com.facebook.presto.SystemSessionProperties.getCteCostThreshold;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class CostComparator
{
    private final double cpuWeight;
    private final double memoryWeight;
    private final double networkWeight;

    @Inject
    public CostComparator(FeaturesConfig featuresConfig)
    {
        this(featuresConfig.getCpuCostWeight(), featuresConfig.getMemoryCostWeight(), featuresConfig.getNetworkCostWeight());
    }

    @VisibleForTesting
    public CostComparator(double cpuWeight, double memoryWeight, double networkWeight)
    {
        checkArgument(cpuWeight >= 0, "cpuWeight cannot be negative");
        checkArgument(memoryWeight >= 0, "memoryWeight cannot be negative");
        checkArgument(networkWeight >= 0, "networkWeight cannot be negative");
        this.cpuWeight = cpuWeight;
        this.memoryWeight = memoryWeight;
        this.networkWeight = networkWeight;
    }

    public Ordering<PlanCostEstimate> forSession(Session session)
    {
        requireNonNull(session, "session is null");
        return Ordering.from((left, right) -> this.compare(session, left, right));
    }

    protected double getCost(PlanCostEstimate estimate)
    {
        return estimate.getCpuCost() * cpuWeight
                + estimate.getMaxMemory() * memoryWeight
                + estimate.getNetworkCost() * networkWeight;
    }

    public int compare(Session session, PlanCostEstimate left, PlanCostEstimate right)
    {
        requireNonNull(session, "session is null");
        requireNonNull(left, "left is null");
        requireNonNull(right, "right is null");
        checkArgument(!left.hasUnknownComponents() && !right.hasUnknownComponents(), "cannot compare unknown costs");

        // TODO when one left.getMaxMemory() and right.getMaxMemory() exceeds query memory limit * configurable safety margin, choose the plan with lower memory usage

        double leftCost = getCost(left);
        double rightCost = getCost(right);

        return Double.compare(leftCost, rightCost);
    }

    /**
     * Compares left to right, but multiplies the right cost by some threshold value. This is useful
     * when comparing an optimized plan, but where we want the optimization to exceed a certain
     * threshold to be applied
     * <br>
     * Essentially it performs the computation {@code compare(left * threshold, right)
     *
     * @param session user session
     * @param left plan cost estimate
     * @param right plan cost estimate
     * @param threshold order of magnitude to multiply the right cost by
     * @return
     */
    public int compareWithThreshold(Session session, PlanCostEstimate left, PlanCostEstimate right)
    {
        requireNonNull(session, "session is null");
        requireNonNull(left, "left is null");
        requireNonNull(right, "right is null");
        checkArgument(!left.hasUnknownComponents() && !right.hasUnknownComponents(), "cannot compare unknown costs");

        // TODO when one left.getMaxMemory() and right.getMaxMemory() exceeds query memory limit * configurable safety margin, choose the plan with lower memory usage

        double leftCost = getCost(left);
        double rightCost = getCost(right);
        return Double.compare(leftCost * getCteCostThreshold(session), rightCost);
    }
}
