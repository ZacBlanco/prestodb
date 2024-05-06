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
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.IntersectNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Lookup;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.shouldOptimizerUseHistograms;
import static com.facebook.presto.sql.planner.plan.Patterns.intersect;
import static com.google.common.base.Preconditions.checkArgument;

public class IntersectStatsRule
        extends SimpleStatsRule<IntersectNode>
{
    private static final Pattern<IntersectNode> PATTERN = intersect();

    public IntersectStatsRule(StatsNormalizer normalizer)
    {
        super(normalizer);
    }

    @Override
    public Pattern<IntersectNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    protected Optional<PlanNodeStatsEstimate> doCalculate(
            IntersectNode node, StatsProvider statsProvider, Lookup lookup, Session session, TypeProvider types)
    {
        checkArgument(!node.getSources().isEmpty(), "Empty intersection is not supported");

        Optional<PlanNodeStatsEstimate> estimate = Optional.empty();
        for (int i = 0; i < node.getSources().size(); i++) {
            PlanNode source = node.getSources().get(i);
            PlanNodeStatsEstimate sourceStats = statsProvider.getStats(source);

            PlanNodeStatsEstimate sourceStatsWithMappedSymbols = mapToOutputSymbols(sourceStats, node, i);

            if (estimate.isPresent()) {
                PlanNodeStatsEstimateMath calculator = new PlanNodeStatsEstimateMath(shouldOptimizerUseHistograms(session));
                estimate = Optional.of(calculator.addStatsAndIntersect(estimate.get(), sourceStatsWithMappedSymbols));
            }
            else {
                estimate = Optional.of(sourceStatsWithMappedSymbols);
            }
        }

        return estimate;
    }

    private PlanNodeStatsEstimate mapToOutputSymbols(PlanNodeStatsEstimate estimate, IntersectNode node, int index)
    {
        PlanNodeStatsEstimate.Builder mapped = PlanNodeStatsEstimate.builder().setOutputRowCount(estimate.getOutputRowCount());
        node.getOutputVariables().forEach(variable -> mapped.addVariableStatistics(
                variable, estimate.getVariableStatistics(node.getVariableMapping().get(variable).get(index))));

        return mapped.build();
    }
}
