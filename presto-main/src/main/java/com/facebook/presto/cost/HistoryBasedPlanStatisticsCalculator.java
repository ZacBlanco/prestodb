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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.common.plan.PlanCanonicalizationStrategy;
import com.facebook.presto.dispatcher.DispatchManager;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.server.HttpRequestSessionContext;
import com.facebook.presto.server.protocol.LocalQueryProvider;
import com.facebook.presto.server.protocol.Query;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.OutputNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeWithHash;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.statistics.HistoricalPlanStatistics;
import com.facebook.presto.spi.statistics.HistoricalPlanStatisticsEntry;
import com.facebook.presto.spi.statistics.HistoryBasedPlanStatisticsProvider;
import com.facebook.presto.spi.statistics.HistoryBasedSourceInfo;
import com.facebook.presto.spi.statistics.PlanStatistics;
import com.facebook.presto.sql.planner.PlanCanonicalInfoProvider;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Provider;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import static com.facebook.presto.SystemSessionProperties.enableVerboseHistoryBasedOptimizerRuntimeStats;
import static com.facebook.presto.SystemSessionProperties.getHistoryBasedOptimizerTimeoutLimit;
import static com.facebook.presto.SystemSessionProperties.getHistoryInputTableStatisticsMatchingThreshold;
import static com.facebook.presto.SystemSessionProperties.isVerboseRuntimeStatsEnabled;
import static com.facebook.presto.SystemSessionProperties.useHistoryBasedPlanStatisticsEnabled;
import static com.facebook.presto.common.RuntimeMetricName.HISTORY_OPTIMIZER_QUERY_REGISTRATION_GET_PLAN_NODE_HASHES;
import static com.facebook.presto.common.RuntimeMetricName.HISTORY_OPTIMIZER_QUERY_REGISTRATION_GET_STATISTICS;
import static com.facebook.presto.common.RuntimeUnit.NANO;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.cost.HistoricalPlanStatisticsUtil.getSelectedHistoricalPlanStatisticsEntry;
import static com.facebook.presto.cost.HistoryBasedPlanStatisticsManager.historyBasedPlanCanonicalizationStrategyList;
import static com.facebook.presto.spi.statistics.PlanStatistics.toConfidenceLevel;
import static com.facebook.presto.sql.planner.iterative.Plans.resolveGroupReferences;
import static com.facebook.presto.sql.planner.planPrinter.PlanPrinter.jsonFragmentPlan;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.graph.Traverser.forTree;
import static io.airlift.units.DataSize.succinctDataSize;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class HistoryBasedPlanStatisticsCalculator
        implements StatsCalculator
{
    private static final com.facebook.airlift.log.Logger log = Logger.get(HistoryBasedPlanStatisticsCalculator.class);

    private final Supplier<HistoryBasedPlanStatisticsProvider> historyBasedPlanStatisticsProvider;
    private final HistoryBasedStatisticsCacheManager historyBasedStatisticsCacheManager;
    private final StatsCalculator delegate;
    private final PlanCanonicalInfoProvider planCanonicalInfoProvider;
    private final Provider<DispatchManager> dispatchManagerProvider;
    private final Provider<LocalQueryProvider> queryProvider;
    private final Provider<QueryManager> queryManagerProvider;
    private final Metadata metadata;

    public HistoryBasedPlanStatisticsCalculator(
            Supplier<HistoryBasedPlanStatisticsProvider> historyBasedPlanStatisticsProvider,
            HistoryBasedStatisticsCacheManager historyBasedStatisticsCacheManager,
            StatsCalculator delegate,
            PlanCanonicalInfoProvider planCanonicalInfoProvider,
            Provider<DispatchManager> dispatchManagerProvider,
            Provider<LocalQueryProvider> queryProvider,
            Provider<QueryManager> queryManagerProvider,
            Metadata metadata)
    {
        this.historyBasedPlanStatisticsProvider = requireNonNull(historyBasedPlanStatisticsProvider, "historyBasedPlanStatisticsProvider is null");
        this.historyBasedStatisticsCacheManager = requireNonNull(historyBasedStatisticsCacheManager, "historyBasedStatisticsCacheManager is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.planCanonicalInfoProvider = requireNonNull(planCanonicalInfoProvider, "planHasher is null");
        this.dispatchManagerProvider = dispatchManagerProvider;
        this.queryProvider = queryProvider;
        this.queryManagerProvider = queryManagerProvider;
        this.metadata = metadata;
    }

    @Override
    public PlanNodeStatsEstimate calculateStats(PlanNode node, StatsProvider sourceStats, Lookup lookup, Session session, TypeProvider types)
    {
        PlanNodeStatsEstimate delegateStats = delegate.calculateStats(node, sourceStats, lookup, session, types);
        return getStatistics(node, session, lookup, delegateStats);
    }

    @Override
    public boolean registerPlan(PlanNode root, Session session, long startTimeInNano, long timeoutInMilliseconds)
    {
        // If previous registration timeout for this query, this run is likely to timeout too, return false.
        if (historyBasedStatisticsCacheManager.historyBasedQueryRegistrationTimeout(session.getQueryId())) {
            return false;
        }
        // record the statsEquivalentPlanNode of root node, and do serialization if enabled when query completes to avoid introduce additional latency for HBO optimizer
        if (root.getStatsEquivalentPlanNode().isPresent()) {
            historyBasedStatisticsCacheManager.setStatsEquivalentPlanRootNode(session.getQueryId(), root.getStatsEquivalentPlanNode().get());
        }
        ImmutableList.Builder<PlanNodeWithHash> planNodesWithHash = ImmutableList.builder();
        Iterable<PlanNode> planNodeIterable = forTree(PlanNode::getSources).depthFirstPreOrder(root);
        boolean enableVerboseRuntimeStats = isVerboseRuntimeStatsEnabled(session) || enableVerboseHistoryBasedOptimizerRuntimeStats(session);
        long profileStartTime = 0;
        for (PlanNode plan : planNodeIterable) {
            if (checkTimeOut(startTimeInNano, timeoutInMilliseconds)) {
                historyBasedStatisticsCacheManager.setHistoryBasedQueryRegistrationTimeout(session.getQueryId());
                return false;
            }
            if (plan.getStatsEquivalentPlanNode().isPresent()) {
                if (enableVerboseRuntimeStats) {
                    profileStartTime = System.nanoTime();
                }
                planNodesWithHash.addAll(getPlanNodeHashes(plan, session, false).values());
                if (enableVerboseRuntimeStats) {
                    session.getRuntimeStats().addMetricValue(HISTORY_OPTIMIZER_QUERY_REGISTRATION_GET_PLAN_NODE_HASHES, NANO, System.nanoTime() - profileStartTime);
                }
            }
        }
        try {
            if (enableVerboseRuntimeStats) {
                profileStartTime = System.nanoTime();
            }
            historyBasedStatisticsCacheManager.getStatisticsCache(session.getQueryId(), historyBasedPlanStatisticsProvider, getHistoryBasedOptimizerTimeoutLimit(session).toMillis()).getAll(planNodesWithHash.build());
            if (enableVerboseRuntimeStats) {
                session.getRuntimeStats().addMetricValue(HISTORY_OPTIMIZER_QUERY_REGISTRATION_GET_STATISTICS, NANO, System.nanoTime() - profileStartTime);
            }
        }
        catch (ExecutionException e) {
            throw new RuntimeException("Unable to register plan: ", e.getCause());
        }

        if (checkTimeOut(startTimeInNano, timeoutInMilliseconds)) {
            historyBasedStatisticsCacheManager.setHistoryBasedQueryRegistrationTimeout(session.getQueryId());
        }
        // Return true even if get empty history statistics, so that HistoricalStatisticsEquivalentPlanMarkingOptimizer still return the plan with StatsEquivalentPlanNode which
        // will be used in populating history statistics
        return true;
    }

    private boolean checkTimeOut(long startTimeInNano, long timeoutInMilliseconds)
    {
        return NANOSECONDS.toMillis(System.nanoTime() - startTimeInNano) > timeoutInMilliseconds;
    }

    @VisibleForTesting
    public PlanCanonicalInfoProvider getPlanCanonicalInfoProvider()
    {
        return planCanonicalInfoProvider;
    }

    @VisibleForTesting
    public StatsCalculator getDelegate()
    {
        return delegate;
    }

    @VisibleForTesting
    public Supplier<HistoryBasedPlanStatisticsProvider> getHistoryBasedPlanStatisticsProvider()
    {
        return historyBasedPlanStatisticsProvider;
    }

    private Map<PlanCanonicalizationStrategy, PlanNodeWithHash> getPlanNodeHashes(PlanNode plan, Session session, boolean cacheOnly)
    {
        if (!useHistoryBasedPlanStatisticsEnabled(session) || !plan.getStatsEquivalentPlanNode().isPresent()) {
            return ImmutableMap.of();
        }

        PlanNode statsEquivalentPlanNode = plan.getStatsEquivalentPlanNode().get();
        ImmutableMap.Builder<PlanCanonicalizationStrategy, PlanNodeWithHash> allHashesBuilder = ImmutableMap.builder();
        for (PlanCanonicalizationStrategy strategy : historyBasedPlanCanonicalizationStrategyList(session)) {
            Optional<String> hash = planCanonicalInfoProvider.hash(session, statsEquivalentPlanNode, strategy, cacheOnly);
            if (hash.isPresent()) {
                allHashesBuilder.put(strategy, new PlanNodeWithHash(statsEquivalentPlanNode, hash));
            }
        }

        return allHashesBuilder.build();
    }

    private PlanNodeStatsEstimate getStatistics(PlanNode planNode, Session session, Lookup lookup, PlanNodeStatsEstimate delegateStats)
    {
        if (!useHistoryBasedPlanStatisticsEnabled(session)) {
            return delegateStats;
        }

        PlanNode plan = resolveGroupReferences(planNode, lookup);
        Map<PlanCanonicalizationStrategy, PlanNodeWithHash> allHashes = getPlanNodeHashes(plan, session, true);

        Map<PlanNodeWithHash, HistoricalPlanStatistics> statistics = ImmutableMap.of();
        try {
            statistics = historyBasedStatisticsCacheManager
                    .getStatisticsCache(session.getQueryId(), historyBasedPlanStatisticsProvider, getHistoryBasedOptimizerTimeoutLimit(session).toMillis())
                    .getAll(allHashes.values().stream().distinct().collect(toImmutableList()));
        }
        catch (ExecutionException e) {
            throw new RuntimeException(format("Unable to get plan statistics for %s", planNode), e.getCause());
        }
        double historyMatchingThreshold = getHistoryInputTableStatisticsMatchingThreshold(session);
        // Return statistics corresponding to first strategy that we find, in order specified by `historyBasedPlanCanonicalizationStrategyList`
        for (PlanCanonicalizationStrategy strategy : historyBasedPlanCanonicalizationStrategyList(session)) {
            for (Map.Entry<PlanNodeWithHash, HistoricalPlanStatistics> entry : statistics.entrySet()) {
                if (allHashes.containsKey(strategy) && entry.getKey().getHash().isPresent() && allHashes.get(strategy).equals(entry.getKey())) {
                    Optional<List<PlanStatistics>> inputTableStatistics = getPlanNodeInputTableStatistics(plan, session, strategy, true);
                    if (inputTableStatistics.isPresent()) {
                        Optional<HistoricalPlanStatisticsEntry> historicalPlanStatisticsEntry = getSelectedHistoricalPlanStatisticsEntry(entry.getValue(), inputTableStatistics.get(), historyMatchingThreshold);
                        if (historicalPlanStatisticsEntry.isPresent()) {
                            PlanStatistics predictedPlanStatistics = historicalPlanStatisticsEntry.get().getPlanStatistics();
                            if ((toConfidenceLevel(predictedPlanStatistics.getConfidence()).getConfidenceOrdinal() >= delegateStats.confidenceLevel().getConfidenceOrdinal())) {
                                return delegateStats.combineStats(
                                        predictedPlanStatistics,
                                        new HistoryBasedSourceInfo(entry.getKey().getHash(), inputTableStatistics, Optional.ofNullable(historicalPlanStatisticsEntry.get().getHistoricalPlanStatisticsEntryInfo())));
                            }
                        }
                        else {
                            try {
                                DispatchManager dispatchManager = dispatchManagerProvider.get();
                                LocalQueryProvider queryFactory = queryProvider.get();
                                QueryId id = dispatchManager.createQueryId();
                                String slug = randomUUID() + "_autogen";
                                if (!(plan instanceof OutputNode)) {
                                    AggregationNode aggNode = new AggregationNode(
                                            Optional.empty(),
                                            new PlanNodeId("autogen-count"),
                                            plan,
                                            ImmutableMap.of(new VariableReferenceExpression(Optional.empty(), "count", BIGINT),
                                                    new AggregationNode.Aggregation(
                                                            new CallExpression(
                                                                    "count",
                                                                    metadata.getFunctionAndTypeManager().lookupFunction("count", ImmutableList.of()),
                                                                    BIGINT,
                                                                    ImmutableList.of()),
                                                            Optional.empty(),
                                                            Optional.empty(),
                                                            false,
                                                            Optional.empty())),
                                            new AggregationNode.GroupingSetDescriptor(ImmutableList.of(), 1, ImmutableSet.of(1)),
                                            ImmutableList.of(),
                                            AggregationNode.Step.SINGLE,
                                            Optional.empty(),
                                            Optional.empty(),
                                            Optional.empty());
                                    plan = new OutputNode(Optional.empty(), new PlanNodeId("autogen-output"), aggNode, ImmutableList.of(), ImmutableList.of());
                                }

                                String jsonPlan = jsonFragmentPlan(plan, VariablesExtractor.extractOutputVariables(plan), StatsAndCosts.empty(), metadata.getFunctionAndTypeManager(), session);
                                Future<?> f = dispatchManager.createQuery(id, slug, 0, session.getSessionContext(), jsonPlan, Optional.of(plan));
                                f.get(); // wait for creation
                                dispatchManager.waitForDispatched(id).get();
                                Query query = queryFactory.getQuery(id, slug);
                                if (!(session.getSessionContext() instanceof HttpRequestSessionContext)) {
                                    continue;
                                }
                                HttpRequestSessionContext requestSessionContext = (HttpRequestSessionContext) session.getSessionContext();
                                Future<?> results = query.waitForResults(0, requestSessionContext.getUriInfo(), requestSessionContext.getUriInfo().getBaseUri().getScheme(), Duration.succinctDuration(10, SECONDS), succinctDataSize(1, DataSize.Unit.MEGABYTE), false);
                                results.get();
                            }
                            catch (Exception e) {
                                log.error("Failed to submit history query: " + e.getMessage(), e);
                            }
                        }
                    }
                }
            }
        }

        return delegateStats;
    }

    private Optional<List<PlanStatistics>> getPlanNodeInputTableStatistics(PlanNode plan, Session session, PlanCanonicalizationStrategy strategy, boolean cacheOnly)
    {
        if (!useHistoryBasedPlanStatisticsEnabled(session) || !plan.getStatsEquivalentPlanNode().isPresent()) {
            return Optional.empty();
        }

        PlanNode statsEquivalentPlanNode = plan.getStatsEquivalentPlanNode().get();
        return planCanonicalInfoProvider.getInputTableStatistics(session, statsEquivalentPlanNode, strategy, cacheOnly);
    }
}
