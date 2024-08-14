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

package com.facebook.presto.sql.analyzer.preplanned;

import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.analyzer.AccessControlReferences;
import com.facebook.presto.spi.analyzer.QueryAnalysis;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.sql.planner.SimplePlanVisitor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class PrePlannedQueryAnalysis
        implements QueryAnalysis
{
    private final PlanNode plan;

    public PrePlannedQueryAnalysis(PlanNode plan)
    {
        this.plan = requireNonNull(plan, "plan is null");
    }

    @Override
    public String getUpdateType()
    {
        return null;
    }

    @Override
    public Optional<String> getExpandedQuery()
    {
        return Optional.empty();
    }

    @Override
    public Map<FunctionKind, Set<String>> getInvokedFunctions()
    {
        Map<FunctionKind, Set<String>> functionKindSetMap = new HashMap<>();
        SimplePlanVisitor<Void> functionExtractor = new SimplePlanVisitor<Void>()
        {
            @Override
            public Void visitAggregation(AggregationNode node, Void context)
            {
                node.getAggregations().values().forEach(agg -> {
                    functionKindSetMap.merge(agg.getFunctionHandle().getKind(), new HashSet<>(ImmutableList.of(agg.getFunctionHandle().getName())), (key, value) -> {
                        value.add(agg.getFunctionHandle().getName());
                        return value;
                    });
                });
                return context;
            }

            @Override
            public Void visitProject(ProjectNode node, Void context)
            {
                node.getAssignments().getExpressions()
                        .stream().map(expr -> {
                            Map<FunctionKind, Set<String>> functions = new HashMap<>();
                            new RowExpressionVisitor<Void, Void>()
                            {
                                @Override
                                public Void visitExpression(RowExpression expression, Void context)
                                {
                                    expr.getChildren().forEach(child -> child.accept(this, null));
                                    return null;
                                }

                                @Override
                                public Void visitCall(CallExpression call, Void context)
                                {
                                    functions.compute(call.getFunctionHandle().getKind(), (kind, set) -> {
                                        if (set == null) {
                                            set = new HashSet<>();
                                        }
                                        set.add(call.getDisplayName());
                                        return set;
                                    });
                                    return context;
                                }
                            };
                            return functions;
                        })
                        .forEach(funcMap -> {
                            funcMap.forEach((key1, value1) -> functionKindSetMap.merge(key1, value1, (key, value) -> {
                                value.addAll(value1);
                                return value;
                            }));
                        });
                return context;
            }
        };
        functionExtractor.visitPlan(plan, null);
        return functionKindSetMap;
    }

    @Override
    public AccessControlReferences getAccessControlReferences()
    {
        return new AccessControlReferences();
    }

    @Override
    public boolean isExplainAnalyzeQuery()
    {
        return false;
    }

    @Override
    public Set<ConnectorId> extractConnectors()
    {
        ImmutableSet.Builder<ConnectorId> connectors = ImmutableSet.builder();
        SimplePlanVisitor<Void> connectorCollector = new SimplePlanVisitor<Void>()
        {
            @Override
            public Void visitTableScan(TableScanNode node, Void context)
            {
                connectors.add(node.getTable().getConnectorId());
                return null;
            }
        };
        connectorCollector.visitPlan(plan, null);
        return connectors.build();
    }

    public PlanNode getPlan()
    {
        return plan;
    }
}
