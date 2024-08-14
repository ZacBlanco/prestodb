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

import com.facebook.presto.common.analyzer.PreparedQuery;
import com.facebook.presto.spi.analyzer.AnalyzerContext;
import com.facebook.presto.spi.analyzer.AnalyzerProvider;
import com.facebook.presto.spi.analyzer.QueryAnalysis;
import com.facebook.presto.spi.analyzer.QueryAnalyzer;
import com.facebook.presto.spi.analyzer.QueryPreparer;
import com.facebook.presto.spi.plan.PlanNode;
import com.google.inject.Inject;

import static com.google.common.base.Verify.verify;

public class PrePlannedQueryAnalyzerProvider
        implements AnalyzerProvider
{
    @Inject
    public PrePlannedQueryAnalyzerProvider()
    {}

    public static final String PREPLANNED_ANALYZER = "preplanned-analyzer";

    @Override
    public String getType()
    {
        return PREPLANNED_ANALYZER;
    }

    @Override
    public QueryPreparer getQueryPreparer()
    {
        return (analyzerOptions, query, preparedStatements, warningCollector) -> {
            verify(analyzerOptions instanceof PrePlannedAnalyzerOptions, "analyzer options must be pre-planned");
            PrePlannedAnalyzerOptions prePlannedAnalyzerOptions = (PrePlannedAnalyzerOptions) analyzerOptions;
            return new PrePlannedPreparedQuery(prePlannedAnalyzerOptions.getPlan());
        };
    }

    @Override
    public QueryAnalyzer getQueryAnalyzer()
    {
        return new QueryAnalyzer()
        {
            @Override
            public QueryAnalysis analyze(AnalyzerContext analyzerContext, PreparedQuery preparedQuery)
            {
                verify(preparedQuery instanceof PrePlannedPreparedQuery, "preplanned analyzer only support pre-planned prepared query");
                PrePlannedPreparedQuery query = (PrePlannedPreparedQuery) preparedQuery;
                return new PrePlannedQueryAnalysis(query.getPlan());
            }

            @Override
            public PlanNode plan(AnalyzerContext analyzerContext, QueryAnalysis queryAnalysis)
            {
                verify(queryAnalysis instanceof PrePlannedQueryAnalysis, "preplanned analyzer only support pre-planned prepared query");
                PrePlannedQueryAnalysis analysis = (PrePlannedQueryAnalysis) queryAnalysis;
                return analysis.getPlan();
            }
        };
    }
}
