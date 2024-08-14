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

package com.facebook.presto.connector;

import com.facebook.presto.annotation.UsedByGeneratedCode;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.SqlQueryManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.procedure.Procedure;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.util.Reflection.methodHandle;
import static java.util.Objects.requireNonNull;

public class ClearHistoryBasedStatisticsProcedure
{
    private static final MethodHandle CLEAR_HBO = methodHandle(ClearHistoryBasedStatisticsProcedure.class, "clearHbo");

    private final QueryManager queryManager;

    @Inject
    public ClearHistoryBasedStatisticsProcedure(QueryManager queryManager)
    {
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
    }

    @UsedByGeneratedCode
    public void clearHbo()
    {
        if (!(queryManager instanceof SqlQueryManager)) {
            throw new PrestoException(NOT_SUPPORTED, "HBO stats can only be cleared when using the SqlQueryManager. Found " + queryManager.getClass().getSimpleName());
        }
        SqlQueryManager sqlQueryManager = (SqlQueryManager) queryManager;
        sqlQueryManager.getHistoryBasedPlanStatisticsTracker().getHistoryBasedPlanStatisticsProvider().clear();
    }

    public Procedure getProcedure()
    {
        return new Procedure(
                "runtime",
                "clear_history_stats",
                ImmutableList.<Procedure.Argument>builder()
                        .build(),
                CLEAR_HBO.bindTo(this));
    }
}
