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
package com.facebook.presto.protocol.v2.adapter;

import com.facebook.drift.annotations.ThriftField;
import com.facebook.presto.SessionRepresentation;
import com.facebook.presto.client.Column;
import com.facebook.presto.client.QueryError;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StageStats;
import com.facebook.presto.client.StatementStats;
import com.facebook.presto.execution.ExecutionFailureInfo;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.execution.StageExecutionInfo;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.execution.buffer.OutputBufferInfo;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.BasicQueryStats;
import com.facebook.presto.server.TaskUpdateRequest;
import com.facebook.presto.spi.PrestoWarning;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Descriptors.Descriptor;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.lang.Character.toLowerCase;
import static java.lang.reflect.Modifier.isPublic;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestProtocolBoundaryRegistry
{
    private static final List<ProtocolBoundary> TASK_UPDATE_BOUNDARIES = ImmutableList.of(
            boundary(
                    "task-update request",
                    TaskUpdateRequest.class,
                    com.facebook.presto.protocol.v2.TaskUpdateRequest.getDescriptor(),
                    ImmutableMap.<String, String>builder()
                            .put("session", "session")
                            .put("extraCredentials", "extra_credentials")
                            .put("fragment", "fragment")
                            .put("sources", "sources")
                            .put("outputIds", "output_ids")
                            .put("tableWriteInfo", "table_write_info")
                            .build(),
                    ImmutableMap.of(),
                    ImmutableMap.of()),
            boundary(
                    "task-update session",
                    SessionRepresentation.class,
                    com.facebook.presto.protocol.v2.SessionRepresentation.getDescriptor(),
                    ImmutableMap.<String, String>builder()
                            .put("queryId", "query_id")
                            .put("transactionId", "transaction_id")
                            .put("clientTransactionSupport", "client_transaction_support")
                            .put("user", "user")
                            .put("principal", "principal")
                            .put("source", "source")
                            .put("catalog", "catalog")
                            .put("schema", "schema")
                            .put("traceToken", "trace_token")
                            .put("timeZoneKey", "time_zone_key")
                            .put("locale", "locale")
                            .put("remoteUserAddress", "remote_user_address")
                            .put("userAgent", "user_agent")
                            .put("clientInfo", "client_info")
                            .put("clientTags", "client_tags")
                            .put("startTime", "start_time")
                            .put("resourceEstimates", "resource_estimates")
                            .put("systemProperties", "system_properties")
                            .put("catalogProperties", "catalog_properties")
                            .put("unprocessedCatalogProperties", "unprocessed_catalog_properties")
                            .put("roles", "roles")
                            .put("preparedStatements", "prepared_statements")
                            .put("selectedUser", "selected_user")
                            .put("reasonForSelect", "reason_for_select")
                            .build(),
                    ImmutableMap.of("sessionFunctions", "intentionallyOmitted: session function proto-to-Java conversion is not part of the initial task-update boundary"),
                    ImmutableMap.of("session_functions", "intentionallyOmitted: session function proto-to-Java conversion is not part of the initial task-update boundary")),
            boundary(
                    "task source",
                    TaskSource.class,
                    com.facebook.presto.protocol.v2.TaskSource.getDescriptor(),
                    ImmutableMap.<String, String>builder()
                            .put("planNodeId", "plan_node_id")
                            .put("splits", "splits")
                            .put("noMoreSplitsForLifespan", "no_more_splits_for_lifespan")
                            .put("noMoreSplits", "no_more_splits")
                            .build(),
                    ImmutableMap.of(),
                    ImmutableMap.of()),
            boundary(
                    "scheduled split",
                    ScheduledSplit.class,
                    com.facebook.presto.protocol.v2.ScheduledSplit.getDescriptor(),
                    ImmutableMap.<String, String>builder()
                            .put("sequenceId", "sequence_id")
                            .put("planNodeId", "plan_node_id")
                            .put("split", "split")
                            .build(),
                    ImmutableMap.of(),
                    ImmutableMap.of()),
            boundary(
                    "lifespan",
                    Lifespan.class,
                    com.facebook.presto.protocol.v2.Lifespan.getDescriptor(),
                    ImmutableMap.<String, String>builder()
                            .put("grouped", "grouped")
                            .put("groupId", "group_id")
                            .build(),
                    ImmutableMap.of(),
                    ImmutableMap.of()),
            boundary(
                    "output buffers",
                    OutputBuffers.class,
                    com.facebook.presto.protocol.v2.OutputBuffers.getDescriptor(),
                    ImmutableMap.<String, String>builder()
                            .put("type", "type")
                            .put("version", "version")
                            .put("noMoreBufferIds", "no_more_buffer_ids")
                            .put("buffers", "buffers")
                            .build(),
                    ImmutableMap.of(),
                    ImmutableMap.of()),
            boundary(
                    "table write info",
                    TableWriteInfo.class,
                    com.facebook.presto.protocol.v2.TableWriteInfo.getDescriptor(),
                    ImmutableMap.<String, String>builder()
                            .put("writerTarget", "writer_target")
                            .put("analyzeTableHandle", "analyze_table_handle")
                            .build(),
                    ImmutableMap.of("writerTargetUnion", "v1Only: thrift compatibility field derived from writerTarget"),
                    ImmutableMap.of()),
            boundary(
                    "task status",
                    TaskStatus.class,
                    com.facebook.presto.protocol.v2.TaskStatus.getDescriptor(),
                    ImmutableMap.<String, String>builder()
                            .put("taskInstanceIdLeastSignificantBits", "task_instance_id_least_significant_bits")
                            .put("taskInstanceIdMostSignificantBits", "task_instance_id_most_significant_bits")
                            .put("version", "version")
                            .put("state", "state")
                            .put("self", "self")
                            .put("completedDriverGroups", "completed_driver_groups")
                            .put("failures", "failures")
                            .put("queuedPartitionedDrivers", "queued_partitioned_drivers")
                            .put("runningPartitionedDrivers", "running_partitioned_drivers")
                            .put("outputBufferUtilization", "output_buffer_utilization")
                            .put("outputBufferOverutilized", "output_buffer_overutilized")
                            .put("physicalWrittenDataSizeInBytes", "physical_written_data_size_in_bytes")
                            .put("memoryReservationInBytes", "memory_reservation_in_bytes")
                            .put("systemMemoryReservationInBytes", "system_memory_reservation_in_bytes")
                            .put("fullGcCount", "full_gc_count")
                            .put("fullGcTimeInMillis", "full_gc_time_in_millis")
                            .put("peakNodeTotalMemoryReservationInBytes", "peak_node_total_memory_reservation_in_bytes")
                            .put("totalCpuTimeInNanos", "total_cpu_time_in_nanos")
                            .put("taskAgeInMillis", "task_age_in_millis")
                            .put("queuedPartitionedSplitsWeight", "queued_partitioned_splits_weight")
                            .put("runningPartitionedSplitsWeight", "running_partitioned_splits_weight")
                            .build(),
                    ImmutableMap.of(),
                    ImmutableMap.of()),
            boundary(
                    "execution failure info",
                    ExecutionFailureInfo.class,
                    com.facebook.presto.protocol.v2.ExecutionFailureInfo.getDescriptor(),
                    ImmutableMap.<String, String>builder()
                            .put("type", "type")
                            .put("message", "message")
                            .put("cause", "cause")
                            .put("suppressed", "suppressed")
                            .put("stack", "stack")
                            .put("errorLocation", "error_location")
                            .put("errorCode", "error_code")
                            .put("remoteHost", "remote_host")
                            .put("errorCause", "error_cause")
                            .build(),
                    ImmutableMap.of(),
                    ImmutableMap.of()),
            boundary(
                    "task info",
                    TaskInfo.class,
                    com.facebook.presto.protocol.v2.TaskInfo.getDescriptor(),
                    ImmutableMap.<String, String>builder()
                            .put("taskId", "task_id")
                            .put("taskStatus", "task_status")
                            .put("lastHeartbeatInMillis", "last_heartbeat_in_millis")
                            .put("outputBuffers", "output_buffers")
                            .put("noMoreSplits", "no_more_splits")
                            .put("stats", "stats")
                            .put("needsPlan", "needs_plan")
                            .put("nodeId", "node_id")
                            .build(),
                    ImmutableMap.of(),
                    ImmutableMap.of()),
            boundary(
                    "output buffer info",
                    OutputBufferInfo.class,
                    com.facebook.presto.protocol.v2.OutputBufferInfo.getDescriptor(),
                    ImmutableMap.<String, String>builder()
                            .put("type", "type")
                            .put("state", "state")
                            .put("buffers", "buffers")
                            .put("canAddBuffers", "can_add_buffers")
                            .put("canAddPages", "can_add_pages")
                            .put("totalBufferedBytes", "total_buffered_bytes")
                            .put("totalBufferedPages", "total_buffered_pages")
                            .put("totalRowsSent", "total_rows_sent")
                            .put("totalPagesSent", "total_pages_sent")
                            .build(),
                    ImmutableMap.of(),
                    ImmutableMap.of()),
            boundary(
                    "task stats",
                    TaskStats.class,
                    com.facebook.presto.protocol.v2.TaskStats.getDescriptor(),
                    ImmutableMap.<String, String>builder()
                            .put("createTimeInMillis", "create_time_in_millis")
                            .put("firstStartTimeInMillis", "first_start_time_in_millis")
                            .put("lastStartTimeInMillis", "last_start_time_in_millis")
                            .put("lastEndTimeInMillis", "last_end_time_in_millis")
                            .put("endTimeInMillis", "end_time_in_millis")
                            .put("elapsedTimeInNanos", "elapsed_time_in_nanos")
                            .put("queuedTimeInNanos", "queued_time_in_nanos")
                            .put("totalDrivers", "total_drivers")
                            .put("queuedDrivers", "queued_drivers")
                            .put("runningDrivers", "running_drivers")
                            .put("blockedDrivers", "blocked_drivers")
                            .put("completedDrivers", "completed_drivers")
                            .put("cumulativeUserMemory", "cumulative_user_memory")
                            .put("cumulativeTotalMemory", "cumulative_total_memory")
                            .put("userMemoryReservationInBytes", "user_memory_reservation_in_bytes")
                            .put("revocableMemoryReservationInBytes", "revocable_memory_reservation_in_bytes")
                            .put("systemMemoryReservationInBytes", "system_memory_reservation_in_bytes")
                            .put("peakUserMemoryInBytes", "peak_user_memory_in_bytes")
                            .put("peakTotalMemoryInBytes", "peak_total_memory_in_bytes")
                            .put("peakNodeTotalMemoryInBytes", "peak_node_total_memory_in_bytes")
                            .put("totalScheduledTimeInNanos", "total_scheduled_time_in_nanos")
                            .put("totalCpuTimeInNanos", "total_cpu_time_in_nanos")
                            .put("totalBlockedTimeInNanos", "total_blocked_time_in_nanos")
                            .put("fullyBlocked", "fully_blocked")
                            .put("blockedReasons", "blocked_reasons")
                            .put("totalAllocationInBytes", "total_allocation_in_bytes")
                            .put("rawInputDataSizeInBytes", "raw_input_data_size_in_bytes")
                            .put("rawInputPositions", "raw_input_positions")
                            .put("processedInputDataSizeInBytes", "processed_input_data_size_in_bytes")
                            .put("processedInputPositions", "processed_input_positions")
                            .put("outputDataSizeInBytes", "output_data_size_in_bytes")
                            .put("outputPositions", "output_positions")
                            .put("physicalWrittenDataSizeInBytes", "physical_written_data_size_in_bytes")
                            .put("pipelines", "pipelines")
                            .put("queuedPartitionedDrivers", "queued_partitioned_drivers")
                            .put("queuedPartitionedSplitsWeight", "queued_partitioned_splits_weight")
                            .put("runningPartitionedDrivers", "running_partitioned_drivers")
                            .put("runningPartitionedSplitsWeight", "running_partitioned_splits_weight")
                            .put("fullGcCount", "full_gc_count")
                            .put("fullGcTimeInMillis", "full_gc_time_in_millis")
                            .put("runtimeStats", "runtime_stats")
                            .put("totalSplits", "total_splits")
                            .put("queuedSplits", "queued_splits")
                            .put("runningSplits", "running_splits")
                            .put("completedSplits", "completed_splits")
                            .put("totalNewDrivers", "total_new_drivers")
                            .put("queuedNewDrivers", "queued_new_drivers")
                            .put("runningNewDrivers", "running_new_drivers")
                            .put("completedNewDrivers", "completed_new_drivers")
                            .build(),
                    ImmutableMap.of(),
                    ImmutableMap.of()));

    private static final List<ProtocolBoundary> QUERY_BOUNDARIES = ImmutableList.of(
            boundary(
                    "basic query info",
                    BasicQueryInfo.class,
                    com.facebook.presto.protocol.v2.BasicQueryInfo.getDescriptor(),
                    ImmutableMap.<String, String>builder()
                            .put("queryId", "query_id")
                            .put("state", "state")
                            .put("query", "query")
                            .put("queryStats", "query_stats")
                            .build(),
                    ImmutableMap.<String, String>builder()
                            .put("session", "deferred: not part of initial query metadata projection")
                            .put("resourceGroupId", "deferred: not part of initial query metadata projection")
                            .put("memoryPool", "deferred: not part of initial query metadata projection")
                            .put("scheduled", "deferred: not part of initial query metadata projection")
                            .put("self", "deferred: endpoint-local URI, not needed in canonical query metadata projection")
                            .put("queryHash", "derived from query text and omitted from initial query metadata projection")
                            .put("errorType", "deferred: failure fields will be modeled in a later query protocol slice")
                            .put("errorCode", "deferred: failure fields will be modeled in a later query protocol slice")
                            .put("failureInfo", "deferred: failure fields will be modeled in a later query protocol slice")
                            .put("queryType", "deferred: not part of initial query metadata projection")
                            .put("warnings", "deferred: warnings will be modeled in a later query protocol slice")
                            .put("preparedQuery", "deferred: not part of initial query metadata projection")
                            .build(),
                    ImmutableMap.of()),
            boundary(
                    "basic query stats",
                    BasicQueryStats.class,
                    com.facebook.presto.protocol.v2.BasicQueryStats.getDescriptor(),
                    ImmutableMap.<String, String>builder()
                            .put("createTime", "create_time_millis")
                            .put("endTime", "end_time_millis")
                            .put("elapsedTime", "elapsed_time_millis")
                            .build(),
                    ImmutableMap.<String, String>builder()
                            .put("queuedTime", "deferred: initial query stats projection carries only key timestamps and elapsed time")
                            .put("executionTime", "deferred: initial query stats projection carries only key timestamps and elapsed time")
                            .put("totalDrivers", "deferred: driver counts will be modeled in a later stats expansion")
                            .put("queuedDrivers", "deferred: driver counts will be modeled in a later stats expansion")
                            .put("runningDrivers", "deferred: driver counts will be modeled in a later stats expansion")
                            .put("completedDrivers", "deferred: driver counts will be modeled in a later stats expansion")
                            .put("rawInputDataSize", "deferred: data-size fields will be modeled in a later stats expansion")
                            .put("rawInputPositions", "deferred: input/output counters will be modeled in a later stats expansion")
                            .put("cumulativeUserMemory", "deferred: memory fields will be modeled in a later stats expansion")
                            .put("userMemoryReservation", "deferred: memory fields will be modeled in a later stats expansion")
                            .put("totalMemoryReservation", "deferred: memory fields will be modeled in a later stats expansion")
                            .put("peakUserMemoryReservation", "deferred: memory fields will be modeled in a later stats expansion")
                            .put("peakTotalMemoryReservation", "deferred: memory fields will be modeled in a later stats expansion")
                            .put("peakTaskTotalMemoryReservation", "deferred: memory fields will be modeled in a later stats expansion")
                            .put("peakNodeTotalMemoryReservation", "deferred: memory fields will be modeled in a later stats expansion")
                            .put("totalCpuTime", "deferred: not part of BasicQueryStats proto projection")
                            .put("totalScheduledTime", "deferred: timing fields will be modeled in a later stats expansion")
                            .put("fullyBlocked", "deferred: scheduling/blocking fields will be modeled in a later stats expansion")
                            .put("blockedReasons", "deferred: scheduling/blocking fields will be modeled in a later stats expansion")
                            .put("totalAllocation", "deferred: memory/allocation fields will be modeled in a later stats expansion")
                            .put("progressPercentage", "deferred: progress fields will be modeled in a later stats expansion")
                            .put("waitingForPrerequisitesTime", "deferred: timing fields will be modeled in a later stats expansion")
                            .put("cumulativeTotalMemory", "deferred: memory fields will be modeled in a later stats expansion")
                            .put("runningTasks", "deferred: task counts will be modeled in a later stats expansion")
                            .put("peakRunningTasks", "deferred: task counts will be modeled in a later stats expansion")
                            .put("analysisTime", "deferred: timing fields will be modeled in a later stats expansion")
                            .put("totalSplits", "deferred: split counts will be modeled in a later stats expansion")
                            .put("queuedSplits", "deferred: split counts will be modeled in a later stats expansion")
                            .put("runningSplits", "deferred: split counts will be modeled in a later stats expansion")
                            .put("completedSplits", "deferred: split counts will be modeled in a later stats expansion")
                            .put("totalNewDrivers", "deferred: driver counts will be modeled in a later stats expansion")
                            .put("queuedNewDrivers", "deferred: driver counts will be modeled in a later stats expansion")
                            .put("runningNewDrivers", "deferred: driver counts will be modeled in a later stats expansion")
                            .put("completedNewDrivers", "deferred: driver counts will be modeled in a later stats expansion")
                            .build(),
                    ImmutableMap.of()),
            boundary(
                    "query info",
                    QueryInfo.class,
                    com.facebook.presto.protocol.v2.QueryInfo.getDescriptor(),
                    ImmutableMap.<String, String>builder()
                            .put("queryId", "query_id")
                            .put("state", "state")
                            .put("query", "query")
                            .put("queryStats", "query_stats")
                            .put("outputStage", "output_stage")
                            .build(),
                    ImmutableMap.<String, String>builder()
                            .put("session", "deferred: not part of initial query metadata projection")
                            .put("memoryPool", "deferred: not part of initial query metadata projection")
                            .put("scheduled", "deferred: not part of initial query metadata projection")
                            .put("self", "deferred: endpoint-local URI, not needed in canonical query metadata projection")
                            .put("fieldNames", "deferred: result/statement protocol concern")
                            .put("queryHash", "derived from query text and omitted from initial query metadata projection")
                            .put("expandedQuery", "deferred: not part of initial query metadata projection")
                            .put("preparedQuery", "deferred: not part of initial query metadata projection")
                            .put("setCatalog", "deferred: client session mutation, statement protocol concern")
                            .put("setSchema", "deferred: client session mutation, statement protocol concern")
                            .put("setSessionProperties", "deferred: client session mutation, statement protocol concern")
                            .put("resetSessionProperties", "deferred: client session mutation, statement protocol concern")
                            .put("setRoles", "deferred: client session mutation, statement protocol concern")
                            .put("addedPreparedStatements", "deferred: client session mutation, statement protocol concern")
                            .put("deallocatedPreparedStatements", "deferred: client session mutation, statement protocol concern")
                            .put("startedTransactionId", "deferred: client session mutation, statement protocol concern")
                            .put("clearTransactionId", "deferred: client session mutation, statement protocol concern")
                            .put("updateInfo", "deferred: update result metadata will be modeled later")
                            .put("failureInfo", "deferred: failure fields will be modeled in a later query protocol slice")
                            .put("errorType", "deferred: failure fields will be modeled in a later query protocol slice")
                            .put("errorCode", "deferred: failure fields will be modeled in a later query protocol slice")
                            .put("warnings", "deferred: warnings will be modeled in a later query protocol slice")
                            .put("finalQueryInfo", "deferred: lifecycle metadata not part of initial query metadata projection")
                            .put("inputs", "deferred: input/output metadata will be modeled later")
                            .put("output", "deferred: input/output metadata will be modeled later")
                            .put("resourceGroupId", "deferred: not part of initial query metadata projection")
                            .put("queryType", "deferred: not part of initial query metadata projection")
                            .put("failedTasks", "deferred: failure fields will be modeled in a later query protocol slice")
                            .put("runtimeOptimizedStages", "deferred: stage optimization metadata will be modeled later")
                            .put("addedSessionFunctions", "deferred: session function metadata remains outside initial query projection")
                            .put("removedSessionFunctions", "deferred: session function metadata remains outside initial query projection")
                            .put("planStatsAndCosts", "deferred: plan metadata can be large and is outside initial query projection")
                            .put("optimizerInformation", "deferred: plan optimizer metadata is outside initial query projection")
                            .put("cteInformationList", "deferred: plan optimizer metadata is outside initial query projection")
                            .put("scalarFunctions", "deferred: function metadata is outside initial query projection")
                            .put("aggregateFunctions", "deferred: function metadata is outside initial query projection")
                            .put("windowFunctions", "deferred: function metadata is outside initial query projection")
                            .put("prestoSparkExecutionContext", "deferred: Spark-specific metadata is outside initial query projection")
                            .build(),
                    ImmutableMap.of()),
            boundary(
                    "query stats",
                    QueryStats.class,
                    com.facebook.presto.protocol.v2.QueryStats.getDescriptor(),
                    ImmutableMap.<String, String>builder()
                            .put("createTime", "create_time_millis")
                            .put("endTime", "end_time_millis")
                            .put("elapsedTime", "elapsed_time_millis")
                            .put("totalCpuTime", "total_cpu_time_millis")
                            .build(),
                    ImmutableMap.<String, String>builder()
                            .put("executionStartTime", "deferred: initial query stats projection carries only key timestamps and cpu time")
                            .put("lastHeartbeat", "deferred: initial query stats projection carries only key timestamps and cpu time")
                            .put("waitingForPrerequisitesTime", "deferred: timing fields will be modeled in a later stats expansion")
                            .put("resourceWaitingTime", "deferred: timing fields will be modeled in a later stats expansion")
                            .put("semanticAnalyzingTime", "deferred: timing fields will be modeled in a later stats expansion")
                            .put("columnAccessPermissionCheckingTime", "deferred: timing fields will be modeled in a later stats expansion")
                            .put("dispatchingTime", "deferred: timing fields will be modeled in a later stats expansion")
                            .put("queuedTime", "deferred: timing fields will be modeled in a later stats expansion")
                            .put("executionTime", "deferred: timing fields will be modeled in a later stats expansion")
                            .put("analysisTime", "deferred: timing fields will be modeled in a later stats expansion")
                            .put("totalPlanningTime", "deferred: timing fields will be modeled in a later stats expansion")
                            .put("finishingTime", "deferred: timing fields will be modeled in a later stats expansion")
                            .put("totalTasks", "deferred: task counts will be modeled in a later stats expansion")
                            .put("runningTasks", "deferred: task counts will be modeled in a later stats expansion")
                            .put("peakRunningTasks", "deferred: task counts will be modeled in a later stats expansion")
                            .put("completedTasks", "deferred: task counts will be modeled in a later stats expansion")
                            .put("totalDrivers", "deferred: driver counts will be modeled in a later stats expansion")
                            .put("queuedDrivers", "deferred: driver counts will be modeled in a later stats expansion")
                            .put("runningDrivers", "deferred: driver counts will be modeled in a later stats expansion")
                            .put("blockedDrivers", "deferred: driver counts will be modeled in a later stats expansion")
                            .put("completedDrivers", "deferred: driver counts will be modeled in a later stats expansion")
                            .put("totalNewDrivers", "deferred: driver counts will be modeled in a later stats expansion")
                            .put("queuedNewDrivers", "deferred: driver counts will be modeled in a later stats expansion")
                            .put("runningNewDrivers", "deferred: driver counts will be modeled in a later stats expansion")
                            .put("completedNewDrivers", "deferred: driver counts will be modeled in a later stats expansion")
                            .put("totalSplits", "deferred: split counts will be modeled in a later stats expansion")
                            .put("queuedSplits", "deferred: split counts will be modeled in a later stats expansion")
                            .put("runningSplits", "deferred: split counts will be modeled in a later stats expansion")
                            .put("completedSplits", "deferred: split counts will be modeled in a later stats expansion")
                            .put("cumulativeUserMemory", "deferred: memory fields will be modeled in a later stats expansion")
                            .put("cumulativeTotalMemory", "deferred: memory fields will be modeled in a later stats expansion")
                            .put("userMemoryReservation", "deferred: memory fields will be modeled in a later stats expansion")
                            .put("totalMemoryReservation", "deferred: memory fields will be modeled in a later stats expansion")
                            .put("peakUserMemoryReservation", "deferred: memory fields will be modeled in a later stats expansion")
                            .put("peakTotalMemoryReservation", "deferred: memory fields will be modeled in a later stats expansion")
                            .put("peakTaskTotalMemory", "deferred: memory fields will be modeled in a later stats expansion")
                            .put("peakNodeTotalMemory", "deferred: memory fields will be modeled in a later stats expansion")
                            .put("peakTaskUserMemory", "deferred: memory fields will be modeled in a later stats expansion")
                            .put("scheduled", "deferred: scheduling fields will be modeled in a later stats expansion")
                            .put("totalScheduledTime", "deferred: timing fields will be modeled in a later stats expansion")
                            .put("retriedCpuTime", "deferred: retry stats will be modeled in a later stats expansion")
                            .put("totalBlockedTime", "deferred: timing fields will be modeled in a later stats expansion")
                            .put("fullyBlocked", "deferred: scheduling/blocking fields will be modeled in a later stats expansion")
                            .put("blockedReasons", "deferred: scheduling/blocking fields will be modeled in a later stats expansion")
                            .put("totalAllocation", "deferred: memory/allocation fields will be modeled in a later stats expansion")
                            .put("rawInputDataSize", "deferred: input/output fields will be modeled in a later stats expansion")
                            .put("rawInputPositions", "deferred: input/output fields will be modeled in a later stats expansion")
                            .put("processedInputDataSize", "deferred: input/output fields will be modeled in a later stats expansion")
                            .put("processedInputPositions", "deferred: input/output fields will be modeled in a later stats expansion")
                            .put("shuffledDataSize", "deferred: input/output fields will be modeled in a later stats expansion")
                            .put("shuffledPositions", "deferred: input/output fields will be modeled in a later stats expansion")
                            .put("outputDataSize", "deferred: input/output fields will be modeled in a later stats expansion")
                            .put("outputPositions", "deferred: input/output fields will be modeled in a later stats expansion")
                            .put("writtenOutputPositions", "deferred: input/output fields will be modeled in a later stats expansion")
                            .put("writtenOutputLogicalDataSize", "deferred: input/output fields will be modeled in a later stats expansion")
                            .put("writtenOutputPhysicalDataSize", "deferred: input/output fields will be modeled in a later stats expansion")
                            .put("writtenIntermediatePhysicalDataSize", "deferred: input/output fields will be modeled in a later stats expansion")
                            .put("stageGcStatistics", "deferred: deep stage stats will be modeled in a later stats expansion")
                            .put("operatorSummaries", "deferred: operator summaries are large and outside initial query projection")
                            .put("progressPercentage", "deferred: progress fields will be modeled in a later stats expansion")
                            .put("spilledDataSize", "deferred: input/output fields will be modeled in a later stats expansion")
                            .put("runtimeStats", "deferred: runtime stats use a separate bridge pattern and are outside initial query projection")
                            .build(),
                    ImmutableMap.of()),
            boundary(
                    "stage info",
                    StageInfo.class,
                    com.facebook.presto.protocol.v2.StageInfo.getDescriptor(),
                    ImmutableMap.<String, String>builder()
                            .put("stageId", "stage_id")
                            .put("latestAttemptExecutionInfo", "state")
                            .put("subStages", "sub_stages")
                            .build(),
                    ImmutableMap.<String, String>builder()
                            .put("self", "deferred: endpoint-local URI, not needed in canonical stage projection")
                            .put("plan", "deferred: plan fragments can be large and are outside initial stage projection")
                            .put("previousAttemptsExecutionInfos", "deferred: retry attempts are outside initial stage projection")
                            .put("runtimeOptimized", "deferred: optimization metadata is outside initial stage projection")
                            .build(),
                    ImmutableMap.of()),
            boundary(
                    "stage execution info",
                    StageExecutionInfo.class,
                    com.facebook.presto.protocol.v2.StageExecutionInfo.getDescriptor(),
                    ImmutableMap.<String, String>builder()
                            .put("state", "state")
                            .build(),
                    ImmutableMap.<String, String>builder()
                            .put("stats", "deferred: stage execution stats will be modeled in a later stats expansion")
                            .put("tasks", "deferred: task details are outside initial query metadata projection")
                            .put("failureCause", "deferred: failure fields will be modeled in a later query protocol slice")
                            .build(),
                    ImmutableMap.of()));

    private static final List<ProtocolBoundary> STATEMENT_BOUNDARIES = ImmutableList.of(
            boundary(
                    "statement query results",
                    QueryResults.class,
                    com.facebook.presto.protocol.v2.QueryResults.getDescriptor(),
                    ImmutableMap.<String, String>builder()
                            .put("id", "id")
                            .put("infoUri", "info_uri")
                            .put("partialCancelUri", "partial_cancel_uri")
                            .put("nextUri", "next_uri")
                            .put("columns", "columns")
                            .put("binaryData", "binary_data")
                            .put("stats", "stats")
                            .put("error", "error")
                            .put("warnings", "warnings")
                            .put("updateType", "update_type")
                            .put("updateCount", "update_count")
                            .build(),
                    ImmutableMap.of("data", "deferred: row-oriented JSON data is not part of the initial protobuf statement projection; binaryData remains supported"),
                    ImmutableMap.of()),
            boundary(
                    "statement column",
                    Column.class,
                    com.facebook.presto.protocol.v2.Column.getDescriptor(),
                    ImmutableMap.<String, String>builder()
                            .put("name", "name")
                            .put("type", "type")
                            .build(),
                    ImmutableMap.of("typeSignature", "deferred: initial statement column projection keeps the v1 type string only"),
                    ImmutableMap.of()),
            boundary(
                    "statement stats",
                    StatementStats.class,
                    com.facebook.presto.protocol.v2.StatementStats.getDescriptor(),
                    ImmutableMap.<String, String>builder()
                            .put("state", "state")
                            .put("waitingForPrerequisites", "waiting_for_prerequisites")
                            .put("queued", "queued")
                            .put("scheduled", "scheduled")
                            .put("nodes", "nodes")
                            .put("totalSplits", "total_splits")
                            .put("queuedSplits", "queued_splits")
                            .put("runningSplits", "running_splits")
                            .put("completedSplits", "completed_splits")
                            .put("cpuTimeMillis", "cpu_time_millis")
                            .put("wallTimeMillis", "wall_time_millis")
                            .put("waitingForPrerequisitesTimeMillis", "waiting_for_prerequisites_time_millis")
                            .put("queuedTimeMillis", "queued_time_millis")
                            .put("elapsedTimeMillis", "elapsed_time_millis")
                            .put("processedRows", "processed_rows")
                            .put("processedBytes", "processed_bytes")
                            .put("peakMemoryBytes", "peak_memory_bytes")
                            .put("peakTotalMemoryBytes", "peak_total_memory_bytes")
                            .put("peakTaskTotalMemoryBytes", "peak_task_total_memory_bytes")
                            .put("rootStage", "root_stage")
                            .put("spilledBytes", "spilled_bytes")
                            .build(),
                    ImmutableMap.<String, String>builder()
                            .put("runtimeStats", "deferred: runtime stats are outside initial statement projection")
                            .put("progressPercentage", "derived client-side from scheduled and split counts")
                            .build(),
                    ImmutableMap.of()),
            boundary(
                    "statement stage stats",
                    StageStats.class,
                    com.facebook.presto.protocol.v2.StageStats.getDescriptor(),
                    ImmutableMap.<String, String>builder()
                            .put("stageId", "stage_id")
                            .put("state", "state")
                            .put("subStages", "sub_stages")
                            .put("done", "done")
                            .put("nodes", "nodes")
                            .put("totalSplits", "total_splits")
                            .put("queuedSplits", "queued_splits")
                            .put("runningSplits", "running_splits")
                            .put("completedSplits", "completed_splits")
                            .put("cpuTimeMillis", "cpu_time_millis")
                            .put("wallTimeMillis", "wall_time_millis")
                            .put("processedRows", "processed_rows")
                            .put("processedBytes", "processed_bytes")
                            .build(),
                    ImmutableMap.of(),
                    ImmutableMap.of()),
            boundary(
                    "statement query error",
                    QueryError.class,
                    com.facebook.presto.protocol.v2.QueryError.getDescriptor(),
                    ImmutableMap.<String, String>builder()
                            .put("message", "message")
                            .put("sqlState", "sql_state")
                            .put("errorCode", "error_code")
                            .put("errorName", "error_name")
                            .put("errorType", "error_type")
                            .put("retriable", "retriable")
                            .build(),
                    ImmutableMap.<String, String>builder()
                            .put("errorLocation", "deferred: error location will be modeled in a later statement expansion")
                            .put("failureInfo", "deferred: recursive failure info will be modeled in a later statement expansion")
                            .build(),
                    ImmutableMap.of()),
            boundary(
                    "statement warning",
                    PrestoWarning.class,
                    com.facebook.presto.protocol.v2.ClientWarning.getDescriptor(),
                    ImmutableMap.<String, String>builder()
                            .put("warningCode", "code")
                            .put("message", "message")
                            .build(),
                    ImmutableMap.of(),
                    ImmutableMap.of("name", "derived from warningCode")));

    @Test
    public void testTaskUpdateBoundaryFieldCoverage()
    {
        for (ProtocolBoundary boundary : TASK_UPDATE_BOUNDARIES) {
            assertProtocolBoundary(boundary);
        }
    }

    @Test
    public void testQueryBoundaryFieldCoverage()
    {
        for (ProtocolBoundary boundary : QUERY_BOUNDARIES) {
            assertProtocolBoundary(boundary);
        }
    }

    @Test
    public void testStatementBoundaryFieldCoverage()
    {
        for (ProtocolBoundary boundary : STATEMENT_BOUNDARIES) {
            assertProtocolBoundary(boundary);
        }
    }

    private static void assertProtocolBoundary(ProtocolBoundary boundary)
    {
        Set<String> javaFields = javaProtocolFields(boundary.getJavaClass());
        Set<String> mappedJavaFields = boundary.getFieldMappings().keySet();

        assertEquals(
                javaFields,
                ImmutableSet.<String>builder()
                        .addAll(mappedJavaFields)
                        .addAll(boundary.getIgnoredJavaFields().keySet())
                        .build(),
                boundary.getName() + " Java fields must be mapped or explicitly ignored");

        for (Map.Entry<String, String> fieldMapping : boundary.getFieldMappings().entrySet()) {
            assertTrue(javaFields.contains(fieldMapping.getKey()), boundary.getName() + " maps missing Java field: " + fieldMapping.getKey());
            assertNotNull(boundary.getProtocolDescriptor().findFieldByName(fieldMapping.getValue()), boundary.getName() + " maps missing protobuf field: " + fieldMapping.getValue());
        }

        Set<String> protocolFields = boundary.getProtocolDescriptor().getFields().stream()
                .map(field -> field.getName())
                .collect(ImmutableSet.toImmutableSet());
        assertEquals(
                protocolFields,
                ImmutableSet.<String>builder()
                        .addAll(boundary.getFieldMappings().values())
                        .addAll(boundary.getIgnoredProtocolFields().keySet())
                        .build(),
                boundary.getName() + " protobuf fields must be mapped or explicitly ignored");

        boundary.getIgnoredJavaFields().forEach((field, reason) -> assertFalse(reason.isEmpty(), boundary.getName() + " ignored Java field must include a reason: " + field));
        boundary.getIgnoredProtocolFields().forEach((field, reason) -> assertFalse(reason.isEmpty(), boundary.getName() + " ignored protobuf field must include a reason: " + field));
    }

    private static ProtocolBoundary boundary(
            String name,
            Class<?> javaClass,
            Descriptor protocolDescriptor,
            Map<String, String> fieldMappings,
            Map<String, String> ignoredJavaFields,
            Map<String, String> ignoredProtocolFields)
    {
        return new ProtocolBoundary(name, javaClass, protocolDescriptor, fieldMappings, ignoredJavaFields, ignoredProtocolFields);
    }

    private static Set<String> javaProtocolFields(Class<?> clazz)
    {
        ImmutableSet.Builder<String> fields = ImmutableSet.builder();
        for (Method method : clazz.getMethods()) {
            if (!isPublic(method.getModifiers()) || method.getParameterCount() != 0 || method.getDeclaringClass().equals(Object.class)) {
                continue;
            }

            Optional<String> jsonField = jsonFieldName(method);
            Optional<String> thriftField = thriftFieldName(method);
            if (jsonField.isPresent()) {
                fields.add(jsonField.get());
            }
            else {
                thriftField.ifPresent(fields::add);
            }
        }
        return fields.build();
    }

    private static Optional<String> jsonFieldName(Method method)
    {
        JsonProperty jsonProperty = method.getAnnotation(JsonProperty.class);
        if (jsonProperty == null) {
            return Optional.empty();
        }
        if (!jsonProperty.value().isEmpty()) {
            return Optional.of(jsonProperty.value());
        }
        return Optional.of(accessorName(method.getName()));
    }

    private static Optional<String> thriftFieldName(Method method)
    {
        ThriftField thriftField = method.getAnnotation(ThriftField.class);
        if (thriftField == null) {
            return Optional.empty();
        }
        if (!thriftField.name().isEmpty()) {
            return Optional.of(thriftField.name());
        }
        return Optional.of(accessorName(method.getName()));
    }

    private static String accessorName(String methodName)
    {
        String property;
        if (methodName.startsWith("get")) {
            property = methodName.substring("get".length());
        }
        else if (methodName.startsWith("is")) {
            property = methodName.substring("is".length());
        }
        else {
            throw new IllegalArgumentException("Unsupported protocol accessor: " + methodName);
        }
        return toLowerCase(property.charAt(0)) + property.substring(1);
    }

    private static final class ProtocolBoundary
    {
        private final String name;
        private final Class<?> javaClass;
        private final Descriptor protocolDescriptor;
        private final Map<String, String> fieldMappings;
        private final Map<String, String> ignoredJavaFields;
        private final Map<String, String> ignoredProtocolFields;

        private ProtocolBoundary(
                String name,
                Class<?> javaClass,
                Descriptor protocolDescriptor,
                Map<String, String> fieldMappings,
                Map<String, String> ignoredJavaFields,
                Map<String, String> ignoredProtocolFields)
        {
            this.name = name;
            this.javaClass = javaClass;
            this.protocolDescriptor = protocolDescriptor;
            this.fieldMappings = ImmutableMap.copyOf(fieldMappings);
            this.ignoredJavaFields = ImmutableMap.copyOf(ignoredJavaFields);
            this.ignoredProtocolFields = ImmutableMap.copyOf(ignoredProtocolFields);
        }

        private String getName()
        {
            return name;
        }

        private Class<?> getJavaClass()
        {
            return javaClass;
        }

        private Descriptor getProtocolDescriptor()
        {
            return protocolDescriptor;
        }

        private Map<String, String> getFieldMappings()
        {
            return fieldMappings;
        }

        private Map<String, String> getIgnoredJavaFields()
        {
            return ignoredJavaFields;
        }

        private Map<String, String> getIgnoredProtocolFields()
        {
            return ignoredProtocolFields;
        }
    }
}
