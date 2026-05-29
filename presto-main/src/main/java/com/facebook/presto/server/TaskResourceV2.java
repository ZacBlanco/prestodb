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
package com.facebook.presto.server;

import com.facebook.airlift.concurrent.BoundedExecutor;
import com.facebook.airlift.json.Codec;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.Session;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManager;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.protocol.v2.adapter.TaskInfoAdapter;
import com.facebook.presto.protocol.v2.adapter.TaskStatusAdapter;
import com.facebook.presto.protocol.v2.adapter.TaskUpdateRequestAdapter;
import com.facebook.presto.sql.planner.PlanFragment;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.MoreFutures.addTimeout;
import static com.facebook.airlift.http.server.AsyncResponseHandler.bindAsyncResponse;
import static com.facebook.presto.PrestoMediaTypes.APPLICATION_PRESTO_PROTOBUF;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CURRENT_STATE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_MAX_WAIT;
import static com.facebook.presto.server.security.RoleType.INTERNAL;
import static com.facebook.presto.util.TaskUtils.randomizeWaitTime;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Protocol v2 task endpoint surface. This is intentionally parallel to {@link TaskResource}
 * so the existing /v1 JSON/Smile/Thrift resource remains unchanged.
 */
@Path("/v2/task")
@RolesAllowed(INTERNAL)
@Consumes(APPLICATION_PRESTO_PROTOBUF)
@Produces(APPLICATION_PRESTO_PROTOBUF)
public class TaskResourceV2
{
    private static final Duration ADDITIONAL_WAIT_TIME = new Duration(5, SECONDS);

    private final TaskManager taskManager;
    private final SessionPropertyManager sessionPropertyManager;
    private final Executor responseExecutor;
    private final ScheduledExecutorService timeoutExecutor;
    private final Codec<PlanFragment> planFragmentCodec;
    private final TaskUpdateRequestAdapter taskUpdateRequestAdapter;
    private final TaskStatusAdapter taskStatusAdapter;
    private final TaskInfoAdapter taskInfoAdapter;
    private final boolean enabled;

    @Inject
    public TaskResourceV2(
            TaskManager taskManager,
            SessionPropertyManager sessionPropertyManager,
            @ForAsyncRpc BoundedExecutor responseExecutor,
            @ForAsyncRpc ScheduledExecutorService timeoutExecutor,
            JsonCodec<PlanFragment> planFragmentJsonCodec,
            ObjectMapper objectMapper,
            ServerConfig serverConfig)
    {
        this.taskManager = requireNonNull(taskManager, "taskManager is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.responseExecutor = requireNonNull(responseExecutor, "responseExecutor is null");
        this.timeoutExecutor = requireNonNull(timeoutExecutor, "timeoutExecutor is null");
        this.planFragmentCodec = requireNonNull(planFragmentJsonCodec, "planFragmentJsonCodec is null");
        this.taskUpdateRequestAdapter = new TaskUpdateRequestAdapter(requireNonNull(objectMapper, "objectMapper is null"));
        this.taskStatusAdapter = new TaskStatusAdapter();
        this.taskInfoAdapter = new TaskInfoAdapter(objectMapper);
        this.enabled = requireNonNull(serverConfig, "serverConfig is null").isProtocolV2Enabled();
    }

    @POST
    @Path("{taskId}")
    public Response createOrUpdateTask(
            @PathParam("taskId") TaskId taskId,
            com.facebook.presto.protocol.v2.TaskUpdateRequest protocolTaskUpdateRequest)
    {
        if (!enabled) {
            throw new NotFoundException();
        }

        requireNonNull(protocolTaskUpdateRequest, "protocolTaskUpdateRequest is null");

        TaskUpdateRequest taskUpdateRequest = taskUpdateRequestAdapter.fromProtocol(protocolTaskUpdateRequest);
        Session session = taskUpdateRequest.getSession().toSession(sessionPropertyManager, taskUpdateRequest.getExtraCredentials());
        taskManager.updateTask(
                session,
                taskId,
                taskUpdateRequest.getFragment().map(planFragmentCodec::fromBytes),
                taskUpdateRequest.getSources(),
                taskUpdateRequest.getOutputIds(),
                taskUpdateRequest.getTableWriteInfo());

        return Response.noContent().build();
    }

    @GET
    @Path("{taskId}")
    public void getTaskInfo(
            @PathParam("taskId") TaskId taskId,
            @HeaderParam(PRESTO_CURRENT_STATE) TaskState currentState,
            @HeaderParam(PRESTO_MAX_WAIT) Duration maxWait,
            @Context UriInfo uriInfo,
            @Suspended AsyncResponse asyncResponse)
    {
        if (!enabled) {
            throw new NotFoundException();
        }
        requireNonNull(taskId, "taskId is null");

        if (currentState == null || maxWait == null) {
            TaskInfo taskInfo = taskManager.getTaskInfo(taskId);
            if (shouldSummarize(uriInfo)) {
                taskInfo = taskInfo.summarize();
            }
            asyncResponse.resume(taskInfoAdapter.toProtocol(taskInfo));
            return;
        }

        Duration waitTime = randomizeWaitTime(maxWait);
        ListenableFuture<TaskInfo> futureTaskInfo = addTimeout(
                taskManager.getTaskInfo(taskId, currentState),
                () -> taskManager.getTaskInfo(taskId),
                waitTime,
                timeoutExecutor);
        if (shouldSummarize(uriInfo)) {
            futureTaskInfo = Futures.transform(futureTaskInfo, TaskInfo::summarize, directExecutor());
        }
        ListenableFuture<com.facebook.presto.protocol.v2.TaskInfo> protocolFutureTaskInfo = Futures.transform(futureTaskInfo, taskInfoAdapter::toProtocol, directExecutor());

        Duration timeout = new Duration(waitTime.toMillis() + ADDITIONAL_WAIT_TIME.toMillis(), MILLISECONDS);
        bindAsyncResponse(asyncResponse, protocolFutureTaskInfo, responseExecutor)
                .withTimeout(timeout);
    }

    @GET
    @Path("{taskId}/status")
    public void getTaskStatus(
            @PathParam("taskId") TaskId taskId,
            @HeaderParam(PRESTO_CURRENT_STATE) TaskState currentState,
            @HeaderParam(PRESTO_MAX_WAIT) Duration maxWait,
            @Suspended AsyncResponse asyncResponse)
    {
        if (!enabled) {
            throw new NotFoundException();
        }
        requireNonNull(taskId, "taskId is null");

        if (currentState == null || maxWait == null) {
            asyncResponse.resume(taskStatusAdapter.toProtocol(taskManager.getTaskStatus(taskId)));
            return;
        }

        Duration waitTime = randomizeWaitTime(maxWait);
        ListenableFuture<TaskStatus> futureTaskStatus = addTimeout(
                taskManager.getTaskStatus(taskId, currentState),
                () -> taskManager.getTaskStatus(taskId),
                waitTime,
                timeoutExecutor);
        ListenableFuture<com.facebook.presto.protocol.v2.TaskStatus> protocolFutureTaskStatus = Futures.transform(futureTaskStatus, status -> taskStatusAdapter.toProtocol(status), directExecutor());

        Duration timeout = new Duration(waitTime.toMillis() + ADDITIONAL_WAIT_TIME.toMillis(), MILLISECONDS);
        bindAsyncResponse(asyncResponse, protocolFutureTaskStatus, responseExecutor)
                .withTimeout(timeout);
    }

    private static boolean shouldSummarize(UriInfo uriInfo)
    {
        return uriInfo.getQueryParameters().containsKey("summarize");
    }
}
