# Design: Incremental Presto Protocol Migration Using `/v2` APIs and Canonical Protobuf

## 1. Summary

Presto will introduce a new `/v2` protocol surface backed by a well-defined canonical protocol, initially protobuf, while keeping every existing `/v1` endpoint and Java/Jackson DTO unchanged for compatibility.

The migration will be boundary-first, not class-first:

- `/v1` remains the compatibility API.
- `/v2` becomes the canonical protocol API.
- Existing Java protocol classes continue to exist while `/v1` is supported.
- New internal communication moves to `/v2` first.
- Public clients migrate to `/v2` later.
- Once external tooling no longer depends on `/v1`, the `/v1` API and compatibility adapters can be removed.

The core principle is:

> Do not rewrite all Java protocol classes in place. Instead, add canonical `/v2` protocol objects at API boundaries and use explicit adapters between existing Java objects and canonical protocol messages.

This gives us a clear migration path without destabilizing existing public REST clients.

## 2. Goals

### 2.1 Functional goals

1. Add a `/v2` protocol surface using protobuf/canonical messages.
2. Keep all existing `/v1` endpoints behaviorally compatible.
3. Allow internal Presto communication to move to `/v2` before public clients migrate.
4. Allow Presto internals to gradually move from Java DTOs to proto-backed internal representations.
5. Provide a strict mechanism to keep `/v1` and `/v2` protocol changes synchronized while both are supported.
6. Support rolling upgrades and mixed-version clusters.

### 2.2 Compatibility goals

1. Existing `/v1` JSON output must remain unchanged unless intentionally changed.
2. Existing external clients, CLI tools, monitoring tools, and UI integrations must continue using `/v1`.
3. New `/v2` clients should not depend on Jackson class shapes or Java package names.
4. `/v2` schemas must be forward/backward compatible according to protobuf evolution rules.

### 2.3 Migration goals

1. Migrate internal task update protocol first, because it is internal-only and high-value.
2. Avoid extracting all existing Java protocol classes into a new module.
3. Avoid changing public DTO constructors/getters just to support `/v2`.
4. Make migration possible one endpoint family at a time.

## 3. Non-goals

1. We will not rewrite all protocol classes in a single change.
2. We will not remove `/v1` until after external tooling migration.
3. We will not require public clients to consume protobuf immediately.
4. We will not attempt to make existing Java DTOs the canonical schema.
5. We will not extract existing execution classes such as `QueryInfo`, `TaskInfo`, `TaskStatus`, or `SessionRepresentation` into a separate protocol module because these classes contain mixed protocol, execution, and compatibility logic.

Examples of mixed classes that should remain in place during the migration:

- `QueryInfo` mixes public query metadata, execution state, plan information, stats, warnings, and internal-only fields.
- `TaskInfo` is both an execution object and a serialized protocol object, with fields such as `TaskStatus`, `OutputBufferInfo`, `TaskStats`, and `needsPlan`.
- `TaskStatus` contains remote task versioning and task runtime state.
- `SessionRepresentation` is used in both public query information and internal task update state.

## 4. High-level architecture

### 4.1 Endpoint model

Presto will operate two protocol versions side by side:

```text
/v1/...    Existing Java DTO + Jackson JSON protocol
/v2/...    Canonical protobuf protocol
```

Examples:

```text
POST /v1/task/{taskId}
POST /v2/task/{taskId}

GET  /v1/task/{taskId}/status
GET  /v2/task/{taskId}/status

POST /v1/statement
POST /v2/statement
```

The `/v1` endpoints remain the compatibility layer. The `/v2` endpoints are the new canonical contract.

### 4.2 Serialization model

#### `/v1`

`/v1` continues to use the existing Java DTOs and Jackson JSON serialization.

Examples:

- `SessionRepresentation` remains JSON/Thrift/Jackson-compatible.
- `BasicQueryInfo` remains a public `/v1/query` DTO.
- `TaskInfo` remains JSON-compatible and continues exposing `TaskStatus`, output buffers, stats, and node information.
- `QueryResults` remains the public client protocol root object for `/v1/statement`.

#### `/v2`

`/v2` uses protobuf messages as the canonical schema.

Initial content type:

```http
Content-Type: application/x-presto-protobuf
Accept: application/x-presto-protobuf
```

Optional future content types may include:

```http
application/json; protocol=presto-v2
application/x-presto-protobuf; protocol=presto-v2
```

The initial implementation should keep the `/v2` wire format simple: protobuf only.

### 4.3 Adapter model

Each migrated boundary gets an explicit adapter:

```java
public interface ProtocolAdapter<J, P>
{
    P toProtocol(J value);
    J fromProtocol(P value);
}
```

Where:

- `J` is the existing Java DTO/internal object.
- `P` is the generated protobuf message.

Example adapters:

```text
TaskUpdateRequestAdapter
TaskStatusAdapter
TaskInfoAdapter
SessionRepresentationAdapter
QueryResultsAdapter
BasicQueryInfoAdapter
QueryInfoAdapter
```

Adapters are intentionally boundary-owned. We do not globally replace Java classes.

### 4.4 Boundary-first migration

The migration order should be:

1. Internal task update request.
2. Internal task status/task info response.
3. Internal exchange/output buffer metadata, if needed.
4. Public query status APIs.
5. Public statement/client protocol APIs.
6. Remaining public REST APIs.
7. Deprecate `/v1`.
8. Remove `/v1`.

The first useful migration target is internal task update because it is primarily cluster-internal and does not need external client compatibility.

## 5. Module-level design

### 5.1 New generated protocol module

Add a small module for canonical protocol schemas and generated code.

Suggested module name:

```text
presto-protocol
```

This module does **not** contain existing Java DTOs. It only contains protobuf definitions and generated code.

Suggested layout:

```text
presto-protocol/
  src/main/proto/presto/v2/common.proto
  src/main/proto/presto/v2/session.proto
  src/main/proto/presto/v2/task.proto
  src/main/proto/presto/v2/query.proto
  src/main/proto/presto/v2/statement.proto
```

Responsibilities:

1. Own `.proto` definitions.
2. Generate Java protobuf classes.
3. Enforce protobuf compatibility checks.
4. Provide schema descriptors if needed for tooling.

Dependencies:

- Should depend on protobuf runtime only.
- Should not depend on `presto-main`, `presto-main-base`, or execution classes.
- Should not import Jackson DTO classes.

Testing:

1. Protobuf compilation test.
2. Schema compatibility test.
3. Reserved field/name enforcement.
4. Golden binary/textproto fixtures for representative messages.

### 5.2 `presto-main-base`

`presto-main-base` keeps the existing Java DTOs and gains adapter code.

Responsibilities:

1. Keep existing Java protocol/execution classes unchanged for `/v1`.
2. Add adapter classes from Java DTOs/internal objects to protobuf messages.
3. Add adapter unit tests.
4. Avoid moving mixed execution/protocol classes out of this module.

Suggested new packages:

```text
com.facebook.presto.protocol.v2.adapter
com.facebook.presto.protocol.v2.testing
```

Example classes:

```text
ProtocolAdapter
ProtocolAdapters
TaskUpdateRequestAdapter
TaskSourceAdapter
ScheduledSplitAdapter
SessionRepresentationAdapter
TaskStatusAdapter
TaskInfoAdapter
BasicQueryInfoAdapter
QueryInfoAdapter
```

Important existing classes that remain in place:

- `SessionRepresentation`, because it is used in public query info and internal task update state.
- `QueryInfo`, because it mixes public query metadata, execution state, stage info, plan info, stats, warnings, and internal-only fields.
- `BasicQueryInfo`, because it is public `/v1/query` compatibility surface.
- `TaskInfo`, because it is both internal task state and reachable through public query/stage/task graphs.
- `TaskStatus`, because it contains remote task versioning and task runtime state.

Testing:

1. Adapter round-trip tests:

   ```text
   Java object -> proto -> Java object
   ```

2. Proto round-trip tests:

   ```text
   proto -> Java object -> proto
   ```

3. Field coverage tests ensuring every required Java field is explicitly mapped.
4. Default-value tests for optional/missing proto fields.
5. Backward compatibility tests using older protobuf fixtures.
6. Failure tests for invalid proto messages.

### 5.3 `presto-main`

`presto-main` owns REST resources and task/query execution services. It will expose `/v2` resources next to the existing `/v1` resources.

Responsibilities:

1. Keep existing `/v1` resources unchanged.
2. Add new `/v2` resource classes.
3. Add content-type handling for protobuf request/response bodies.
4. Add worker capability discovery.
5. Add coordinator-side routing logic to prefer `/v2` when the remote worker supports it.
6. Add fallback to `/v1` during rolling upgrades.

Suggested new resource classes:

```text
com.facebook.presto.server.protocol.v2.TaskResourceV2
com.facebook.presto.server.protocol.v2.QueryResourceV2
com.facebook.presto.server.protocol.v2.StatementResourceV2
com.facebook.presto.server.protocol.v2.NodeResourceV2
```

Existing resource classes stay unchanged:

```text
com.facebook.presto.server.TaskResource
com.facebook.presto.server.QueryResource
com.facebook.presto.server.protocol.QueuedStatementResource
com.facebook.presto.server.protocol.ExecutingStatementResource
```

Testing:

1. Resource unit tests for protobuf input/output.
2. Integration tests where coordinator talks to worker using `/v2`.
3. Mixed-cluster tests:
   - v2 coordinator, v2 worker.
   - v2 coordinator, v1-only worker.
   - v1 coordinator, v2-capable worker.
4. Error handling tests.
5. HTTP content-type negotiation tests.
6. End-to-end distributed query tests.

### 5.4 `presto-client`

`presto-client` currently owns public client DTOs such as `QueryResults`. These must remain compatible for `/v1`.

Responsibilities:

1. Keep existing `/v1` Java client protocol unchanged.
2. Add optional `/v2` client support.
3. Add a client-side adapter from `/v2` protobuf `QueryResults` to existing high-level client result abstractions, if we want old Java client APIs to be able to speak `/v2`.
4. Avoid forcing all Java client users to consume protobuf classes directly.

Possible implementation:

```text
JsonQueryDataClient       // existing /v1 behavior
ProtobufQueryDataClient   // new /v2 behavior
PrestoClientProtocol      // selects v1 or v2
```

Testing:

1. Existing `/v1` client tests unchanged.
2. New `/v2` client tests using protobuf fixtures.
3. Compatibility tests verifying that a Java client can consume either `/v1` JSON or `/v2` protobuf and produce equivalent high-level results.
4. CLI tests if CLI is updated to optionally use `/v2`.

### 5.5 `presto-docs`

Documentation must explicitly define the versioned API contract.

Changes:

1. Keep existing `/v1` REST documentation.
2. Add `/v2` REST documentation.
3. Document content types.
4. Document migration guidance.
5. Document `/v1` deprecation policy after `/v2` reaches feature parity.
6. Document protobuf schema compatibility rules.

Suggested docs layout:

```text
presto-docs/src/main/sphinx/rest/v1/*.rst
presto-docs/src/main/sphinx/rest/v2/*.rst
presto-docs/src/main/sphinx/develop/client-protocol-v1.rst
presto-docs/src/main/sphinx/develop/client-protocol-v2.rst
```

Testing:

1. Docs build.
2. Links from main REST index.
3. Generated schema reference, if we decide to publish one.

### 5.6 `presto-tests` / integration test modules

Responsibilities:

1. Add distributed query tests covering `/v2` internal task communication.
2. Add mixed-version simulation tests.
3. Add public API golden tests for `/v1`.
4. Add `/v2` protobuf golden tests.
5. Add compatibility tests for adapter parity.

## 6. Protocol versioning rules

### 6.1 `/v1` rules

`/v1` remains Jackson JSON and compatibility-focused.

Rules:

1. Do not rename existing JSON fields.
2. Do not remove existing JSON fields.
3. Do not change field meaning.
4. Do not change enum string values unless explicitly handled for compatibility.
5. Additive nullable/optional fields are allowed only with compatibility review.
6. Public `/v1` JSON changes require golden file updates.

### 6.2 `/v2` protobuf rules

`/v2` follows protobuf compatibility rules.

Rules:

1. Never reuse field numbers.
2. Never reuse removed field names without reserving them.
3. Removed fields must be marked `reserved`.
4. New fields must be optional or have safe default behavior.
5. Enum zero value must be an explicit unknown/default value.
6. Enum values must not be renumbered.
7. Unknown fields must be preserved where relevant.
8. Required fields are prohibited.
9. Large opaque payloads should be represented as `bytes` only when the content is already separately versioned.

Example:

```proto
enum TaskState {
  TASK_STATE_UNKNOWN = 0;
  TASK_STATE_PLANNED = 1;
  TASK_STATE_RUNNING = 2;
  TASK_STATE_FINISHED = 3;
  TASK_STATE_CANCELED = 4;
  TASK_STATE_ABORTED = 5;
  TASK_STATE_FAILED = 6;
}
```

## 7. Keeping `/v1` and `/v2` in sync

While both protocols are supported, every protocol-visible change must update both sides or explicitly document why it is version-specific.

This should be enforced with a combination of ownership, registries, tests, and CI checks.

### 7.1 Protocol boundary registry

Introduce a registry file or Java test registry listing all supported protocol boundaries.

Example:

```java
public final class ProtocolBoundaryRegistry
{
    public static final List<ProtocolBoundary<?, ?>> BOUNDARIES = ImmutableList.of(
            boundary("task-update-request", TaskUpdateRequest.class, V2TaskUpdateRequest.class, TaskUpdateRequestAdapter.class),
            boundary("task-status", TaskStatus.class, V2TaskStatus.class, TaskStatusAdapter.class),
            boundary("task-info", TaskInfo.class, V2TaskInfo.class, TaskInfoAdapter.class),
            boundary("basic-query-info", BasicQueryInfo.class, V2BasicQueryInfo.class, BasicQueryInfoAdapter.class),
            boundary("query-info", QueryInfo.class, V2QueryInfo.class, QueryInfoAdapter.class));
}
```

Each boundary declares:

1. `/v1` Java class.
2. `/v2` protobuf class.
3. Adapter.
4. Golden fixture directory.
5. Whether the boundary is public or internal.
6. Whether it is request-only, response-only, or bidirectional.

This registry becomes the source of truth for tests.

### 7.2 Adapter coverage tests

For each registered boundary, add a test that verifies fields are intentionally mapped.

For Java DTOs, the test can inspect:

- Constructor `@JsonProperty` names.
- Getter `@JsonProperty` names.
- Known ignored fields.
- Adapter mapping metadata.

For protobuf messages, the test can inspect:

- Descriptor field names.
- Field numbers.
- Reserved fields.
- Enum values.

The goal is not to automatically prove semantic equivalence. The goal is to prevent silent drift.

Example rule:

```text
If a new @JsonProperty is added to TaskInfo, then either:
  1. The corresponding v2 proto field is added and mapped, or
  2. The field is added to an explicit "v1-only" allowlist with a reason.
```

### 7.3 Explicit compatibility annotations or mapping metadata

Use explicit metadata to document the intended mapping.

Example:

```java
public final class TaskInfoAdapter
        implements ProtocolAdapter<TaskInfo, V2TaskInfo>
{
    private static final ProtocolFieldMapping FIELD_MAPPING = ProtocolFieldMapping.builder(TaskInfo.class, V2TaskInfo.class)
            .maps("taskId", "task_id")
            .maps("taskStatus", "task_status")
            .maps("lastHeartbeatInMillis", "last_heartbeat_millis")
            .maps("outputBuffers", "output_buffers")
            .maps("noMoreSplits", "no_more_splits")
            .maps("stats", "stats")
            .maps("needsPlan", "needs_plan")
            .maps("nodeId", "node_id")
            .build();
}
```

The tests then verify:

1. Every `/v1` public field appears in the mapping metadata.
2. Every mapped `/v2` field exists in the protobuf descriptor.
3. Every mapped `/v1` field exists on the Java DTO.
4. Any unmapped field is explicitly listed as one of:
   - `v1Only`
   - `v2Only`
   - `internalOnly`
   - `deprecated`
   - `derived`
   - `intentionallyOmitted`

This is more maintainable than relying on reflection alone.

### 7.4 Golden compatibility tests

For every public `/v1` boundary:

```text
Java object -> /v1 JSON
```

must match checked-in golden JSON fixtures.

For every `/v2` boundary:

```text
Java object -> /v2 proto -> deterministic textproto
```

must match checked-in golden textproto fixtures.

For every boundary with both versions:

```text
Java object -> /v1 JSON
Java object -> /v2 proto -> Java object -> /v1 JSON
```

The final `/v1 JSON` must match the original `/v1 JSON`, except for explicitly allowed differences.

This catches adapter omissions.

### 7.5 Pull request checklist

Any PR changing protocol-visible objects must answer:

1. Is this field visible in `/v1`?
2. Is this field visible in `/v2`?
3. Is this field public or internal-only?
4. Does the `/v1` golden JSON change?
5. Does the `/v2` protobuf schema change?
6. Does the adapter mapping change?
7. Is the change backward compatible?
8. Does mixed-cluster behavior still work?

This checklist should be added to contributor docs or PR templates.

### 7.6 CI enforcement

Add CI tests that fail when:

1. A registered `/v1` DTO adds/removes a `@JsonProperty` without updating mapping metadata.
2. A registered protobuf schema changes without updating fixtures.
3. A protobuf field number is reused.
4. A protobuf enum value is renumbered.
5. An adapter does not cover all required fields.
6. `/v1` golden JSON changes unexpectedly.
7. `/v2` golden textproto changes unexpectedly.

## 8. Migration phases

### Phase 0: Freeze and inventory current `/v1` public protocol

Objective: establish the existing `/v1` API as a compatibility contract before adding `/v2`.

Scope:

1. Inventory public REST endpoints documented under `presto-docs/src/main/sphinx/rest/*.rst` and `presto-docs/src/main/sphinx/develop/client-protocol.rst`.
2. Identify endpoint path, method, resource class, request type, response type, public object graph, and whether the endpoint is public, internal, or mixed.

Module changes:

- `presto-docs`: add or update a protocol inventory section listing public `/v1` endpoints.
- `presto-main`: no behavior change.
- `presto-main-base`: no behavior change.

Tests:

- Add baseline golden JSON tests for important `/v1` public objects:
  - `QueryResults`
  - `BasicQueryInfo`
  - `QueryInfo`
  - `TaskInfo`
  - `TaskStatus`
  - `StageInfo`
  - `SessionRepresentation`

Acceptance criteria:

1. Public `/v1` endpoints are documented.
2. Golden JSON fixtures exist for major response DTOs.
3. No `/v1` behavior changes.
4. CI can detect accidental `/v1` JSON changes.

### Phase 1: Add protobuf schema infrastructure

Objective: introduce canonical protocol schema support without changing runtime behavior.

Module changes:

- `presto-protocol`: add initial `.proto` files.
- `presto-main-base`: add adapter package and base `ProtocolAdapter<J, P>` interface.
- `presto-main`: no endpoint behavior change yet.

Initial messages should cover only the first migration target: task update.

Likely first schema set:

```proto
message V2TaskUpdateRequest
message V2SessionRepresentation
message V2TaskSource
message V2ScheduledSplit
message V2OutputBuffers
message V2TableWriteInfo
```

Tests:

1. Protobuf compile tests.
2. Adapter unit tests.
3. Schema compatibility tests.
4. Fixture tests for `V2TaskUpdateRequest`.

Acceptance criteria:

1. Build generates protobuf classes.
2. Initial proto schemas are checked in.
3. No production traffic uses `/v2` yet.
4. Existing `/v1` tests remain unchanged.

### Phase 2: Add `/v2/task/{taskId}` for internal task updates

Objective: add the first real `/v2` endpoint for internal coordinator-to-worker task updates.

Endpoint:

```http
POST /v2/task/{taskId}
Content-Type: application/x-presto-protobuf
Accept: application/x-presto-protobuf
```

Behavior:

1. Parse `V2TaskUpdateRequest`.
2. Convert to existing `TaskUpdateRequest` using `TaskUpdateRequestAdapter`.
3. Call the same internal task execution service as `/v1`.
4. Convert resulting `TaskInfo` or `TaskStatus` to `/v2` response if needed.
5. Return protobuf response.

Security:

- Same authorization as existing internal task endpoint.
- Keep `@RolesAllowed(INTERNAL)` equivalent behavior.

Module changes:

- `presto-main`: add `TaskResourceV2`.
- `presto-main-base`: add `TaskUpdateRequestAdapter`, `SessionRepresentationAdapter`, `TaskSourceAdapter`, `ScheduledSplitAdapter`, `OutputBuffersAdapter`, and `TableWriteInfoAdapter`.
- `presto-protocol`: add or finalize task update request/response messages.

Tests:

1. Unit test `TaskResourceV2`.
2. Adapter round-trip tests.
3. Integration test where coordinator sends `/v2/task` update to worker.
4. Negative tests for malformed protobuf, missing semantic fields, unknown enum values, and unsupported protocol versions.
5. Verify `/v1/task` still passes existing tests.

Acceptance criteria:

1. Worker accepts `/v2/task/{taskId}`.
2. Existing `/v1/task/{taskId}` still works.
3. Coordinator does not yet need to prefer `/v2`.
4. No public client behavior changes.

### Phase 3: Add worker capability discovery and coordinator `/v2` preference

Objective: allow the coordinator to use `/v2` task updates when the worker supports them, while preserving rolling upgrade compatibility.

Coordinator behavior:

```text
if worker supports TASK_UPDATE_PROTOBUF:
    send POST /v2/task/{taskId}
else:
    send POST /v1/task/{taskId}
```

The choice should be per-worker, not cluster-global.

Module changes:

- `presto-main`: add worker protocol capability discovery and routing logic.
- `presto-main-base`: no major changes beyond adapter hardening.
- `presto-protocol`: add capability messages if using protobuf for capability discovery.

Tests:

1. Mixed-cluster tests:
   - v2 coordinator + v2 worker uses `/v2`.
   - v2 coordinator + v1 worker falls back to `/v1`.
   - v1 coordinator + v2 worker still uses `/v1`.
2. Rolling restart simulation.
3. Capability cache invalidation tests.
4. Worker downgrade tests.

Acceptance criteria:

1. Mixed clusters work.
2. Coordinator prefers `/v2` only when safe.
3. `/v1` fallback is automatic.
4. No cluster-wide flag is required for correctness.

### Phase 4: Move internal task status/task info responses to `/v2`

Objective: migrate task status and task info responses to canonical protocol.

Rationale:

`TaskStatus` and `TaskInfo` are used internally by remote task management, but they are also reachable through public object graphs. Therefore, do not replace the Java classes immediately. Add explicit `/v2` proto schemas and adapters instead.

Module changes:

- `presto-protocol`: add `V2TaskInfo`, `V2TaskStatus`, `V2ExecutionFailureInfo`, `V2OutputBufferInfo`, and `V2TaskStats`.
- `presto-main-base`: add `TaskInfoAdapter`, `TaskStatusAdapter`, `ExecutionFailureInfoAdapter`, `OutputBufferInfoAdapter`, and `TaskStatsAdapter`.
- `presto-main`: add `/v2` variants for task status/info endpoints and make the coordinator remote task client use `/v2` status/info when supported.

Tests:

1. `TaskStatus` adapter round-trip tests.
2. `TaskInfo` adapter round-trip tests.
3. Golden `/v1` JSON tests remain unchanged.
4. Golden `/v2` textproto tests added.
5. Mixed-cluster tests for task info/status polling.
6. Failure propagation tests.

Acceptance criteria:

1. Internal task status polling can use `/v2`.
2. `/v1` task status/info remains compatible.
3. Public query/task object graphs remain unchanged for `/v1`.

### Phase 5: Introduce `/v2/query` public query metadata APIs

Objective: add `/v2` versions of query metadata endpoints while keeping `/v1/query` intact.

Module changes:

- `presto-protocol`: add `BasicQueryInfo`, `QueryInfo`, `QueryStats`, `BasicQueryStats`, `StageInfo`, `StageExecutionInfo`, `FailureInfo`, and `Warning`.
- `presto-main-base`: add `BasicQueryInfoAdapter`, `QueryInfoAdapter`, `QueryStatsAdapter`, `BasicQueryStatsAdapter`, `StageInfoAdapter`, and `StageExecutionInfoAdapter`.
- `presto-main`: add `QueryResourceV2`.
- `presto-docs`: document `/v2/query`.

Endpoints:

```http
GET /v2/query
GET /v2/query/{queryId}
```

Responses:

```text
BasicQueryInfo list
QueryInfo
```

Tests:

1. Existing `/v1/query` golden JSON tests.
2. New `/v2/query` protobuf fixture tests.
3. Adapter parity tests.
4. Web UI compatibility tests verifying `/v1` remains unchanged.
5. Query failure tests.
6. Query warnings tests.
7. Query session property tests.

Acceptance criteria:

1. `/v2/query` exposes canonical query metadata.
2. `/v1/query` remains unchanged.
3. New query metadata fields require explicit v1/v2 mapping decisions.

### Phase 6: Introduce `/v2/statement` client protocol

Objective: add a canonical `/v2` client protocol for query submission and result fetching.

This is the most externally visible migration and should happen only after internal protocol and query metadata are stable.

Module changes:

- `presto-protocol`: add `V2QueryResults`, `V2Column`, `V2StatementStats`, `V2StageStats`, `V2QueryError`, `V2ClientWarning`, `V2TransactionInfo`, and `V2SessionUpdate`.
- `presto-main-base`: add `QueryResultsAdapter`, `ColumnAdapter`, `StatementStatsAdapter`, `StageStatsAdapter`, and `QueryErrorAdapter`.
- `presto-main`: add `QueuedStatementResourceV2` and `ExecutingStatementResourceV2`.
- `presto-client`: add optional `/v2` mode.
- `presto-docs`: add `/v2` client protocol documentation.

Endpoints:

```http
POST   /v2/statement
GET    /v2/statement/queued/{queryId}/{slug}/{token}
GET    /v2/statement/executing/{queryId}/{slug}/{token}
DELETE /v2/statement/executing/{queryId}/{slug}/{token}
```

Behavior:

1. Query lifecycle remains unchanged.
2. Request/response bodies use `/v2` canonical protocol.
3. `nextUri` values returned from `/v2` must point to `/v2`.
4. `/v1` and `/v2` query lifecycles must not be mixed for a single client query unless explicitly supported.

Tests:

1. Existing `/v1/statement` client protocol tests.
2. New `/v2/statement` client protocol tests.
3. CLI compatibility tests.
4. Java client `/v2` tests.
5. Transaction/session update tests.
6. Error response tests.
7. Query cancellation tests.
8. Spooling/result pagination tests if applicable.
9. Golden result fixtures.

Acceptance criteria:

1. A client can submit and fetch query results through `/v2`.
2. Existing clients continue using `/v1`.
3. `/v2` client protocol has complete documentation.
4. `/v1` and `/v2` result semantics are equivalent.

### Phase 7: Gradually move internal representations toward proto

Objective: after `/v2` boundaries are stable, internal components can begin using proto-backed objects where beneficial.

Important constraint:

This phase is optional and incremental. The existence of `/v2` does not require every internal object to immediately become proto.

Candidate areas:

1. Remote task update payloads.
2. Remote task status snapshots.
3. Query result protocol objects.
4. Exchange metadata.
5. Plan fragment serialization envelopes.
6. Connector handle serialization wrappers.

Module changes:

- `presto-main-base`: some internal services may start storing or passing canonical protocol objects internally.
- `presto-main`: resource classes continue to expose both versions.

Adapters may reverse direction over time:

```text
Before:
  Java execution object -> proto only at HTTP boundary

Later:
  proto internal object -> Java compatibility DTO only for /v1
```

Tests:

1. Existing `/v1` tests still pass.
2. `/v2` tests still pass.
3. Internal service tests update to canonical protocol objects where appropriate.
4. Performance regression tests.

Acceptance criteria:

1. Internal representation can change without impacting `/v1`.
2. `/v1` becomes a compatibility adapter over canonical state.
3. `/v2` remains the native protocol path.

### Phase 8: Deprecate `/v1`

Objective: once `/v2` reaches public feature parity and external tooling has migrated, mark `/v1` deprecated.

Module changes:

- `presto-docs`: add deprecation warnings to `/v1` docs.
- `presto-main`: optionally add response headers.
- `presto-client`: default new clients to `/v2`, with explicit opt-in for `/v1`.

Optional headers:

```http
Deprecation: true
Sunset: <date>
Link: </docs/rest/v2>; rel="successor-version"
```

Tests:

1. Verify headers on `/v1`.
2. Verify `/v1` still works.
3. Verify `/v2` is default where configured.

Acceptance criteria:

1. `/v1` is documented as deprecated.
2. `/v2` is documented as the preferred protocol.
3. Users have a migration path.

### Phase 9: Remove `/v1`

Objective: remove compatibility code after the deprecation window.

Module changes:

- `presto-main`: remove `/v1` resource registrations.
- `presto-main-base`: remove adapters only needed for `/v1` compatibility. Do not necessarily remove execution classes unless they are no longer useful internally.
- `presto-client`: remove or isolate legacy `/v1` client code.
- `presto-docs`: move `/v1` docs to archived documentation.

Tests:

1. Remove `/v1` compatibility tests.
2. Keep `/v2` tests.
3. Keep schema compatibility tests.

Acceptance criteria:

1. `/v2` is the only supported protocol.
2. No public behavior depends on Jackson DTO shapes.
3. Canonical protocol is the source of truth.

## 9. Testing strategy summary

### 9.1 Unit tests

For every adapter:

```text
Java -> proto
proto -> Java
Java -> proto -> Java
proto -> Java -> proto
```

Test:

1. Null/optional handling.
2. Empty collections.
3. Unknown enum values.
4. Large payloads.
5. Failure objects.
6. Backward-compatible missing fields.
7. Forward-compatible unknown fields where applicable.

### 9.2 Golden `/v1` JSON tests

Purpose: ensure public `/v1` output remains unchanged.

Examples:

```text
BasicQueryInfo JSON
QueryInfo JSON
TaskInfo JSON
TaskStatus JSON
QueryResults JSON
```

### 9.3 Golden `/v2` protobuf tests

Purpose: ensure `/v2` schema and adapters are stable.

Use deterministic textproto snapshots for reviewability.

Example:

```text
src/test/resources/protocol/v2/task_info.textproto
src/test/resources/protocol/v2/query_results.textproto
```

### 9.4 Cross-version parity tests

For each boundary:

```text
original Java object
    -> v1 JSON

original Java object
    -> v2 proto
    -> Java object
    -> v1 JSON
```

The two JSON outputs must match except for explicitly allowed differences.

### 9.5 Mixed-cluster tests

Required scenarios:

1. Old coordinator, old worker.
2. Old coordinator, new worker.
3. New coordinator, old worker.
4. New coordinator, new worker.
5. New coordinator with partial worker rollout.
6. Worker downgraded after capability discovery.
7. Coordinator restarted during rollout.

### 9.6 Public API tests

For public endpoints:

1. `/v1/query`
2. `/v1/query/{queryId}`
3. `/v1/statement`
4. `/v1/task` public/debug endpoints, if exposed
5. `/v2/query`
6. `/v2/query/{queryId}`
7. `/v2/statement`

Validate:

1. HTTP status codes.
2. Content types.
3. Headers.
4. Error response shape.
5. Query lifecycle.
6. Cancellation.
7. Session updates.
8. Warnings.
9. Failures.

### 9.7 Performance tests

Compare:

1. `/v1` JSON encode/decode cost.
2. `/v2` protobuf encode/decode cost.
3. Task update payload size.
4. Task status polling overhead.
5. Query result response overhead.
6. Coordinator CPU cost.
7. Worker CPU cost.

Acceptance should be:

- `/v2` is not slower than `/v1` for internal task protocol.
- Payload size should generally improve or remain acceptable.
- No measurable regression in query latency from adapter overhead.

## 10. Compatibility and rollout plan

### 10.1 Default behavior

Initial releases:

```text
/v1 enabled
/v2 enabled but not necessarily preferred
```

Then:

```text
internal task communication prefers /v2 when worker supports it
public clients continue defaulting to /v1
```

Later:

```text
new clients default to /v2
/v1 emits deprecation headers
```

Finally:

```text
/v1 removed
```

### 10.2 Feature flags

Suggested flags:

```properties
protocol.v2.enabled=true
```

This flag controls whether `/v2` endpoints are enabled.

## 11. Concrete first implementation target

The first implementation should be intentionally narrow.

First target:

```http
POST /v2/task/{taskId}
```

Backed by:

```proto
message V2TaskUpdateRequest
```

Adapter:

```text
TaskUpdateRequestAdapter
```

Why this target:

1. Internal-only.
2. High-value.
3. Exercises session serialization, plan fragment payloads, splits, output buffers, and write info.
4. Does not require public clients to change.
5. Gives us the adapter/testing/compatibility infrastructure needed for later phases.

Do not start with:

1. `/v2/statement`
2. Full `QueryInfo`
3. Full `TaskInfo`
4. Full public REST replacement

Those are larger and should come after the task update path proves out the approach.

## 12. Final expected end state

At the end of the migration:

```text
/v2 protobuf protocol is the canonical Presto protocol.
```

The system should look like:

```text
Internal execution state
        |
        | native canonical protocol objects where practical
        v
/v2 resources  ---> protobuf canonical messages

/v1 resources  ---> compatibility adapters only
```

Eventually:

```text
/v1 resources removed
legacy Java DTO compatibility removed where no longer needed
canonical protocol remains
```

This gives us a path that is incremental, testable, compatible, and reversible during rollout.
