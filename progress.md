# Protocol `/v2` Implementation Progress

This file tracks implementation progress against `protocol-implementation-plan.md`.

## Current status

- Overall phase: In progress
- Current milestone: `/v2/statement` server endpoints
- Current focus: Step 15 statement resource implementation

## Progress log

### 2026-05-27

- Created this progress tracker.
- Next action: inspect existing build, protobuf usage, REST resource tests, and JSON compatibility test patterns before making code changes.
- Inspected existing protobuf build patterns using `presto-grpc-api` and root Maven plugin management.
- Added the `presto-protocol` Maven module and registered it in the root reactor.
- Added initial protobuf schemas under `presto-protocol/src/main/proto/presto/v2/`:
  - `common.proto`
  - `session.proto`
  - `task.proto`
  - `query.proto`
  - `statement.proto`
- Added `TestProtocolSchemas` to validate basic generated-message round trips for `TaskUpdateRequest`.
- Added `presto-main-base` dependency on `presto-protocol`.
- Added initial adapter infrastructure under `com.facebook.presto.protocol.v2.adapter`:
  - `ProtocolAdapter`
  - `ProtocolAdapters`
  - `SessionRepresentationAdapter` with initial Java-to-protobuf mapping.
- Removed unrelated prototype files from the previous abandoned codec approach so they do not block compilation:
  - `presto-main-base/src/main/java/com/facebook/presto/codec/*`
  - `presto-main-base/src/main/java/com/facebook/presto/metadata/SqlInvokedFunctionWrapper.java`
  - `presto-main-base/src/main/java/com/facebook/presto/util/ProtocolJsonSerde.java`
- Incorporated the protocol schema rename from `OpaquePayload` to `ConnectorPayload`.
- Added Java-to-protobuf task-update adapters:
  - `ConnectorPayloadAdapter`
  - `LifespanAdapter`
  - `OutputBuffersAdapter`
  - `ScheduledSplitAdapter`
  - `TaskSourceAdapter`
  - `TableWriteInfoAdapter`
  - `TaskUpdateRequestAdapter`
- Added `TestTaskUpdateRequestAdapter` covering `TaskUpdateRequest` Java-to-protobuf mapping and `OutputBuffers` round trip.
- Added direct `protobuf-java` dependency to `presto-main-base` because adapter code uses `ByteString` directly.
- Added `protocol.v2.enabled` to `ServerConfig`, defaulting to disabled, with config mapping coverage in `TestServerConfig`.
- Added the `/v2` protobuf media type constant `application/x-presto-protobuf`.
- Added JAX-RS protobuf message body reader/writer support for `com.google.protobuf.Message` payloads.
- Added partial proto-to-Java adapter support needed by request handling when connector-owned payloads are absent:
  - `SessionRepresentationAdapter#fromProtocol`
  - `TaskSourceAdapter#fromProtocol` for source updates with no split payloads
  - `TableWriteInfoAdapter#fromProtocol` for empty table write info
  - `TaskUpdateRequestAdapter#fromProtocol`
- Added `TaskResourceV2` at `POST /v2/task/{taskId}`, gated by `protocol.v2.enabled`, using the same task manager update path as `/v1` and returning `204 No Content` for the initial update endpoint.
- Registered `TaskResourceV2` and protobuf body providers in `ServerMainModule`.
- Added direct `presto-protocol` and `protobuf-java` dependencies to `presto-main` for the new v2 resource and body providers.
- Implemented connector-owned opaque payload handling using `ConnectorPayload` encoding `json+java-class` with a Jackson JSON wrapper containing `@type` and `payload` fields.
- Wired the shared connector payload adapter through split, task source, table write info, and task update request adapters so `/v2` can decode existing Java/Jackson connector payloads without requiring connectors to move to protobuf yet.
- Updated `TaskResourceV2` to use the server `ObjectMapper` when converting protobuf task update requests back to existing Java request objects.
- Added adapter test coverage for task update request round trips with split payloads using the same Jackson modules needed for connector split and transaction handle deserialization.
- Added coordinator-side `/v2` task-update request construction in `HttpRemoteTaskWithEventLoop`, selected by `protocol.v2.enabled`.
- Wired `ServerConfig` and the server `ObjectMapper` into `HttpRemoteTaskFactory` so the remote task path can serialize task updates through `TaskUpdateRequestAdapter` when `/v2` is enabled.
- Added remote task test coverage proving that the coordinator sends protobuf task updates to `/v2/task/{taskId}` while preserving the existing `/v1` task update path when disabled.
- Added task-update protocol boundary registry coverage in `TestProtocolBoundaryRegistry`, registering the Java/protobuf mapping decisions for `TaskUpdateRequest`, `SessionRepresentation`, `TaskSource`, `ScheduledSplit`, `Lifespan`, `OutputBuffers`, and `TableWriteInfo`.
- Added explicit allowlisted omissions for session functions and the thrift-only `writerTargetUnion` compatibility accessor so future field additions require an explicit mapping or omission decision.
- Tightened task-update adapter coverage for `noMoreSplitsForLifespan` round trips.
- Added task status/task info protobuf schemas to `task.proto`, including `TaskStatus`, `TaskInfo`, `ExecutionFailureInfo`, `OutputBufferInfo`, `TaskStats`, and supporting nested/enumerated types.
- Added task status/info adapter coverage:
  - `TaskStatusAdapter`
  - `TaskInfoAdapter`
  - `ExecutionFailureInfoAdapter`
  - `OutputBufferInfoAdapter`
  - `TaskStatsAdapter`
- Registered task status/info boundary mappings in `TestProtocolBoundaryRegistry` so field additions require explicit mapping decisions.
- Added `TestTaskStatusInfoAdapters` round-trip coverage for task status, task info, and task stats.
- Added `/v2` task status/info GET endpoints in `TaskResourceV2`:
  - `GET /v2/task/{taskId}` returns protobuf `TaskInfo` and preserves `summarize` support.
  - `GET /v2/task/{taskId}/status` returns protobuf `TaskStatus` and preserves long-poll headers.
- Added `ProtobufResponseHandler` so coordinator-side polling can parse protobuf responses through the existing `BaseResponse` callback flow.
- Wired `ContinuousTaskStatusFetcherWithEventLoop` and `TaskInfoFetcherWithEventLoop` to poll `/v2` endpoints with `Accept: application/x-presto-protobuf` when `protocol.v2.enabled` is true.
- Extended `TestHttpRemoteTaskWithEventLoop` coverage to assert v2 task update, task status polling, and task info polling all use `/v2` endpoints when enabled.
- Added initial query metadata adapter coverage for existing `query.proto` projections:
  - `BasicQueryInfoAdapter`
  - `BasicQueryStatsAdapter`
  - `QueryInfoAdapter`
  - `QueryStatsAdapter`
  - `StageInfoAdapter`
  - `StageExecutionInfoAdapter`
- Registered query metadata boundaries in `TestProtocolBoundaryRegistry`, explicitly documenting fields deferred from the initial query metadata projection.
- Added `TestQueryInfoAdapters` coverage for basic query info, full query info with/without output stage, recursive stage mapping, and server-to-client-only reverse conversion behavior.
- Added `BasicQueryInfoList` to `query.proto` so `/v2/query` can return a protobuf query list response without relying on JSON arrays.
- Added `QueryResourceV2` with protobuf `GET /v2/query` and `GET /v2/query/{queryId}` endpoints, guarded by `protocol.v2.enabled` and leaving the existing `/v1/query` resource unchanged.
- Registered `QueryResourceV2` in `CoordinatorModule` alongside the existing `QueryResource`.
- Added `TestQueryResourceV2` coverage for query listing, state/limit filters, single full query response, missing query, disabled `/v2`, invalid limits, and resource-manager mode behavior.
- Expanded `statement.proto` to cover the initial client statement result projection, including optional URIs, columns, binary result data, statement/stage stats, query errors, warnings, and update metadata.
- Added statement protocol adapters:
  - `QueryResultsAdapter`
  - `ColumnAdapter`
  - `StatementStatsAdapter`
  - `StageStatsAdapter`
  - `QueryErrorAdapter`
  - `ClientWarningAdapter`
- Registered statement protocol boundaries in `TestProtocolBoundaryRegistry`, explicitly documenting deferred JSON row data, type signatures, runtime stats, error locations, and recursive failure info.
- Added `TestStatementAdapters` coverage for representative statement result round trips and omitted optional fields.

### 2026-05-28

- Added `/v2/statement` endpoint coverage to the existing statement resources while leaving `/v1/statement` paths intact:
  - query submission and pre-minted query submission return protobuf `QueryResults` when `protocol.v2.enabled` is true.
  - queued polling, retry polling, executing polling, and cancellation are available under `/v2/statement/...`.
  - v2 polling generates follow-up `nextUri` and retry URIs under `/v2/statement` instead of redirecting clients back to `/v1/statement`.
- Extended the shared statement response utilities so executing responses can preserve existing session/transaction response headers while serializing the response entity as protobuf.
- Extended the local queued-to-executing fast path so queued v2 polling can directly return protobuf executing results without an extra JSON handoff.
- Added targeted statement URI regression tests proving v2 queued URIs are generated under `/v2/statement` while the default v1 behavior remains unchanged.

## Step checklist

| Step | Description | Status | Notes |
| --- | --- | --- | --- |
| 1 | Add `/v1` protocol guardrails | Deferred | Skipped for initial code path to first establish proto module and compile foundation. Should be added before enabling public `/v2` APIs. |
| 2 | Add `presto-protocol` module | Complete | Module builds and runs its schema smoke test. |
| 3 | Add initial task-update protobuf schemas | Complete | Initial task-update schema exists and uses `ConnectorPayload` for connector-owned serialized payloads. |
| 4 | Add adapter infrastructure | Mostly complete | Java-to-protobuf task-update adapter path is implemented and tested. Proto-to-Java task update conversion now supports existing Jackson-backed connector payloads via `json+java-class`; remaining gaps are non-task-update protocol areas and any payload classes without registered Jackson modules. |
| 5 | Add protobuf HTTP serialization support | Complete | Added protobuf media type plus generic JAX-RS protobuf reader/writer. |
| 6 | Add `protocol.v2.enabled` | Complete | Added to `ServerConfig`, default disabled, with config tests. |
| 7 | Add `TaskResourceV2` | Mostly complete | Registered `/v2/task/{taskId}` behind config. Endpoint accepts protobuf task updates, converts them through the adapter stack using the injected server `ObjectMapper`, and returns 204. |
| 8 | Add coordinator-side `/v2` task-update client path | Complete | `HttpRemoteTaskWithEventLoop` serializes `TaskUpdateRequest` as protobuf, sends `application/x-presto-protobuf` to `/v2/task/{taskId}`, and treats the `204 No Content` response as task-update acknowledgement when `protocol.v2.enabled` is true. Existing `/v1` JSON/Smile/Thrift paths remain intact when disabled. |
| 9 | Add protocol boundary registry and adapter coverage checks | Complete | Added test-side boundary registry coverage for task-update Java/protobuf mappings and explicit allowlisted omissions. |
| 10 | Add task status/task info schemas and adapters | Complete | Added protobuf messages and Java/protobuf adapters for task status/info response objects, with round-trip tests and boundary registry mapping coverage. |
| 11 | Add `/v2` task status/info endpoints | Complete | `TaskResourceV2` now exposes protobuf task info/status GET endpoints. Coordinator-side task info/status fetchers use these endpoints when `protocol.v2.enabled` is true, with targeted remote-task test coverage. |
| 12 | Add `/v2/query` protobuf schemas and adapters | Complete | Reused the existing initial `query.proto` metadata shape and added server-to-client projection adapters for basic/full query info, basic/full query stats, and stage metadata. Full proto-to-Java reconstruction is intentionally unsupported because the initial query protobuf omits required Java DTO constructor fields. |
| 13 | Add `/v2/query` endpoints | Complete | Added `QueryResourceV2` for protobuf query list and full query metadata responses, registered it in the coordinator module, and added targeted resource tests. Resource-manager proxying is intentionally not implemented for the first local-coordinator endpoint slice. |
| 14 | Add `/v2/statement` protobuf schemas and adapters | Complete | Expanded statement protobuf messages and added bidirectional adapters for query results, columns, statement/stage stats, query errors, and client warnings. Row-oriented JSON data is intentionally deferred; binary result data is covered. |
| 15 | Add `/v2/statement` endpoints | Complete | Added protobuf `/v2/statement` submission, queued/executing polling, retry, and cancel endpoints on the existing statement resources. The v2 paths are gated by `protocol.v2.enabled`; v1 JSON endpoints remain unchanged. |

## Validation log

- `./mvnw test -pl presto-protocol` — PASS. Validates protobuf generation and `TestProtocolSchemas`.
- `./mvnw test -pl presto-main-base -am -DskipTests` — PASS after removing unrelated prototype files and adding an initial generated-protocol reference from `SessionRepresentationAdapter`.
- `./mvnw test -pl presto-protocol,presto-main-base -am -Dtest=TestProtocolSchemas,TestTaskUpdateRequestAdapter -Dsurefire.failIfNoSpecifiedTests=false` — PASS. Validates schema smoke test and task-update adapter tests.
- `./mvnw test -pl presto-main-base -am -Dtest=TestServerConfig,TestTaskUpdateRequestAdapter -Dsurefire.failIfNoSpecifiedTests=false` — PASS. Validates protocol v2 config mapping and task-update adapter coverage.
- `./mvnw test -pl presto-main -am -DskipTests` — PASS. Validates `presto-main` compilation with `TaskResourceV2` and protobuf body providers.
- `./mvnw test -pl presto-main-base -am -Dtest=TestTaskUpdateRequestAdapter -Dsurefire.failIfNoSpecifiedTests=false` — PASS. Validates task-update adapter coverage including `json+java-class` connector split payload round trip.
- `./mvnw test -pl presto-main -am -Dtest=TestHttpRemoteTaskWithEventLoop#testRegularWithProtocolV2TaskUpdate -Dsurefire.failIfNoSpecifiedTests=false` — PASS. Validates coordinator-side `/v2` task-update request path.
- `./mvnw test -pl presto-main -am -Dtest=TestHttpRemoteTaskWithEventLoop#testRegular -Dsurefire.failIfNoSpecifiedTests=false` — PASS. Validates existing remote task update behavior still works with `/v2` disabled.
- `./mvnw test -pl presto-main -am -DskipTests` — PASS. Validates `presto-main` and dependency compile/test-compile after wiring `ServerConfig` and `ObjectMapper` through `HttpRemoteTaskFactory`.
- `./mvnw test -pl presto-main-base,presto-main -am -Dtest=TestTaskUpdateRequestAdapter,TestServerConfig,TestHttpRemoteTaskWithEventLoop -Dsurefire.failIfNoSpecifiedTests=false` — PASS. Validates broader task-update adapter/config/remote-task coverage without method filtering.
- `./mvnw test -pl presto-main-base -am -Dtest=TestTaskUpdateRequestAdapter,TestProtocolBoundaryRegistry -Dsurefire.failIfNoSpecifiedTests=false` — PASS. Validates task-update adapter round trips and protocol boundary mapping coverage checks.
- `./mvnw test -pl presto-protocol,presto-main-base -am -Dtest=TestProtocolSchemas,TestTaskStatusInfoAdapters,TestTaskUpdateRequestAdapter,TestProtocolBoundaryRegistry -Dsurefire.failIfNoSpecifiedTests=false` — PASS after fixing import ordering. Validates protobuf generation, task-update adapter coverage, task status/info adapter round trips, and protocol boundary mapping coverage.
- `./mvnw test -pl presto-main -am -Dtest=TestHttpRemoteTaskWithEventLoop#testRegularWithProtocolV2TaskUpdate -Dsurefire.failIfNoSpecifiedTests=false` — PASS after fixing `TaskResourceV2` import ordering and replacing ambiguous `TaskStatusAdapter` method references with lambdas. Validates v2 task update plus v2 task status/info polling endpoint usage.
- `./mvnw test -pl presto-main-base -am -Dtest=TestQueryInfoAdapters,TestProtocolBoundaryRegistry -Dsurefire.failIfNoSpecifiedTests=false` — PASS after fixing query boundary registry omissions. Validates query metadata adapters and boundary coverage.
- `./mvnw test -pl presto-main -am -Dtest=TestQueryResourceV2,TestQueryInfoAdapters,TestProtocolBoundaryRegistry -Dsurefire.failIfNoSpecifiedTests=false` — PASS. Validates `/v2/query` resource behavior, query metadata adapters, protobuf generation, and protocol boundary coverage.
- `./mvnw test -pl presto-protocol,presto-main-base -am -Dtest=TestStatementAdapters,TestProtocolBoundaryRegistry -Dsurefire.failIfNoSpecifiedTests=false` — PASS. Validates statement protobuf generation, statement adapters, and statement boundary coverage.
- `./mvnw -pl presto-main -am -DskipTests -Dair.check.skip-all=true compile` — PASS. Validates `presto-main` and dependencies compile after adding `/v2/statement` endpoints.
- `./mvnw -pl presto-main -am -Dair.check.skip-all=true -Dtest=com.facebook.presto.server.protocol.TestStatementResourceV2 -Dsurefire.failIfNoSpecifiedTests=false test` — PASS. Validates statement v2 URI generation and v1 default URI preservation. Test output includes a late `LogTestDurationListener` thread exception after Maven reports build success.
- `./mvnw test -pl presto-main -am -Dtest=TestStatementResourceV2,TestStatementAdapters,TestProtocolBoundaryRegistry -Dsurefire.failIfNoSpecifiedTests=false` — PASS. Validates statement v2 resource tests plus statement adapters/boundary checks with checkstyle enabled.
- `./mvnw install -pl presto-main -am` — FAIL on macOS/aarch64 before reaching `presto-main` because `testing-mysql-server-8` only packages `mysql-Linux-amd64.tar.gz` and `mysql-Mac_OS_X-x86_64.tar.gz`, while the local JVM reports `mysql-Mac_OS_X-aarch64.tar.gz`.
- `DOCKER_HOST=unix:///var/run/docker.sock ./mvnw install -pl presto-main -am -Dos.arch=x86_64 -rf :presto-main` — PASS after forcing the available embedded MySQL archive classifier and pointing Testcontainers at the local Docker socket. Validates all `presto-main` tests, checkstyle, spotbugs, PMD, packaging, and install for the resumed `presto-main` module.

## Open notes

- Next implementation step: continue with the next migration-plan step after `/v2/statement` server endpoints.
- Current task-update adapters support Java-to-protobuf conversion and proto-to-Java conversion for Jackson-backed split and table-write payloads.
- Current task status/info adapters support Java-to-protobuf and proto-to-Java conversion for representative response objects. `PipelineStats` and `RuntimeStats` currently use the same Jackson-backed `ConnectorPayload` bridge pattern while deeper canonical stats modeling is deferred.
- Connector-owned payloads are intentionally wrapped as JSON plus fully-qualified Java class names (`@type`) inside protobuf `ConnectorPayload` so connectors can continue using existing Java/Jackson handle classes while the v2 protocol is introduced incrementally.
- Remaining known adapter gap: session functions are still not reconstructed from protobuf task update requests.
