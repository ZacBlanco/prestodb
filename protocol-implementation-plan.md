# Step-by-Step Implementation Plan: Presto `/v2` Protocol

## 1. Implementation principles

This plan implements the design in `protocol-v2-migration-design.md` incrementally. The implementation should be split into reviewable PRs. Each PR should compile independently, preserve existing `/v1` behavior, and add tests for the new behavior it introduces.

Core rules:

1. Keep `/v1` resources and Java/Jackson DTOs unchanged unless a change is explicitly required for compatibility tests or adapters.
2. Add `/v2` as a parallel protocol surface, not as a replacement for `/v1`.
3. Use protobuf package/module boundaries to identify `/v2` protocol objects. Avoid encoding version information redundantly in every new protobuf message name when the package already provides it.
4. Convert between existing Java objects and protobuf messages only at explicit protocol boundaries.
5. Add tests before enabling any new `/v2` endpoint for production traffic.
6. Use a single coarse feature flag:

   ```properties
   protocol.v2.enabled=true
   ```

7. Do not require rolling-upgrade-specific compatibility work in the first implementation. `/v2` can be introduced as an explicitly enabled parallel endpoint surface.

## 2. Target PR sequence

The recommended implementation sequence is:

1. Add `/v1` protocol guardrails and golden JSON fixtures.
2. Add the `presto-protocol` module and protobuf build plumbing.
3. Add initial task-update protobuf schemas.
4. Add adapter infrastructure and task-update adapters.
5. Add protobuf HTTP serialization support.
6. Add `TaskResourceV2` behind `protocol.v2.enabled`.
7. Add coordinator-side `/v2` task-update client path.
8. Add protocol boundary registry and adapter coverage checks.
9. Add task status/task info protobuf schemas and `/v2` endpoints.
10. Add `/v2/query` metadata endpoints.
11. Add `/v2/statement` client protocol.
12. Move internal representations toward protobuf where useful.
13. Deprecate `/v1` after `/v2` reaches public feature parity.
14. Remove `/v1` after the migration window.

The rest of this document expands each step into concrete implementation tasks and validation gates.

---

## Step 1: Add `/v1` protocol guardrails

### Goal

Freeze the current `/v1` protocol shape before adding `/v2`.

### Modules

- `presto-main-base`
- `presto-main`
- `presto-client`
- `presto-tests`, or the existing module that owns REST/API compatibility tests

### Implementation tasks

1. Identify the highest-value `/v1` protocol DTOs to protect first:
   - `QueryResults`
   - `BasicQueryInfo`
   - `QueryInfo`
   - `TaskInfo`
   - `TaskStatus`
   - `StageInfo`
   - `SessionRepresentation`
2. Add deterministic JSON fixture generation helpers.
3. Add checked-in golden JSON files for representative objects.
4. Add tests that serialize current Java objects to JSON and compare against fixtures.
5. Add an explicit fixture update workflow, such as a test flag or developer-only utility, if the repository already has one.

### Validation

Run targeted tests for the affected modules, for example:

```bash
./mvnw test -pl presto-main-base -Dtest='*Protocol*Test,*Json*Test'
./mvnw test -pl presto-client -Dtest='*QueryResults*Test'
```

### Exit criteria

1. Golden JSON fixtures exist for the initial `/v1` DTO set.
2. Tests fail if a protected `/v1` JSON field is renamed, removed, or unexpectedly changed.
3. No production code behavior changes.

---

## Step 2: Add the `presto-protocol` module

### Goal

Introduce a dedicated module for canonical protobuf schemas and generated Java code.

### Modules

- Root Maven build
- New `presto-protocol` module

### Implementation tasks

1. Add a new Maven module:

   ```text
   presto-protocol/
   ```

2. Add protobuf build configuration.
3. Add the module to the root Maven reactor.
4. Add initial proto directory structure:

   ```text
   presto-protocol/src/main/proto/presto/v2/common.proto
   presto-protocol/src/main/proto/presto/v2/session.proto
   presto-protocol/src/main/proto/presto/v2/task.proto
   presto-protocol/src/main/proto/presto/v2/query.proto
   presto-protocol/src/main/proto/presto/v2/statement.proto
   ```

5. Configure generated Java package names so protobuf classes do not collide with existing Java DTO packages.
6. Add an initial smoke-test proto message if needed to verify generation.
7. Ensure the module depends only on protobuf runtime and build plugins.

### Naming rule

Use the protobuf package/module to encode versioning. Prefer names like:

```proto
package presto.v2;

message TaskUpdateRequest {}
message TaskInfo {}
message QueryInfo {}
```

over names like:

```proto
message V2TaskUpdateRequest {}
```

If Java generated names conflict with existing classes, solve that with generated package names or outer class names, not by adding `V2` to every message name.

### Validation

```bash
./mvnw test -pl presto-protocol
```

### Exit criteria

1. `presto-protocol` builds independently.
2. Protobuf Java classes are generated.
3. No existing modules depend on `presto-protocol` yet, except where necessary for compilation smoke tests.

---

## Step 3: Add initial task-update protobuf schemas

### Goal

Define the first canonical `/v2` protocol boundary: task update.

### Modules

- `presto-protocol`

### Implementation tasks

1. Add common primitive wrapper messages in `common.proto` only where required. Avoid unnecessary wrappers if native protobuf types are sufficient.
2. Add session protocol messages in `session.proto` for fields required by `SessionRepresentation`.
3. Add task update messages in `task.proto`:
   - `TaskUpdateRequest`
   - `TaskSource`
   - `ScheduledSplit`
   - `OutputBuffers`
   - `TableWriteInfo`
4. Represent complex or not-yet-modeled internal payloads as explicit bytes only when the payload already has a separate serialization contract.
5. Add enum zero values for all enums.
6. Reserve no fields initially, but document the rule that deleted fields must be reserved.
7. Add deterministic textproto fixtures for representative task-update messages.

### Validation

```bash
./mvnw test -pl presto-protocol
```

### Exit criteria

1. Task update schemas compile.
2. The schema covers the minimum fields needed to reconstruct the existing Java `TaskUpdateRequest`.
3. Initial textproto fixtures are checked in.

---

## Step 4: Add adapter infrastructure

### Goal

Add explicit Java/protobuf conversion support without changing runtime behavior.

### Modules

- `presto-main-base`
- `presto-protocol`

### Implementation tasks

1. Add a dependency from `presto-main-base` to `presto-protocol`.
2. Add adapter package:

   ```text
   presto-main-base/src/main/java/com/facebook/presto/protocol/v2/adapter/
   ```

3. Add adapter interface:

   ```java
   public interface ProtocolAdapter<J, P>
   {
       P toProtocol(J value);
       J fromProtocol(P value);
   }
   ```

4. Add adapter helpers for repeated fields, optionals, maps, enums, and byte arrays.
5. Add task-update adapters:
   - `TaskUpdateRequestAdapter`
   - `SessionRepresentationAdapter`
   - `TaskSourceAdapter`
   - `ScheduledSplitAdapter`
   - `OutputBuffersAdapter`
   - `TableWriteInfoAdapter`
6. Add tests for every adapter:
   - Java object to proto.
   - Proto to Java object.
   - Java object to proto to Java object.
   - Proto to Java object to proto.
7. Add explicit tests for optional fields, empty collections, missing optional proto fields, and unknown enum values.

### Validation

```bash
./mvnw test -pl presto-main-base -Dtest='*AdapterTest,*Protocol*Test'
```

### Exit criteria

1. Task-update adapters are complete and tested.
2. Existing `/v1` DTO serialization tests still pass.
3. No HTTP endpoint behavior changes.

---

## Step 5: Add protobuf HTTP serialization support

### Goal

Teach the server to read and write protobuf payloads for `/v2` resources.

### Modules

- `presto-main`
- Possibly `presto-main-base` if shared utilities belong there

### Implementation tasks

1. Define the `/v2` protobuf media type:

   ```text
   application/x-presto-protobuf
   ```

2. Add constants for the media type in a shared protocol utility class.
3. Add request parsing helpers for protobuf messages.
4. Add response helpers for protobuf messages.
5. Add error handling for malformed protobuf bodies.
6. Add tests for content type and accept handling.
7. Verify existing JSON resource behavior remains unchanged.

### Validation

```bash
./mvnw test -pl presto-main -Dtest='*Resource*Test,*Protocol*Test'
```

### Exit criteria

1. Server-side code can parse protobuf request bodies.
2. Server-side code can emit protobuf responses.
3. Malformed protobuf input returns a predictable client error.
4. `/v1` JSON behavior is unaffected.

---

## Step 6: Add `protocol.v2.enabled`

### Goal

Add one coarse-grained feature flag for enabling `/v2` endpoints.

### Modules

- `presto-main`
- `presto-main-base` if the configuration class lives there

### Implementation tasks

1. Add config property:

   ```properties
   protocol.v2.enabled=true
   ```

2. Add a strongly typed config field, likely in the existing server/features configuration area.
3. Default the flag according to the desired rollout posture. If unsure, default to disabled for the first merge and enable in test configurations only.
4. Inject the config into `/v2` resource registration or resource handlers.
5. Add config tests.

### Validation

```bash
./mvnw test -pl presto-main -Dtest='*Config*Test'
```

### Exit criteria

1. Exactly one protocol-v2 feature flag exists.
2. `/v2` resources can be disabled by configuration.
3. No per-endpoint `/v2` flags are introduced.

---

## Step 7: Add `TaskResourceV2`

### Goal

Expose the first `/v2` endpoint for internal task updates.

### Modules

- `presto-main`
- `presto-main-base`
- `presto-protocol`

### Endpoint

```http
POST /v2/task/{taskId}
Content-Type: application/x-presto-protobuf
Accept: application/x-presto-protobuf
```

### Implementation tasks

1. Add `TaskResourceV2` next to the existing task resource implementation.
2. Keep the same authorization model as the existing internal task endpoint.
3. In the handler:
   - Parse protobuf `TaskUpdateRequest`.
   - Convert to Java `TaskUpdateRequest` with `TaskUpdateRequestAdapter`.
   - Call the same task execution path used by `/v1`.
   - Convert the response to protobuf if the endpoint returns a body.
4. Ensure `/v1/task/{taskId}` remains registered and unchanged.
5. Guard the endpoint with `protocol.v2.enabled`.
6. Add tests for enabled and disabled behavior.

### Validation

```bash
./mvnw test -pl presto-main -Dtest='TestTaskResourceV2,TestTaskResource'
```

### Exit criteria

1. `/v2/task/{taskId}` accepts protobuf task updates when enabled.
2. `/v2/task/{taskId}` is unavailable or rejects requests predictably when disabled.
3. `/v1/task/{taskId}` behavior remains unchanged.

---

## Step 8: Add coordinator-side `/v2` task-update client path

### Goal

Allow internal coordinator code to send task updates through `/v2` when configured to do so.

### Modules

- `presto-main`
- `presto-main-base`
- `presto-protocol`

### Implementation tasks

1. Identify the remote task client code path that currently calls `/v1/task/{taskId}`.
2. Add a parallel request builder for `/v2/task/{taskId}`.
3. Serialize `TaskUpdateRequest` through `TaskUpdateRequestAdapter`.
4. Send `application/x-presto-protobuf` request bodies.
5. Decode protobuf responses if present.
6. Select `/v2` only when `protocol.v2.enabled` is true.
7. Keep the `/v1` client path intact.
8. Add tests that verify the requested URI and content type.

### Validation

```bash
./mvnw test -pl presto-main -Dtest='*RemoteTask*Test,*Task*Client*Test'
```

### Exit criteria

1. Coordinator can send task updates over `/v2` in tests.
2. Coordinator still uses `/v1` when `/v2` is disabled.
3. End-to-end task execution works with `/v2` enabled in a test cluster.

---

## Step 9: Add protocol boundary registry and coverage checks

### Goal

Prevent `/v1` and `/v2` protocol drift as more boundaries are migrated.

### Modules

- `presto-main-base`
- `presto-protocol`

### Implementation tasks

1. Add a protocol boundary registry in test code or production-adjacent test utilities.
2. Register the task-update boundary first.
3. Add field mapping metadata to adapters.
4. Add tests that verify:
   - Every mapped Java field exists.
   - Every mapped protobuf field exists.
   - Every public Java field is mapped or explicitly ignored with a reason.
5. Add allowlist categories:
   - `v1Only`
   - `v2Only`
   - `internalOnly`
   - `deprecated`
   - `derived`
   - `intentionallyOmitted`

### Validation

```bash
./mvnw test -pl presto-main-base -Dtest='*ProtocolBoundary*Test,*AdapterCoverage*Test'
```

### Exit criteria

1. Adding a new protected `/v1` field without an explicit mapping decision fails tests.
2. Removing or renaming a protected protobuf field fails tests or fixtures.
3. Adapter mapping decisions are reviewable in code.

---

## Step 10: Add task status/task info schemas and adapters

### Goal

Extend `/v2` to task status and task info responses.

### Modules

- `presto-protocol`
- `presto-main-base`

### Implementation tasks

1. Add task response messages in `task.proto`:
   - `TaskStatus`
   - `TaskInfo`
   - `ExecutionFailureInfo`
   - `OutputBufferInfo`
   - `TaskStats`
2. Add adapters:
   - `TaskStatusAdapter`
   - `TaskInfoAdapter`
   - `ExecutionFailureInfoAdapter`
   - `OutputBufferInfoAdapter`
   - `TaskStatsAdapter`
3. Register these boundaries in the protocol boundary registry.
4. Add golden textproto fixtures.
5. Add parity tests against `/v1` JSON fixtures where possible.

### Validation

```bash
./mvnw test -pl presto-protocol
./mvnw test -pl presto-main-base -Dtest='*TaskInfo*Adapter*Test,*TaskStatus*Adapter*Test,*ProtocolBoundary*Test'
```

### Exit criteria

1. Task status/info messages are modeled in protobuf.
2. Task status/info adapters round-trip representative objects.
3. `/v1` JSON fixtures remain unchanged.

---

## Step 11: Add `/v2` task status/info endpoints

### Goal

Expose protobuf task status/info endpoints.

### Modules

- `presto-main`
- `presto-main-base`
- `presto-protocol`

### Implementation tasks

1. Add `/v2` routes corresponding to task status/info endpoints.
2. Reuse the same underlying task state services as `/v1`.
3. Convert Java `TaskStatus` and `TaskInfo` to protobuf at the boundary.
4. Guard endpoints with `protocol.v2.enabled`.
5. Add resource tests for success, failure, not-found, and disabled cases.
6. Update coordinator-side polling code to use `/v2` when `protocol.v2.enabled` is true.

### Validation

```bash
./mvnw test -pl presto-main -Dtest='*TaskResourceV2*Test,*RemoteTask*Test'
```

### Exit criteria

1. Internal task status/info polling works over `/v2` in tests.
2. `/v1` task status/info behavior remains unchanged.
3. Public object graphs exposed through `/v1` remain compatible.

---

## Step 12: Add `/v2/query` protobuf schemas and adapters

### Goal

Add canonical protobuf types for query metadata.

### Modules

- `presto-protocol`
- `presto-main-base`

### Implementation tasks

1. Add query metadata messages in `query.proto`:
   - `BasicQueryInfo`
   - `QueryInfo`
   - `QueryStats`
   - `BasicQueryStats`
   - `StageInfo`
   - `StageExecutionInfo`
   - `FailureInfo`
   - `Warning`
2. Add adapters:
   - `BasicQueryInfoAdapter`
   - `QueryInfoAdapter`
   - `QueryStatsAdapter`
   - `BasicQueryStatsAdapter`
   - `StageInfoAdapter`
   - `StageExecutionInfoAdapter`
3. Register query boundaries in the protocol boundary registry.
4. Add golden textproto fixtures for representative running, finished, and failed queries.
5. Add parity tests comparing `/v1` JSON before and after proto round trips where feasible.

### Validation

```bash
./mvnw test -pl presto-protocol
./mvnw test -pl presto-main-base -Dtest='*QueryInfo*Adapter*Test,*StageInfo*Adapter*Test,*ProtocolBoundary*Test'
```

### Exit criteria

1. Query metadata schemas compile.
2. Query adapters cover the initial public metadata surface.
3. `/v1/query` JSON fixtures remain unchanged.

---

## Step 13: Add `/v2/query` endpoints

### Goal

Expose public query metadata through `/v2` while keeping `/v1/query` intact.

### Modules

- `presto-main`
- `presto-main-base`
- `presto-protocol`

### Endpoints

```http
GET /v2/query
GET /v2/query/{queryId}
```

### Implementation tasks

1. Add `QueryResourceV2`.
2. Reuse the same query manager/service calls as `QueryResource`.
3. Convert `BasicQueryInfo` lists and `QueryInfo` objects to protobuf at the boundary.
4. Return `application/x-presto-protobuf` responses.
5. Guard endpoints with `protocol.v2.enabled`.
6. Add tests for:
   - Query list response.
   - Single query response.
   - Missing query.
   - Failed query.
   - Disabled `/v2`.
   - Content type.

### Validation

```bash
./mvnw test -pl presto-main -Dtest='*QueryResourceV2*Test,*QueryResource*Test'
```

### Exit criteria

1. `/v2/query` returns protobuf query metadata.
2. `/v1/query` JSON remains unchanged.
3. Web UI compatibility is not affected.

---

## Step 14: Add `/v2/statement` protobuf schemas and adapters

### Goal

Define canonical client protocol result messages.

### Modules

- `presto-protocol`
- `presto-main-base` or `presto-client`, depending on where adapters best fit
- `presto-client`

### Implementation tasks

1. Add statement/client messages in `statement.proto`:
   - `QueryResults`
   - `Column`
   - `StatementStats`
   - `StageStats`
   - `QueryError`
   - `ClientWarning`
   - `TransactionInfo`
   - `SessionUpdate`
2. Add server-side adapters from existing Java client protocol objects to protobuf.
3. Add client-side adapters if the Java client should expose existing high-level result APIs while speaking `/v2`.
4. Add golden textproto fixtures for successful, failed, and paginated results.
5. Add parity tests against `/v1/statement` JSON where practical.

### Validation

```bash
./mvnw test -pl presto-protocol
./mvnw test -pl presto-client -Dtest='*QueryResults*Test,*Protocol*Test'
```

### Exit criteria

1. Statement protocol schemas compile.
2. Query results adapters round-trip representative results.
3. Existing `/v1` client protocol tests still pass.

---

## Step 15: Add `/v2/statement` server endpoints

### Goal

Expose query submission and result fetching through `/v2`.

### Modules

- `presto-main`
- `presto-main-base`
- `presto-protocol`

### Endpoints

```http
POST   /v2/statement
GET    /v2/statement/queued/{queryId}/{slug}/{token}
GET    /v2/statement/executing/{queryId}/{slug}/{token}
DELETE /v2/statement/executing/{queryId}/{slug}/{token}
```

### Implementation tasks

1. Add `QueuedStatementResourceV2`.
2. Add `ExecutingStatementResourceV2`.
3. Reuse the same query lifecycle services as `/v1/statement`.
4. Ensure `/v2` `nextUri` values point to `/v2` URLs.
5. Return protobuf `QueryResults` responses.
6. Keep `/v1` and `/v2` query lifecycles separated for a single client query.
7. Guard endpoints with `protocol.v2.enabled`.
8. Add resource tests for submission, polling, cancellation, failures, and session updates.

### Validation

```bash
./mvnw test -pl presto-main -Dtest='*StatementResourceV2*Test,*StatementResource*Test'
```

### Exit criteria

1. A query can be submitted through `/v2/statement`.
2. Results can be fetched through `/v2` `nextUri` links.
3. Query cancellation works through `/v2`.
4. Existing `/v1/statement` behavior remains unchanged.

---

## Step 16: Add optional `/v2` Java client support

### Goal

Allow Java client users to opt into `/v2` without forcing direct protobuf consumption.

### Modules

- `presto-client`
- CLI module, if applicable

### Implementation tasks

1. Add a client protocol selection enum or configuration:

   ```java
   V1
   V2
   ```

2. Keep `V1` as the compatibility default until the public migration is complete.
3. Add a `/v2` HTTP client path using protobuf content types.
4. Convert protobuf responses into existing high-level client result abstractions where possible.
5. Add tests using a mock server or existing client test infrastructure.
6. Add CLI opt-in only if CLI migration is in scope for this implementation cycle.

### Validation

```bash
./mvnw test -pl presto-client
```

### Exit criteria

1. Existing Java client behavior remains unchanged by default.
2. Java client can opt into `/v2`.
3. `/v2` client tests cover success, failure, pagination, and cancellation.

---

## Step 17: Add end-to-end and performance coverage

### Goal

Validate that `/v2` is functionally equivalent and does not regress performance.

### Modules

- `presto-tests`
- Product test modules, if applicable
- Benchmark/performance modules, if applicable

### Implementation tasks

1. Add end-to-end tests with `protocol.v2.enabled=true`.
2. Run representative distributed queries using `/v2` internal task communication.
3. Run `/v2/query` metadata tests against live queries.
4. Run `/v2/statement` tests against live query submission.
5. Compare task update payload size between `/v1` JSON and `/v2` protobuf.
6. Compare encode/decode CPU cost for representative task update and query result payloads.

### Validation

```bash
./mvnw test -pl presto-tests
```

Run any existing benchmark suite that covers task updates or client protocol serialization.

### Exit criteria

1. `/v2` works in end-to-end query execution tests.
2. `/v1` tests still pass.
3. `/v2` task-update payload size and CPU overhead are acceptable.

---

## Step 18: Documentation and developer workflow

### Goal

Make the protocol update workflow clear for future contributors.

### Modules

- `presto-docs`
- Existing contributor docs or PR template location, if any

### Implementation tasks

1. Document `/v2` endpoint content type.
2. Document schema compatibility rules:
   - Do not reuse field numbers.
   - Reserve deleted field numbers and names.
   - Do not renumber enums.
   - Keep enum zero as unknown/default.
3. Document how to add a new field:
   - Update protobuf schema.
   - Update adapter.
   - Update boundary registry mapping.
   - Update `/v1` JSON fixture if applicable.
   - Update `/v2` textproto fixture.
4. Document `protocol.v2.enabled`.
5. Add a PR checklist for protocol-visible changes.

### Validation

```bash
./mvnw test -pl presto-docs
```

or the repository's existing docs build command.

### Exit criteria

1. Developers have a clear protocol-change workflow.
2. `/v2` public behavior is documented.
3. Compatibility rules are visible in docs or contributor guidance.

---

## Step 19: Internal representation migration

### Goal

After `/v2` boundaries are stable, start using canonical protobuf objects internally where it reduces conversion cost or simplifies code.

### Modules

- `presto-main-base`
- `presto-main`
- Other modules that own affected internal state

### Implementation tasks

1. Identify hot protocol boundaries where Java-to-proto conversion is repeated frequently.
2. Pick one narrow internal object graph at a time.
3. Change internal storage or passing representation to protobuf only where the execution code does not become less clear.
4. Keep `/v1` compatibility adapters at the resource boundary.
5. Keep `/v2` as the native path for migrated objects.
6. Add performance tests before and after the change.

### Validation

Run targeted unit, integration, and performance tests for each migrated object graph.

### Exit criteria

1. Internal code can use canonical objects without affecting `/v1` output.
2. Adapter overhead decreases or code clarity improves.
3. Tests prove `/v1` and `/v2` remain equivalent at protocol boundaries.

---

## Step 20: `/v1` deprecation and eventual removal

### Goal

Deprecate and eventually remove `/v1` after `/v2` reaches feature parity and external consumers migrate.

### Modules

- `presto-main`
- `presto-client`
- `presto-docs`
- Compatibility tests

### Implementation tasks for deprecation

1. Mark `/v1` documentation as deprecated.
2. Optionally add deprecation headers to `/v1` responses.
3. Keep `/v1` tests active during the deprecation window.
4. Make new clients default to `/v2` only after compatibility requirements are satisfied.

### Implementation tasks for removal

1. Remove `/v1` resource registration.
2. Remove compatibility adapters that only exist for `/v1`.
3. Remove `/v1` client mode or isolate it in legacy code if required.
4. Remove `/v1` golden JSON tests only after `/v1` is unsupported.
5. Keep `/v2` schema compatibility tests permanently.

### Validation

Run the full test suite or the repository's standard pre-merge validation suite.

### Exit criteria

1. `/v2` is the only supported protocol.
2. No public behavior depends on Jackson DTO shapes.
3. Canonical protobuf schemas are the source of truth.

---

## 3. Minimum first milestone

The first concrete milestone should stop after Step 8.

### Included

1. `/v1` JSON guardrails for the initial DTO set.
2. `presto-protocol` module.
3. Task-update protobuf schema.
4. Task-update adapters.
5. Protobuf HTTP support.
6. `protocol.v2.enabled`.
7. `POST /v2/task/{taskId}`.
8. Coordinator-side `/v2` task-update path.

### Not included

1. `/v2/task` status/info polling.
2. `/v2/query`.
3. `/v2/statement`.
4. Java client `/v2` support.
5. Internal representation replacement.

### Milestone validation

```bash
./mvnw test -pl presto-protocol
./mvnw test -pl presto-main-base -Dtest='*AdapterTest,*Protocol*Test'
./mvnw test -pl presto-main -Dtest='TestTaskResourceV2,*RemoteTask*Test'
```

### Milestone acceptance criteria

1. Existing `/v1/task/{taskId}` behavior is unchanged.
2. `/v2/task/{taskId}` works when `protocol.v2.enabled=true`.
3. `/v2/task/{taskId}` is disabled or rejects predictably when `protocol.v2.enabled=false`.
4. The coordinator can execute the task-update path over `/v2` in tests.
5. Initial protocol drift checks are in place or have a follow-up PR immediately queued.

---

## 4. Ongoing compatibility checklist

For every future protocol-visible change, require the author to answer:

1. Is this field visible in `/v1`?
2. Is this field visible in `/v2`?
3. Is this field public, internal-only, derived, deprecated, or intentionally omitted?
4. Does the protobuf schema change?
5. Does the Java/Jackson DTO shape change?
6. Does an adapter change?
7. Does the protocol boundary registry change?
8. Does a `/v1` JSON fixture change?
9. Does a `/v2` textproto fixture change?
10. Is the change backward compatible for existing clients?

If any answer is unclear, the PR should not merge until the protocol mapping decision is explicit.
