# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

---

## 1. LLM Behavioral Guidelines

Behavioral guidelines to reduce common LLM coding mistakes. Merge with project-specific instructions as needed.

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

### Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:

- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them — don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

Storage systems punish assumptions. Read the relevant source files, understand the data flow, and trace the call path before proposing changes. If you are unsure about a component's behavior, read its implementation and tests before modifying it.

### Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

### Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:

- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it — don't delete it.

When your changes create orphans:

- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

### Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:

- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:

```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.

**Use subagents by default** where possible for parallel research, exploration, and independent subtasks.

---

## 2. PR Guidelines

PRs must be logically isolated and easy to review: small, coherent diffs with one purpose per PR. Avoid drive-by refactors.

PR descriptions **must** include a `## Testing Done` section (not `## Testing`).

Aim for as close to 100% unit test coverage as practical for touched logic. Cover risky paths and edge cases.

---

## 3. Ambry System-Level Invariants

### Durability Paranoia

Ambry is a production blob storage system. **"You must never lose data"** is a non-negotiable invariant. Every PR must include a durability risk analysis and tests/mitigations.

**Tradeoff priority order** (when forced to choose):
1. **Durability** — Data must never be lost or silently corrupted
2. **Security** — ACLs, encryption, and auth must not be weakened
3. **Availability** — The system must remain serving
4. **Operability** — Changes must be observable and debuggable
5. **Efficiency** — Resource usage matters but not at the cost of the above
6. **Performance** — Latency and throughput matter but not at the cost of the above

No repo-specific guideline can override the durability invariant.

### Engineering Principles

- Explicit invariants and clear failure-mode handling
- Safe retries and idempotency — operations must be safe to retry after partial failure
- Defensive coding — validate inputs at system boundaries, handle nulls explicitly
- Code should be readable by humans and LLM agents (future maintainers)
- Strong bias toward correctness and operability over cleverness

### Durability Risk Checklist

Apply to every PR. If any item is checked, the PR requires extra scrutiny and explicit testing.

- [ ] **Write path** — Does this change touch PUT, named blob PUT, TTL update, or delete operations?
- [ ] **Metadata** — Does this change modify how blob metadata is stored, read, or indexed?
- [ ] **Ordering / Atomicity** — Could operations complete partially, leaving inconsistent state?
- [ ] **Callback semantics** — Could success be reported to a client before data is durable?
- [ ] **Resource cleanup** — Does this modify ByteBuf release, Closeable.close(), or stream lifecycle?
- [ ] **Retries / Idempotency** — Are retries safe? Could a retry cause duplicate writes or skipped deletes?
- [ ] **Partial failures** — What happens if this operation fails midway? Is the system left in a recoverable state?
- [ ] **Corruption handling** — If data corruption is detected (e.g., MD5 mismatch), is it logged and surfaced, not silently ignored?

### Java Review Standards

- **Do** use SLF4J parameterized logging: `logger.info("Processing blob {} for container {}", blobId, containerName)`. **Avoid** string concatenation in log statements.
- **Do** use correct log levels: `error` = unexpected failures requiring attention, `warn` = recoverable issues, `info` = important state changes, `debug`/`trace` = detailed debugging. **Avoid** logging expected conditions at `error` or `warn`.
- **Do** ensure `LoggerFactory.getLogger()` references the enclosing class. **Avoid** copy-pasting logger declarations — this is a recurring source of bugs.
- **Do** add explicit null checks before dereferencing, especially for values from `RestRequest.getArgs()` and enum comparisons. **Avoid** `enumValue != SOME_ENUM` without a null guard — it returns `true` when `enumValue` is `null`.
- **Do** delete unused code outright. **Avoid** commenting out code blocks. Use version control to recover old code; use config flags for runtime control.
- **Do** make defensive copies of mutable arrays (e.g., `byte[]`) in exactly one place — either the builder setter or the constructor. **Avoid** copying in both.
- **Do** catch the narrowest exception type possible and log with context: `logger.error("Failed to process blob {}", blobId, e)`. **Avoid** catching generic `Exception` without re-throwing.
- **Do** use import statements. **Avoid** fully qualified names in code, unless there is a naming collision.
- **Do** release Netty `ByteBuf` in `finally` blocks. Implement `Closeable` for services holding network clients or other resources. Verify `refCnt() > 0` before calling `release()`.
- **Do** mark static mutable fields as `volatile` or use proper synchronization. **Avoid** unprotected static mutable state — services are long-running and concurrent.
- **Do** extract non-obvious numeric literals to named constants with explanatory comments. **Avoid** magic numbers like `9223372036854775806L` (use `Long.MAX_VALUE - 1` with a comment).
- **Do** annotate with `@VisibleForTesting` when widening field or method visibility solely for test access. **Avoid** silently making fields package-private without documenting why.

### Documentation and Test Update Requirement

Every PR that changes behavior, APIs, config, or architecture must update relevant documentation AND tests.

---

## 4. Repo-Specific Guidelines

### Build and Development Commands

```bash
# Build
./gradlew build -x test                       # Build without tests

# Test
./gradlew test                                # Run all tests

# Run specific test class
./gradlew test --tests "com.github.ambry.clustermap.AmbryReplicaSyncUpManagerTest"

# Run specific test method
./gradlew test --tests "com.github.ambry.clustermap.AmbryReplicaSyncUpManagerTest.bootstrapBasicTest"
```

### Architecture Overview

Ambry is an immutable distributed blob storage system. See [GitHub](https://github.com/linkedin/ambry) for details.

#### Key Modules

| Module | Purpose |
|--------|---------|
| `ambry-api/` | Public interfaces and configs |
| `ambry-clustermap/` | Cluster topology, Helix integration, state management |
| `ambry-replication/` | Cross-datacenter replication engine |
| `ambry-router/` | Client-side routing and operation tracking |
| `ambry-server/` | Storage data layer |
| `ambry-store/` | On-disk blob store implementation |
| `ambry-frontend/` | REST API gateway |
| `ambry-test-utils/` | Shared test mocks and utilities |

#### Package Convention

- `com.github.ambry.*` — All Ambry classes

### CRITICAL: Schema Change Safety

Any change to a schema — whether it is a JSON-serialized Java class, a MySQL DDL, a Protobuf definition, a ZooKeeper node structure, or a Helix property store format — must be treated as a cross-cluster, cross-fabric change. All Ambry clusters across all fabrics read shared data. A backward-incompatible change poisons every cluster simultaneously.

**General rules for ALL schema changes:**

1. **Every schema change MUST be backward and forward compatible.** Backward compatibility means new code can read data written by older schemas. Forward compatibility means old code can safely ignore unknown fields/columns written by newer schemas.

2. **Deployment ordering:** First deploy the code that can **read** the new schema to ALL hosts. Only then deploy the code that **writes** the new schema. Never write new fields/columns before all readers can handle them.

3. **Any PR that changes a schema MUST include compatibility tests:**
   - Forward compatibility test: read data written by the **new** schema (with new fields/columns) using the **old** code path — old readers must tolerate new data
   - Backward compatibility test: read data written by the **old** schema (without new fields/columns) using the **new** code path — new readers must handle missing data with sensible defaults

4. **Key shared schemas to watch:**
   - `Account`, `Container` (JSON in MySQL `account_metadata`)
   - Any class serialized to ZooKeeper or Helix property stores
   - MySQL DDL schemas (column additions, type changes, index changes)
   - Any wire protocol or RPC message format changes

5. **Fail-fast on startup, degrade gracefully at runtime:**
   - If shared metadata (e.g., account data) cannot be loaded at startup, the frontend MUST fail to start — not serve with an empty cache
   - At runtime, not all records are safe to skip. Records affecting access control, quotas, routing, or ownership metadata must fail the operation or raise an alert if they cannot be loaded — silently skipping them could change correctness or authorization. For non-critical record types, skip the failing record and continue, but emit structured logs and metrics with record identifiers so the issue is detectable.

**Jackson-specific rules (for shared-store DTOs):**

6. **Every DTO deserialized from a shared store (MySQL JSON metadata, ZooKeeper, Helix property stores) MUST have `@JsonIgnoreProperties(ignoreUnknown = true)`** at the class level. Without it, adding a new field to the JSON will cause deserialization failures on any host running older code that does not recognize the field. This requirement applies to shared-store DTOs, not necessarily to purely internal config objects or in-process-only types.

7. **When reviewing PRs that touch Jackson-deserialized shared-store DTOs:**
   - Check: Does the class have `@JsonIgnoreProperties(ignoreUnknown = true)`? If not, **block the PR**.
   - Check: Are there tests for unknown field tolerance? If not, **request them**.
   - Test round-trip: serialize → inject unknown fields into the JSON string → deserialize → verify no data loss of known fields.
   - For critical records (account/container metadata), also test that required fields are validated — removing or renaming a required field should fail deserialization or be caught by validation logic.

### Error Handling

- **Do** wrap errors in `RestServiceException` with appropriate `RestServiceErrorCode`.
- **Do** check for errors first in callback handlers before processing results.
- **Do** guard against NPE in validation callbacks: if primaryResult or secondaryResult is null, record a metric and return early.

### Style Rules

- **Imports:** Static imports first, then java.*, third-party, com.github.ambry.*. No wildcards except for static utility constants.
- **Line length:** ~120 characters max.
- **Annotations:** Each on its own line, `@Override` first.
- **Naming:** PascalCase classes, camelCase methods/variables, UPPER_SNAKE_CASE constants, `{Name}Factory`/`{Name}Config`/`{Name}Test` suffixes.
- **Test methods:** `test{Scenario}` (e.g., `testSecondaryFailure`, `testResourceCleanup`).

### Must-Not-Do List

1. **Never change public APIs** (REST endpoints, account/container schemas) unless explicitly instructed
2. **Never change default config values** without explicit instruction — this affects all deployments
3. **Never remove or weaken existing tests** unless instructed
4. **Never use `jdk.internal.*` imports** — they are unstable and fail across JDK versions
5. **Never use `Thread.sleep()` for test synchronization** — use `CountDownLatch`, `CompletableFuture.get(timeout, ...)`, or polling with timeout
6. **Never use `Random` for security-sensitive operations** — use `java.security.SecureRandom`
7. **Never buffer entire blobs in memory** — use streaming patterns throughout
8. **Never widen field visibility without `@VisibleForTesting`** annotation

### Testing Conventions

**Framework:** JUnit 4 (`@Test`, `@Before`, `@After`), Mockito.

**Key patterns:**
- **Do** use `InMemAccountService` for account/container tests, not mocks.
- **Do** verify metric increments against the **actual** Metrics instance used by the class under test. **Avoid** creating a separate Metrics instance in the test — it won't observe real increments.
- **Do** use `CompletableFuture` and `future.get(timeout, unit)` for async callback testing.
- **Do** verify `ByteBuf.refCnt()` drops to 0 in resource cleanup tests.
- **Do** reset static mutable state in `@Before` or `@After` methods to prevent test interdependencies.

**Test file layout:**
```
{module}/src/test/java/       # Unit tests, mirroring main package structure
{module}/src/test/resources/  # Test resources
```

---

## 5. Review Culture

### Copy-paste vigilance for constants

- **Do** verify that when duplicating a constant declaration, BOTH the variable name AND the string literal value are updated.
- **Avoid** the pattern where the variable name is changed but the value still points to the original.

### Async callback race conditions

- **Do** call `primaryFuture.get(timeout, ...)` before reading primary data from request args in secondary comparison callbacks.
- **Avoid** assuming request args are populated before the primary callback has run — if a secondary call completes first, you get stale/null data.

### Test metric verification

- **Do** spy on or retrieve the actual `*Metrics` instance wired into the class under test (via `Whitebox.getInternalState` or the shared `MetricRegistry`).
- **Avoid** creating a fresh `*Metrics` instance in the test and asserting against its counters — it won't observe real increments.
