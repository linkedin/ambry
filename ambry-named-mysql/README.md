# Named delete manifests

`stageDeleteManifest` stores ordered child IDs from one immutable metadata blob in pages of 64. The caller must supply
descriptors from that exact root. Staging does not lock or retire the name or expose the manifest to recovery readers.
`retireWithManifests` validates the sealed pages and the locked name versions, then publishes the supplied
manifest set in the same transaction that soft-deletes those versions. If the commit outcome is unknown, retry with
the same manifest IDs. Do not resolve the name again or fall back to the router's ordinary delete.

A reader with a saved manifest ID can look up that published manifest and read its pages by offset after a restart,
without fetching the original root. Configure the reader data source for `READ COMMITTED` or stronger on every checkout.
A pool shared with the publisher must use `REPEATABLE READ`; readers do not change its isolation. Distinct roots can
reference the same child; finding its ID does not authorize deletion.

## Compatibility and limits

These package-private methods have no frontend caller. They require one authoritative InnoDB writer for the name range,
soft deletes, and `REPEATABLE READ` during publication. They do not coordinate independent active-active writers.
Publication locks the indexed range of unfenced versions, including the gap for new inserts. Every live version needs a
matching manifest and must be READY. An elapsed legacy tombstone or expiry needs no manifest: the same transaction
preserves its timestamp and fences it against TTL finalization, without publishing cleanup work for it. Fenced rows no
longer count toward the limit of 32 unfenced versions per name. A name above that limit, or with an unrepresented live
version, fails without changing any row. Exclude such names from rollout until their history has been safely reconciled;
retrying unchanged input will not help. Publication also limits total children to 131,072.

## Installing or resuming the migration

**Defer production DDL until the activation prerequisites below are ready.** Nothing installs automatically, but once
installed the trigger fires on every update, even on unfenced rows with manifests disabled. If its definer disappears
or loses privileges, ordinary TTL finalization and soft deletes can fail.

From `ambry-named-mysql/src/main/resources`, set `DB` to the approved writer's named-blob schema. Use separate mysql
options files: `MYSQL_CNF` for schema DDL and `DEFINER_CNF` for the durable service account that will own the trigger.
The trigger file uses MySQL's implicit `DEFINER=CURRENT_USER`. Before installation, confirm the matched `user@host`
is the intended permanent account, **not** a personal or temporary migration identity. Have the DBA verify on every
writer and promotion candidate that this account exists and retains `TRIGGER`, `SELECT` and `UPDATE` on `named_blobs_v2`
for the trigger's lifetime. Use the approved trigger-install process for the server's binary logging configuration.

```sh
mysql --defaults-file="$DEFINER_CNF" --database="$DB" -e 'SELECT CURRENT_USER(), DATABASE(); SHOW GRANTS'
```

On a fresh installation, check each result before continuing. The column addition is `INSTANT`; even the `INPLACE`,
`LOCK=NONE` index build needs a metadata lock to finish. Schedule a rollout window and monitor lock waits and replica
lag. Bound each metadata-lock wait, not the index build duration, with a five-second **session** timeout. Use the same
timeout for every retry. Do not use a table-copy fallback.

```sh
mysql --defaults-file="$MYSQL_CNF" --database="$DB" --init-command='SET SESSION lock_wait_timeout=5' \
  < NamedBlobManifestV1.ddl
mysql --defaults-file="$DEFINER_CNF" --database="$DB" --init-command='SET SESSION lock_wait_timeout=5' \
  --delimiter='//' -e 'SOURCE NamedBlobRetirementFenceV1.sql'
mysql --defaults-file="$DEFINER_CNF" --database="$DB" -e 'SHOW CREATE TRIGGER named_blob_retirement_fence_v1\G'
```

The delimiter keeps the compound trigger intact in the mysql client; JDBC installers send the entire trigger file as
one statement. Verify the installed `BEFORE UPDATE` body and the exact `DEFINER` on each writer and before promotion.
On each writer, connect **as the application account** and smoke-test an unfenced update: in one transaction, insert
an approved disposable named version, finalize its TTL using its exact primary key, verify one row changed to READY
with `deleted_ts IS NULL`, and roll back. Verify the fixture row is absent afterward. Do not use a customer row. Stop
on any error; a successful install by the DDL operator is not evidence that application writes still work.

**On any error or lost connection, do not rerun the file.** Each successful DDL statement has already committed.
Wait for any in-flight DDL to finish. Inspect `SHOW CREATE TABLE` for both manifest tables and `named_blobs_v2`, and
`SHOW CREATE TRIGGER` if the trigger exists. Compare definitions in installation order: manifest header table, page
table and foreign key, fence column, ordered unfenced-version index, then trigger and definer. Stop on any conflicting
definition. Fix the cause, resume at the first missing statement using the timeout above, and run only the missing
steps in order. An index build can finish scanning the table and still time out waiting for its final metadata lock.
If the column exists but the index does not, have the owners finish any long-running transactions holding the lock
before retrying `ALTER TABLE ... ADD INDEX`; each retry rebuilds the index. Then install the trigger, recheck the
definitions, and run the application smoke test.

## Before enabling a caller

The database operations alone are not safe to activate. In addition to the DELETE and cleaner gates below:

- After verifying the migration on every read endpoint, enable `mysql.named.blob.honor.retirement.fence` on every
  frontend before publishing any retirements. It defaults to `false`, so readers can run against the original schema.
  When enabled, ordinary and include-expired GET return `Deleted` for a fenced version even if the frontend clock is
  behind. Include-deleted and include-all GET return the selected root. Prefix and no-prefix LIST hide a fenced
  selection without falling back to an older live row. A PUT whose version does not sort above the name's fence
  returns `ServiceUnavailable` without inserting and increments `FenceAwarePutVersionRejectedCount`. Retry a
  generated-version PUT once the frontend clock catches up.
- Use a direct, single-host Connector/J writer with automatic reconnect disabled and no intermediary that multiplexes
  backend sessions. Fence-aware PUT rejects multi-host drivers and reconnect modes rather than allowing the probe and
  INSERT to run on different sessions. With the flag on, the factory configures all writable endpoint pools, including
  remote pools used for GET fallback, to reset isolation to `REPEATABLE READ`. It also sets that isolation explicitly
  on each new physical connection to clear any next-transaction override from server initialization. Leave the isolation
  setting unspecified (`TRANSACTION_NONE`) or choose `TRANSACTION_REPEATABLE_READ`; other values prevent pool startup.
  Do not set a next-transaction override after initialization: the session default does not reveal it.
- Fence-aware PUT requires autocommit on and no active transaction. Before the non-upsert existence check or any
  INSERT, it reads the server's autocommit and isolation settings and checks the transaction status in the same
  response: `START TRANSACTION` leaves autocommit on. A rejected checkout is discarded without committing, resetting
  autocommit, or returning its transaction to readers. At `READ COMMITTED`
  the fence lookup can finish before a concurrent publication commits, even though the insert itself waits.
  Even without retirements, concurrent PUTs to the same name or adjacent new names can deadlock in the same index gap.
  On a confirmed InnoDB deadlock (`1213/40001`), an upsert retries the same conditional INSERT once, then returns
  `ServiceUnavailable` if it deadlocks again. A create-only victim returns `ServiceUnavailable` immediately: its
  existence check is stale. Neither a lock timeout nor an unknown commit outcome is retried. Monitor
  `FenceAwarePutDeadlockCount` for every victim, including recovered upserts. `NamedBlobDBInsertErrorCount` counts
  the deadlock only if it is returned to the caller. Monitor `FenceAwarePutSessionRejectedCount` for rejected checkouts.
  `ConnectionCloseErrorCount` counts failed cleanup steps, not distinct checkouts. If reset and discard both fail,
  one checkout increments it twice, even with the flag off. Ambry logs discard failures it sees; physical close
  failures handled inside Hikari are not included.
- These checks cover named PUT only. Before activating manifest publication, verify its required transaction state
  on a fixed physical session as well; publication requires autocommit **off**. Publication and page reads still use
  JDBC isolation getters. Do not activate those callers with a pool that can report cached rather than server state.
- Every serving endpoint, cross-datacenter GET fallback, and promotion candidate must have applied all acknowledged
  retirement transactions, not just the migration. A local `NotFound` can send GET to another datacenter. If an
  endpoint cannot prove its applied position, exclude it from reads, retries, and promotion.
- Before enabling the flag, compare flag-on and flag-off LIST plans and first- and later-page latency on representative
  large containers, with and without a prefix. Check the deployed SQL option, rows examined, and query timeouts. Also
  measure PUT latency, deadlocks and 503s under same-name and sequential-name contention. The integration fixture is not
  a load test.
- Admit only namespaces that will stay within the version limit **with cleanup excluded**. PUT keeps older versions:
  an initial permanent PUT and 32 overwrites can leave 33 unfenced rows. Even below the limit, a live non-composite
  version cannot supply a composite manifest. Restrict writes accordingly or reconcile history safely first.
- Give the worker indexed discovery of committed publications or durably record the exact manifest IDs before
  retirement. A process-local list of IDs is lost on restart; a saved ID is actionable only after publication.

Before staging the first manifest, route all frontend and direct named DELETE callers away from legacy DELETE for the
enabled namespace, or reject those requests. Also exclude its account or container from `NamedBlobsCleanupRunner` on
every runner, for both stale cleanup and purge. Drain in-flight requests, cleaner batches already selected, and their
router deletes. Either path can send a root to the router, which can eagerly delete shared children. If the exclusion
cannot be guaranteed (for example, the runner cannot resolve the account), keep the cleaner stopped.

On rollback, disable staging and publication but retain fence-aware reads, the DELETE gate, cleaner exclusion, database
fence, index, and published evidence. Do not send affected traffic or cleaner work to an old binary that cannot honor
those gates.

This store does not retire raw parent IDs, establish exclusive ownership of chunks, schedule cleanup, or reclaim space.
Those steps and an ownership policy are required before wiring the public DELETE path.
