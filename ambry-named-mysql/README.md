# Named delete manifests

Each manifest stores the ordered child IDs of one immutable metadata blob in a single MySQL row. Its primary key is
`(account_id, container_id, blob_name, version)`, the same key that identifies the named version. The root ID identifies
the source of the list; repeated child IDs retain their positions. Publication and retirement commit together. If the
commit outcome is unknown, retry the same exact versions, roots and lists. Do not resolve the latest version of the
name or fall back to the router's ordinary delete.

A reader with a saved tuple can recover the published list after a restart without fetching the original root.
Configure the reader data source for `READ COMMITTED` or stronger on every checkout.
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
retrying unchanged input will not help. A publication supports at most 262,144 children across its versions (1 TiB at
4 MiB per child). Each descriptor has a one-byte length and at most 127 ID bytes: the largest encoded list is 32 MiB,
which requires `LONGBLOB` rather than `MEDIUMBLOB`. Before enabling publication, round-trip a maximum-size binary
payload through the deployed JDBC connection and check `@@session.max_allowed_packet` on writers and failover targets.
Connector/J 8.0.21 with client-side prepared statements rejected a synthetic 32 MiB body at a 64 MiB packet limit:
the measured query exceeded 64 MiB before transmission. The same bytes round-tripped with server-side prepared
statements at 64 MiB and with either mode at 128 MiB on local MySQL 8.0.46. These are tested configurations, not
production settings. Check the client limit as well as MySQL and replication limits on each deployment.

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
mysql --defaults-file="$DEFINER_CNF" --database="$DB" --init-command='SET SESSION lock_wait_timeout=5' \
  --delimiter='//' -e 'SOURCE NamedBlobManifestTupleGuardInsertV1.sql'
mysql --defaults-file="$DEFINER_CNF" --database="$DB" --init-command='SET SESSION lock_wait_timeout=5' \
  --delimiter='//' -e 'SOURCE NamedBlobManifestTupleGuardUpdateV1.sql'
mysql --defaults-file="$DEFINER_CNF" --database="$DB" -e 'SHOW CREATE TRIGGER named_blob_retirement_fence_v1\G'
```

The delimiter keeps each compound trigger intact in the mysql client; JDBC installers send each trigger file as
one statement. Verify all three trigger bodies and definers. The tuple guards need locking-read privileges on
`named_blob_manifests_v1`; rehearse using the deployed MySQL version and roles. Every insert checks the manifest
primary key, even when no caller is enabled. Rehearse normal PUT concurrency and watch lock waits before rollout.
On each writer, connect **as the application account** and smoke-test an unfenced update: in one transaction, insert
an approved disposable named version, finalize its TTL using its exact primary key, verify one row changed to READY
with `deleted_ts IS NULL`, and roll back. Verify the fixture row is absent afterward. Do not use a customer row. Stop
on any error; a successful install by the DDL operator is not evidence that application writes still work.

**On any error or lost connection, do not rerun the file.** Each successful DDL statement has already committed.
Wait for any in-flight DDL to finish. Inspect `SHOW CREATE TABLE` for the manifest table and `named_blobs_v2`, and
`SHOW CREATE TRIGGER` if the trigger exists. Compare definitions in installation order: manifest table and composite
primary key, fence column, ordered unfenced-version index, then trigger and definer. Stop on any conflicting
definition. Fix the cause, resume at the first missing statement using the timeout above, and run only the missing
steps in order. An index build can finish scanning the table and still time out waiting for its final metadata lock.
If the column exists but the index does not, have the owners finish any long-running transactions holding the lock
before retrying `ALTER TABLE ... ADD INDEX`; each retry rebuilds the index. Then install the trigger, recheck the
definitions, and run the application smoke test.

## Before enabling a caller

The database operations alone are not safe to activate. In addition to the DELETE and cleaner gates below:

- Make ordinary GET and all LIST paths honor `retirement_fenced` on the **selected version**, irrespective of the
  frontend clock. Do not discard the newest tombstone before selecting a version: that can expose older data. Preserve
  include-deleted GET access to the exact retired root.
- Before permitting failover, verify each promotion candidate has applied all acknowledged retirement transactions
  (manifest rows and fenced named versions), not just the schema and trigger. Do not promote or serve from a candidate whose
  applied position cannot be proven sufficient.
- Admit only namespaces that will stay within the version limit **with cleanup excluded**. PUT keeps older versions:
  an initial permanent PUT and 32 overwrites can leave 33 unfenced rows. Even below the limit, a live non-composite
  version cannot supply a composite manifest. Restrict writes accordingly or reconcile history safely first.
- Give the worker indexed discovery of committed publications or durably record the exact tuples before retirement.
  A process-local list is lost on restart; a saved tuple is actionable only after publication.

Before staging the first manifest, route all frontend and direct named DELETE callers away from legacy DELETE for the
enabled namespace, or reject those requests. Also exclude its account or container from `NamedBlobsCleanupRunner` on
every runner, for both stale cleanup and purge. Drain in-flight requests, cleaner batches already selected, and their
router deletes. Either path can send a root to the router, which can eagerly delete shared children. If the exclusion
cannot be guaranteed (for example, the runner cannot resolve the account), keep the cleaner stopped.

On rollback, disable staging and publication but retain the DELETE gate, cleaner exclusion, database fence, index, and
published evidence. Do not send affected traffic or cleaner work to an old binary that cannot honor those gates.

This store does not retire raw parent IDs, establish exclusive ownership of chunks, schedule cleanup, or reclaim space.
Those steps and an ownership policy are required before wiring the public DELETE path.
