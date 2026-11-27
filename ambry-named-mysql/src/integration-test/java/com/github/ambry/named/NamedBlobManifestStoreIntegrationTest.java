/*
 * Copyright 2026 LinkedIn Corp. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.github.ambry.named;

import static org.junit.Assert.*;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.*;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLDataException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.junit.Test;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.Account;
import com.github.ambry.account.AccountBuilder;
import com.github.ambry.account.Container;
import com.github.ambry.account.ContainerBuilder;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.MySqlNamedBlobDbConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.NamedBlobState;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.mysql.cj.jdbc.MysqlDataSource;

public class NamedBlobManifestStoreIntegrationTest {
  private static final short ACCOUNT = 17;
  private static final short CONTAINER = 4;
  private static final String NAME = "folder/MixedCase";
  // The declared per-manifest and aggregate-publish child cap; exercised at full scale below.
  private static final int MAX_CHILDREN = 262_144;

  @Test
  public void testDetachedRecoveryAcrossReplacement() throws Exception {
    withDatabase(NamedBlobManifestStoreIntegrationTest::exercise);
  }

  private interface Scenario {
    void run(DataSource source, MySqlNamedBlobDb named) throws Exception;
  }

  private static void withDatabase(Scenario scenario) throws Exception {
    withDatabase(null, scenario);
  }

  private interface BeforeMigration {
    void seed(DataSource source) throws Exception;
  }

  private static void withDatabase(BeforeMigration before, Scenario scenario) throws Exception {
    String schema = "ambry_manifest_test_" + UUID.randomUUID().toString().replace("-", "");
    MysqlDataSource admin = source("");
    try (Connection c = admin.getConnection(); Statement s = c.createStatement()) {
      // No IF NOT EXISTS: never adopt, mutate, or drop an existing database.
      s.executeUpdate("CREATE DATABASE " + schema + " CHARACTER SET utf8mb4 COLLATE utf8mb4_bin");
      try {
        MysqlDataSource db = source(schema);
        try (Connection setup = db.getConnection(); Statement ddl = setup.createStatement()) {
          install(ddl, "/NamedBlobsSchema.ddl");
          if (before != null) {
            before.seed(db);
          }
          install(ddl, "/NamedBlobManifestV1.ddl");
          install(ddl, "/NamedBlobRetirementFenceV1.sql");
          install(ddl, "/NamedBlobManifestTupleGuardInsertV1.sql");
          install(ddl, "/NamedBlobManifestTupleGuardUpdateV1.sql");
        }
        try (MySqlNamedBlobDb named = namedDb(db)) {
          scenario.run(db, named);
        }
      } finally {
        s.executeUpdate("DROP DATABASE " + schema);
      }
    }
  }

  private static MySqlNamedBlobDb namedDb(DataSource db) throws Exception {
    InMemAccountService accounts = new InMemAccountService(false, false);
    accounts.updateAccounts(Collections.singletonList(new AccountBuilder(ACCOUNT, "account", Account.AccountStatus.ACTIVE)
        .addOrUpdateContainer(new ContainerBuilder(CONTAINER, "container", Container.ContainerStatus.ACTIVE, "",
            ACCOUNT).build()).build()));
    Properties properties = new Properties();
    properties.setProperty(MySqlNamedBlobDbConfig.DB_INFO,
        "[{\"url\":\"jdbc:mysql://[::1]\",\"datacenter\":\"local\",\"isWriteable\":true,"
            + "\"username\":\"unused\",\"password\":\"unused\",\"sslMode\":\"NONE\"}]");
    return new MySqlNamedBlobDb(accounts, new MySqlNamedBlobDbConfig(new VerifiableProperties(properties)),
        endpoint -> db, "local", new Metrics(new MetricRegistry(), ""));
  }

  /** Stage descriptors for one named version; the sealed row is keyed only by the (account, container, name,
   *  version) tuple, so nothing is returned to identify it afterward. */
  private static void stage(MySqlNamedBlobDb db, String name, long version, BlobId root, int count,
      Iterator<BlobId> children) throws Exception {
    try {
      db.stageDeleteManifest("account", "container", name, version, root, count, children).get(10, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof SQLDataException) {
        throw (SQLDataException) e.getCause();
      }
      throw e;
    }
  }

  private static void exercise(DataSource db, MySqlNamedBlobDb named) throws Exception {
    BlobId old = blob(BlobId.BlobDataType.METADATA);
    BlobId replacement = blob(BlobId.BlobDataType.METADATA);
    List<BlobId> children = new ArrayList<>();
    for (int i = 0; i < 65; i++) {
      children.add(blob(BlobId.BlobDataType.DATACHUNK));
    }
    List<String> ids = children.stream().map(BlobId::getID).collect(Collectors.toList());
    insertNamed(db, 7, old, NamedBlobState.READY);
    insertNamed(db, 8, replacement, NamedBlobState.IN_PROGRESS);
    NamedBlobManifestStore writer = new NamedBlobManifestStore(db);

    expectDataError("No matching live READY", () ->
        stage(named, NAME, 8, replacement, children.size(), children.iterator()));
    expectDataError("No matching live READY", () ->
        stage(named, NAME + " ", 7, old, children.size(), children.iterator()));
    expectDataError("No matching live READY", () ->
        stage(named, NAME, 7, replacement, children.size(), children.iterator()));
    expectDataError("Missing or unsupported child", () ->
        stage(named, NAME, 7, old, children.size(), children.subList(0, children.size() - 1).iterator()));
    assertEquals(0, scalar(db, "SELECT COUNT(*) FROM named_blob_manifests_v1"));

    stage(named, NAME, 7, old, children.size(), children.iterator());
    // A retry against the already-sealed row is idempotent: it must not re-seal or fail.
    stage(named, NAME, 7, old, children.size(), children.iterator());
    List<BlobId> reordered = new ArrayList<>(children);
    Collections.swap(reordered, 0, 1);
    expectDataError("Capture retry disagrees", () ->
        stage(named, NAME, 7, old, reordered.size(), reordered.iterator()));

    assertNull("Staging alone does not publish work", writer.getPublished(ACCOUNT, CONTAINER, NAME, 7));
    expectDataError("Unrepresented or ineligible", () -> retire(named, Collections.singletonList(7L)));
    sql(db, "DELETE FROM named_blobs_v2 WHERE version=8");
    retire(named, Collections.singletonList(7L));
    insertNamed(db, 8, replacement, NamedBlobState.READY);
    retire(named, Collections.singletonList(7L));
    assertEquals(replacement.getID(), named.get("account", "container", NAME).get(10, TimeUnit.SECONDS).getBlobId());
    // Remove the retired fixture row entirely: a worker that resolves the source name cannot pass.
    try (Connection c = db.getConnection(); PreparedStatement remove = c.prepareStatement(
        "DELETE FROM named_blobs_v2 WHERE account_id=? AND container_id=? AND blob_name=? AND version=7 AND blob_id=?")) {
      remove.setShort(1, ACCOUNT);
      remove.setShort(2, CONTAINER);
      remove.setString(3, NAME);
      remove.setBytes(4, old.toBytes());
      assertEquals(1, remove.executeUpdate());
    }

    DataSource readers = mock(DataSource.class);
    when(readers.getConnection()).thenAnswer(invocation -> {
      Connection c = mock(Connection.class, delegatesTo(db.getConnection()));
      doThrow(new AssertionError("Reader changed pool isolation")).when(c).setTransactionIsolation(anyInt());
      return c;
    });
    NamedBlobManifestStore worker = new NamedBlobManifestStore(readers);
    NamedBlobManifestStore.Manifest recovered = worker.getPublished(ACCOUNT, CONTAINER, NAME, 7);
    assertEquals(old.getID(), recovered.root);
    assertEquals(7L, recovered.version);
    // The worker recovered the exact tuple without resolving the name or reading the original root.
    assertEquals(ids, worker.read(recovered));

    try (Connection dirty = db.getConnection()) {
      dirty.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
      doReturn(dirty).when(readers).getConnection();
      try {
        worker.read(recovered);
        fail("A valid header checkout must not authorize a dirty payload checkout");
      } catch (IllegalStateException expected) {
        assertTrue(expected.getMessage(), expected.getMessage().contains("READ COMMITTED"));
      }
      assertTrue(dirty.isClosed());
    }

    NamedBlobManifestStore restarted = new NamedBlobManifestStore(db);
    NamedBlobManifestStore.Manifest again = restarted.getPublished(ACCOUNT, CONTAINER, NAME, 7);
    assertEquals(ids, restarted.read(again));
    assertNull(restarted.getPublished(ACCOUNT, CONTAINER, NAME, 999));
    assertNull(restarted.getPublished(ACCOUNT, CONTAINER, NAME + " ", 7));

    // A second root can legitimately reference the same child. Both rows remain evidence, each keyed by its
    // own tuple: a worker that saved (name, version=8) trusts that row directly, independent of the root at
    // version 7 or any other version.
    stage(named, NAME, 8, replacement, 1, Collections.singletonList(children.get(0)).iterator());
    retire(named, Collections.singletonList(8L));
    NamedBlobManifestStore.Manifest second = restarted.getPublished(ACCOUNT, CONTAINER, NAME, 8);
    assertEquals(replacement.getID(), second.root);
    assertEquals(Collections.singletonList(ids.get(0)), restarted.read(second));
    assertEquals(2, scalar(db, "SELECT COUNT(*) FROM named_blob_manifests_v1 WHERE publication_key IS NOT NULL"));

    try (Connection c = db.getConnection(); PreparedStatement s = c.prepareStatement(
        "UPDATE named_blob_manifests_v1 SET payload=X'00' WHERE account_id=? AND container_id=? AND blob_name=?"
            + " AND version=?")) {
      s.setShort(1, ACCOUNT);
      s.setShort(2, CONTAINER);
      s.setString(3, NAME);
      s.setLong(4, 8);
      assertEquals(1, s.executeUpdate());
    }
    expectDataError("Manifest checksum mismatch", () -> restarted.read(second));
  }

  @Test
  public void testDuplicateCaptureReadsBeyondItsSnapshot() throws Exception {
    withDatabase((db, named) -> {
      BlobId root = blob(BlobId.BlobDataType.METADATA);
      List<BlobId> children = Collections.singletonList(blob(BlobId.BlobDataType.DATACHUNK));
      insertNamed(db, 1, root, NamedBlobState.READY);
      try (Connection loser = db.getConnection(); Statement snapshot = loser.createStatement()) {
        loser.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        loser.setAutoCommit(false);
        try (ResultSet rows = snapshot.executeQuery("SELECT COUNT(*) FROM named_blob_manifests_v1")) {
          assertTrue(rows.next());
          assertEquals(0, rows.getInt(1));
        }
        stage(named, NAME, 1, root, 1, children.iterator());
        // A racer whose snapshot predates the winner's commit must still see the sealed row when it checks out
        // the row FOR UPDATE: that read is not snapshot-isolated, so the retry is idempotent rather than blind.
        NamedBlobManifestStore.capture(loser, ACCOUNT, CONTAINER, NAME, 1, root, 1, children.iterator());
        expectDataError("Capture retry disagrees", () -> NamedBlobManifestStore.capture(loser, ACCOUNT, CONTAINER,
            NAME, 1, root, 1, Collections.singletonList(blob(BlobId.BlobDataType.DATACHUNK)).iterator()));
        loser.rollback();
      }
    });
  }

  @Test
  public void testMigrationPreservesLegacyTtlUntilRetirement() throws Exception {
    BlobId root = blob(BlobId.BlobDataType.METADATA);
    withDatabase(db -> {
      insertNamed(db, 1, blob(BlobId.BlobDataType.METADATA), NamedBlobState.READY);
      insertNamed(db, 2, blob(BlobId.BlobDataType.METADATA), NamedBlobState.IN_PROGRESS);
      insertNamed(db, 3, root, NamedBlobState.IN_PROGRESS);
      sql(db, "UPDATE named_blobs_v2 SET deleted_ts='2000-01-01'");
    }, (db, named) -> {
      NamedBlobRecord newest = NamedBlobRecord.forUpdate("account", "container", NAME, 3);
      named.updateBlobTtlAndStateToReady(newest).get(10, TimeUnit.SECONDS);
      assertEquals(root.getID(), named.get("account", "container", NAME).get(10, TimeUnit.SECONDS).getBlobId());
      stage(named, NAME, 3, root, 1, Collections.singletonList(blob(BlobId.BlobDataType.DATACHUNK)).iterator());
      retire(named, Collections.singletonList(3L));
      try {
        named.get("account", "container", NAME).get(10, TimeUnit.SECONDS);
        fail("Ordinary GET must report Deleted after retirement");
      } catch (ExecutionException e) {
        assertTrue(e.getCause() instanceof RestServiceException);
        assertEquals(RestServiceErrorCode.Deleted, ((RestServiceException) e.getCause()).getErrorCode());
      }
      NamedBlobRecord deleted = named.get("account", "container", NAME, GetOption.Include_Deleted_Blobs, false)
          .get(10, TimeUnit.SECONDS);
      assertEquals(root.getID(), deleted.getBlobId());
      assertEquals(3L, deleted.getVersion());
      assertEquals(2, scalar(db, "SELECT COUNT(*) FROM named_blobs_v2 WHERE version<3 AND retirement_fenced=TRUE"
          + " AND deleted_ts='2000-01-01'"));
      assertEquals(1, scalar(db, "SELECT COUNT(*) FROM named_blob_manifests_v1 WHERE publication_key IS NOT NULL"));
      try {
        named.updateBlobTtlAndStateToReady(NamedBlobRecord.forUpdate("account", "container", NAME, 2))
            .get(10, TimeUnit.SECONDS);
        fail("TTL must not revive fenced history");
      } catch (ExecutionException e) {
        assertTrue(e.getCause() instanceof SQLException);
        assertEquals("45000", ((SQLException) e.getCause()).getSQLState());
      }
    });
  }

  @Test
  public void testNameReuseDoesNotConsumeRetirementLimit() throws Exception {
    withDatabase((db, named) -> {
      for (int version = 1; version <= 33; version++) {
        BlobId root = blob(BlobId.BlobDataType.METADATA);
        insertNamed(db, version, root, NamedBlobState.READY);
        stage(named, NAME, version, root, 1, Collections.singletonList(blob(BlobId.BlobDataType.DATACHUNK))
            .iterator());
        retire(named, Collections.singletonList((long) version));
      }
      assertEquals(33, scalar(db, "SELECT COUNT(*) FROM named_blobs_v2 WHERE retirement_fenced=TRUE"));
    });
  }

  @Test
  public void testRetirementLocksGapForReplacement() throws Exception {
    withDatabase((db, named) -> {
      BlobId root = blob(BlobId.BlobDataType.METADATA);
      insertNamed(db, 7, root, NamedBlobState.READY);
      stage(named, NAME, 7, root, 1, Collections.singletonList(blob(BlobId.BlobDataType.DATACHUNK)).iterator());
      List<Long> jobs = Collections.singletonList(7L);
      try (Connection publisher = db.getConnection(); Connection replacement = db.getConnection();
          Statement insert = replacement.createStatement()) {
        publisher.setAutoCommit(false);
        NamedBlobManifestStore.retire(publisher, ACCOUNT, CONTAINER, NAME, jobs);
        insert.execute("SET SESSION innodb_lock_wait_timeout=1");
        String put = "INSERT INTO named_blobs_v2 (account_id,container_id,blob_name,version,blob_id,blob_state)"
            + " VALUES (17,4,'folder/MixedCase',8,X'AB',1)";
        try {
          insert.executeUpdate(put);
          fail("A new version must wait for retirement");
        } catch (SQLException e) {
          assertEquals("InnoDB lock wait timeout", 1205, e.getErrorCode());
        }
        publisher.commit();
        assertEquals(1, insert.executeUpdate(put));
      }
      retire(named, jobs);
      assertEquals(1, scalar(db, "SELECT COUNT(*) FROM named_blobs_v2 WHERE version=8 AND retirement_fenced=FALSE"
          + " AND deleted_ts IS NULL"));
    });
  }

  @Test
  public void testPublicationRollsBackWhenNameMutationFails() throws Exception {
    withDatabase((db, named) -> {
      List<Long> jobs = new ArrayList<>();
      for (int version = 1; version <= 2; version++) {
        BlobId root = blob(BlobId.BlobDataType.METADATA);
        insertNamed(db, version, root, NamedBlobState.READY);
        stage(named, NAME, version, root, 1, Collections.singletonList(blob(BlobId.BlobDataType.DATACHUNK))
            .iterator());
        jobs.add((long) version);
      }
      // Publication processes the canonical (ascending-by-version) order; make the trigger fail on the last
      // version in that order so the test still proves the earlier publish within the same transaction rolls back.
      long lastVersion = Collections.max(jobs);
      sql(db, "CREATE TRIGGER reject_retirement AFTER UPDATE ON named_blobs_v2 FOR EACH ROW BEGIN"
          + " IF NEW.version=" + lastVersion
          + " THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='fixture rejection'; END IF; END");
      try {
        retire(named, jobs);
        fail("Expected mutation to fail");
      } catch (SQLException expected) {
        assertEquals("45000", expected.getSQLState());
        assertTrue(expected.getMessage(), expected.getMessage().contains("fixture rejection"));
      }
      assertEquals(0, scalar(db, "SELECT COUNT(*) FROM named_blob_manifests_v1 WHERE publication_key IS NOT NULL"));
      assertEquals(2, scalar(db, "SELECT COUNT(*) FROM named_blobs_v2 WHERE deleted_ts IS NULL"
          + " AND retirement_fenced=FALSE"));
      sql(db, "DROP TRIGGER reject_retirement");
      retire(named, jobs);
      jobs.sort(Collections.reverseOrder());
      try (Connection replay = db.getConnection()) {
        replay.setAutoCommit(false);
        NamedBlobManifestStore.retire(replay, ACCOUNT, CONTAINER, NAME, jobs);
        try {
          NamedBlobManifestStore.retire(replay, ACCOUNT, CONTAINER, NAME, Collections.emptyList());
          fail("An empty set is not a publication receipt");
        } catch (IllegalArgumentException expected) {
          assertEquals("Invalid number of manifests", expected.getMessage());
        }
        replay.commit();
      }
      expectDataError("Manifest belongs to a different publication", () -> retire(named, jobs.subList(0, 1)));
    });
  }

  @Test
  public void testWaitingTtlCannotReviveRetiredVersion() throws Exception {
    withDatabase((db, named) -> {
      BlobId root = blob(BlobId.BlobDataType.METADATA);
      insertNamed(db, 7, root, NamedBlobState.READY);
      stage(named, NAME, 7, root, 1, Collections.singletonList(blob(BlobId.BlobDataType.DATACHUNK)).iterator());
      List<Long> jobs = Collections.singletonList(7L);
      String ttlSql = "UPDATE named_blobs_v2 SET blob_state=1, deleted_ts=NULL WHERE account_id=17"
          + " AND container_id=4 AND blob_name='folder/MixedCase' AND version=";
      ExecutorService executor = Executors.newSingleThreadExecutor();
      try (Connection publisher = db.getConnection(); Connection ttl = db.getConnection();
          Statement lock = publisher.createStatement(); Statement update = ttl.createStatement()) {
        publisher.setAutoCommit(false);
        try (ResultSet rows = lock.executeQuery("SELECT version FROM named_blobs_v2 WHERE account_id=17"
            + " AND container_id=4 AND blob_name='folder/MixedCase' FOR UPDATE")) {
          assertTrue(rows.next());
        }
        long connectionId;
        try (ResultSet self = update.executeQuery("SELECT CONNECTION_ID()")) {
          assertTrue(self.next());
          connectionId = self.getLong(1);
        }
        Future<String> blocked = executor.submit(() -> {
          try {
            update.executeUpdate(ttlSql + "7");
            return "updated";
          } catch (SQLException failure) {
            return failure.getSQLState();
          }
        });
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (scalar(db, "SELECT COUNT(*) FROM information_schema.processlist WHERE id=" + connectionId
            + " AND info LIKE 'UPDATE named_blobs_v2 SET blob_state=%'") == 0) {
          assertTrue("TTL statement never began", System.nanoTime() < deadline && !blocked.isDone());
        }
        NamedBlobManifestStore.retire(publisher, ACCOUNT, CONTAINER, NAME, jobs);
        publisher.commit();
        assertEquals("45000", blocked.get(10, TimeUnit.SECONDS));
        insertNamed(db, 8, blob(BlobId.BlobDataType.METADATA), NamedBlobState.IN_PROGRESS);
        sql(db, "UPDATE named_blobs_v2 SET deleted_ts='2000-01-01' WHERE version=8");
        assertEquals(1, update.executeUpdate(ttlSql + "8"));
        assertEquals(1, scalar(db, "SELECT COUNT(*) FROM named_blobs_v2 WHERE version=7 AND retirement_fenced=TRUE"
            + " AND deleted_ts IS NOT NULL"));
      } finally {
        executor.shutdownNow();
      }
    });
  }

  /**
   * The concretely reported gap: capture/retire only re-validate root and count when {@code capture} itself is
   * called again. Neither fences a direct SQL write to named_blobs_v2, so once a cleanup job physically removes
   * the retired row, nothing used to stop a fresh INSERT from rebinding the same tuple to a different root while
   * the published manifest still names the original. Also confirms what must still be permitted: recreating the
   * identical row, and writing an entirely different version, are both untouched by the guard.
   */
  @Test
  public void testDirectReinsertAfterPhysicalRemovalRejectsDifferentRoot() throws Exception {
    withDatabase((db, named) -> {
      BlobId root = blob(BlobId.BlobDataType.METADATA);
      BlobId impostor = blob(BlobId.BlobDataType.METADATA);
      insertNamed(db, 7, root, NamedBlobState.READY);
      stage(named, NAME, 7, root, 1, Collections.singletonList(blob(BlobId.BlobDataType.DATACHUNK)).iterator());
      retire(named, Collections.singletonList(7L));
      sql(db, "DELETE FROM named_blobs_v2 WHERE version=7");

      expectSqlRejection("Named tuple already has a manifest for a different root",
          () -> insertNamed(db, 7, impostor, NamedBlobState.READY));

      // Recreating the exact same row the manifest already names is the documented recovery path and must
      // still work after the guard rejects an impostor for the same tuple.
      insertNamed(db, 7, root, NamedBlobState.READY);
      assertEquals(1, scalar(db, "SELECT COUNT(*) FROM named_blobs_v2 WHERE version=7 AND blob_id=X'"
          + bytesToHex(root.toBytes()) + "'"));
      // A different version was never part of this manifest's key and must be entirely unaffected.
      insertNamed(db, 9, impostor, NamedBlobState.READY);
      assertEquals(1, scalar(db, "SELECT COUNT(*) FROM named_blobs_v2 WHERE version=9 AND blob_id=X'"
          + bytesToHex(impostor.toBytes()) + "'"));
    });
  }

  /**
   * Same prohibition, reached through UPDATE instead of INSERT: a direct rebind of blob_id while the row is still
   * live (not yet purged, not even retired) must be rejected exactly like the post-purge INSERT case, while an
   * ordinary state-only update (the TTL path) is untouched.
   */
  @Test
  public void testDirectUpdateRejectsDifferentRootWhileManifestExists() throws Exception {
    withDatabase((db, named) -> {
      BlobId root = blob(BlobId.BlobDataType.METADATA);
      BlobId impostor = blob(BlobId.BlobDataType.METADATA);
      insertNamed(db, 7, root, NamedBlobState.READY);
      stage(named, NAME, 7, root, 1, Collections.singletonList(blob(BlobId.BlobDataType.DATACHUNK)).iterator());

      expectSqlRejection("Named tuple already has a manifest for a different root", () -> {
        try (Connection c = db.getConnection(); PreparedStatement s = c.prepareStatement(
            "UPDATE named_blobs_v2 SET blob_id=? WHERE account_id=? AND container_id=? AND blob_name=? AND version=7")) {
          s.setBytes(1, impostor.toBytes());
          s.setShort(2, ACCOUNT);
          s.setShort(3, CONTAINER);
          s.setString(4, NAME);
          s.executeUpdate();
        }
      });

      // An ordinary state-only update (no blob_id change) never consults the manifest table and must still work.
      sql(db, "UPDATE named_blobs_v2 SET blob_state=" + NamedBlobState.READY.ordinal() + " WHERE version=7");
      retire(named, Collections.singletonList(7L));
      assertEquals(1, scalar(db, "SELECT COUNT(*) FROM named_blobs_v2 WHERE version=7 AND retirement_fenced=TRUE"));
    });
  }

  /**
   * The actual race the guard exists for: a capture that has reserved (but not yet committed) a manifest row must
   * block a concurrent direct replace for the same tuple, not merely a replace that starts after capture commits.
   */
  @Test
  public void testConcurrentInFlightCaptureBlocksRacingDirectReplace() throws Exception {
    withDatabase((db, named) -> {
      BlobId root = blob(BlobId.BlobDataType.METADATA);
      BlobId impostor = blob(BlobId.BlobDataType.METADATA);
      insertNamed(db, 7, root, NamedBlobState.READY);
      try (Connection capturing = db.getConnection(); Connection replacing = db.getConnection();
          Statement replace = replacing.createStatement()) {
        capturing.setAutoCommit(false);
        // Reserve the manifest row without committing: this is the in-flight window the guard must defend.
        NamedBlobManifestStore.capture(capturing, ACCOUNT, CONTAINER, NAME, 7, root, 1,
            Collections.singletonList(blob(BlobId.BlobDataType.DATACHUNK)).iterator());

        replace.execute("SET SESSION innodb_lock_wait_timeout=1");
        try {
          replace.executeUpdate("DELETE FROM named_blobs_v2 WHERE version=7");
          fail("Purge must wait for an in-flight capture");
        } catch (SQLException blocked) {
          assertEquals(1205, blocked.getErrorCode());
        }
        capturing.commit();
        replace.executeUpdate("DELETE FROM named_blobs_v2 WHERE version=7");
        try {
          insertNamed(db, 7, impostor, NamedBlobState.READY);
          fail("A committed reservation must reject replacement");
        } catch (SQLException conflict) {
          assertEquals("45000", conflict.getSQLState());
        }
      }
      // After capture's committed root loses the race to no one, the same tuple can still be recreated with the
      // root the manifest actually names.
      sql(db, "DELETE FROM named_blobs_v2 WHERE version=7");
      insertNamed(db, 7, root, NamedBlobState.READY);
      retire(named, Collections.singletonList(7L));
      assertEquals(1, scalar(db, "SELECT COUNT(*) FROM named_blobs_v2 WHERE version=7 AND retirement_fenced=TRUE"));
    });
  }

  private static String bytesToHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder(bytes.length * 2);
    for (byte b : bytes) {
      sb.append(String.format("%02X", b));
    }
    return sb.toString();
  }

  private static void expectSqlRejection(String reason, SqlAction action) throws Exception {
    try {
      action.run();
      fail("Expected rejection: " + reason);
    } catch (SQLException expected) {
      assertEquals("45000", expected.getSQLState());
      assertTrue(expected.getMessage(), expected.getMessage().contains(reason));
    }
  }

  /**
   * Captures, publishes and reads back the declared cap of real child descriptors in a single manifest row, and
   * asserts the full ordered list survives exactly, under the same JDBC settings as every other case in this suite
   * (no streaming fetch size, no paging). Check the stored byte length as well as the decoded IDs.
   */
  @Test
  public void testFullScaleManifestRoundTripAtTheDeclaredChildCap() throws Exception {
    withDatabase((db, named) -> {
      BlobId root = blob(BlobId.BlobDataType.METADATA);
      insertNamed(db, 1, root, NamedBlobState.READY);
      List<BlobId> children = new ArrayList<>(MAX_CHILDREN);
      for (int i = 0; i < MAX_CHILDREN; i++) {
        children.add(blob(BlobId.BlobDataType.DATACHUNK));
      }
      List<String> ids = children.stream().map(BlobId::getID).collect(Collectors.toList());

      stage(named, NAME, 1, root, children.size(), children.iterator());
      retire(named, Collections.singletonList(1L));

      NamedBlobManifestStore worker = new NamedBlobManifestStore(db);
      NamedBlobManifestStore.Manifest manifest = worker.getPublished(ACCOUNT, CONTAINER, NAME, 1);
      assertEquals(MAX_CHILDREN, manifest.count);

      List<String> recovered = worker.read(manifest);

      assertEquals(ids, recovered);
      assertEquals(children.stream().mapToInt(child -> 1 + child.sizeInBytes()).sum(),
          scalar(db, "SELECT OCTET_LENGTH(payload) FROM named_blob_manifests_v1"));
      assertEquals(1, scalar(db, "SELECT COUNT(*) FROM named_blob_manifests_v1 WHERE publication_key IS NOT NULL"));
    });
  }

  /** One more than the declared cap must be rejected immediately, before any descriptor is read or any row written. */
  @Test
  public void testCaptureRejectsOneMoreThanTheDeclaredChildCap() throws Exception {
    withDatabase((db, named) -> {
      BlobId root = blob(BlobId.BlobDataType.METADATA);
      insertNamed(db, 1, root, NamedBlobState.READY);
      expectDataError("Unsupported named composite capture", () ->
          stage(named, NAME, 1, root, MAX_CHILDREN + 1, Collections.<BlobId>emptyIterator()));
      assertEquals(0, scalar(db, "SELECT COUNT(*) FROM named_blob_manifests_v1"));
    });
  }

  private static void retire(MySqlNamedBlobDb db, List<Long> versions) throws Exception {
    try {
      db.retireWithManifests("account", "container", NAME, versions).get(10, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof SQLException) {
        throw (SQLException) e.getCause();
      }
      throw e;
    }
  }

  private static int scalar(DataSource db, String sql) throws SQLException {
    try (Connection c = db.getConnection(); Statement s = c.createStatement(); ResultSet r = s.executeQuery(sql)) {
      assertTrue(r.next());
      return r.getInt(1);
    }
  }

  private static void sql(DataSource db, String sql) throws SQLException {
    try (Connection c = db.getConnection(); Statement s = c.createStatement()) {
      s.execute(sql);
    }
  }

  private static MysqlDataSource source(String schema) throws SQLException {
    MysqlDataSource db = new MysqlDataSource();
    String port = System.getenv("AMBRY_MANIFEST_TEST_PORT");
    if (port == null) {
      // Same loopback MySQL fixture and user as the named-DB integration suite.
      db.setUser("travis");
      db.setPassword("");
    } else {
      db.setServerName("[::1]");
      db.setPort(Integer.parseInt(port));
      db.setUser("ambry_manifest_test");
      db.setPassword(System.getenv("AMBRY_MANIFEST_TEST_PASSWORD"));
    }
    db.setDatabaseName(schema);
    db.setServerTimezone("UTC");
    return db;
  }

  private static void install(Statement statement, String resource) throws SQLException {
    InputStream input = NamedBlobManifestStoreIntegrationTest.class.getResourceAsStream(resource);
    assertNotNull(resource, input);
    try (Scanner ddl = new Scanner(input, "UTF-8")) {
      ddl.useDelimiter(resource.endsWith(".sql") ? "\\A" : ";");
      while (ddl.hasNext()) {
        String sql = ddl.next().trim();
        if (!sql.isEmpty()) {
          statement.execute(sql);
        }
      }
    }
  }

  private static BlobId blob(BlobId.BlobDataType type) {
    return new BlobId(BlobId.BLOB_ID_V6, BlobId.BlobIdType.NATIVE, (byte) 0, ACCOUNT, CONTAINER,
        new MockPartitionId(), false, type);
  }

  private static void insertNamed(DataSource db, long version, BlobId root, NamedBlobState state) throws SQLException {
    try (Connection c = db.getConnection(); PreparedStatement s = c.prepareStatement(
        "INSERT INTO named_blobs_v2 (account_id,container_id,blob_name,version,blob_id,blob_state)"
            + " VALUES (?,?,?,?,?,?)")) {
      s.setShort(1, ACCOUNT);
      s.setShort(2, CONTAINER);
      s.setString(3, NAME);
      s.setLong(4, version);
      s.setBytes(5, root.toBytes());
      s.setInt(6, state.ordinal());
      assertEquals(1, s.executeUpdate());
    }
  }

  private interface SqlAction {
    void run() throws Exception;
  }

  private static void expectDataError(String reason, SqlAction action) throws Exception {
    try {
      action.run();
      fail("Expected rejection: " + reason);
    } catch (SQLDataException expected) {
      assertTrue(expected.getMessage(), expected.getMessage().startsWith(reason));
    }
  }
}
