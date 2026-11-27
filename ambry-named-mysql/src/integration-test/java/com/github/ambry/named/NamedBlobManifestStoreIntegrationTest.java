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
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
  // sizeInBytes() for a V1-V5 BlobId is VERSION(2)+FLAG(1)+DATACENTER(1)+ACCOUNT(2)+CONTAINER(2)
  // + MockPartitionId.getBytes().length(10) + length-prefix(4) + uuidStr.length(). 8+10+4=22 fixed bytes, so a
  // 105-character ASCII uuid reaches exactly the declared 127-byte cap through a genuine BlobId instance.
  private static final int MAX_ID_UUID_LENGTH = 105;

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

  /**
   * Validate, publish and fence one named version in one call; there is no separate staging step, so the
   * descriptors are supplied and consumed in the same transaction that retires the version.
   */
  private static void retireOne(MySqlNamedBlobDb db, long version, BlobId root, int count,
      Iterator<BlobId> children) throws Exception {
    retireOneNamed(db, NAME, version, root, count, children);
  }

  private static void retireOneNamed(MySqlNamedBlobDb db, String name, long version, BlobId root, int count,
      Iterator<BlobId> children) throws Exception {
    retireMany(db, name,
        Collections.singletonMap(version, new NamedBlobManifestStore.PendingVersion(root, count, children)));
  }

  private static void retireMany(MySqlNamedBlobDb db, Map<Long, NamedBlobManifestStore.PendingVersion> pending)
      throws Exception {
    retireMany(db, NAME, pending);
  }

  private static void retireMany(MySqlNamedBlobDb db, String name,
      Map<Long, NamedBlobManifestStore.PendingVersion> pending) throws Exception {
    try {
      db.retireWithManifests("account", "container", name, pending).get(10, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof SQLException) {
        throw (SQLException) e.getCause();
      }
      throw e;
    }
  }

  private static Map<Long, NamedBlobManifestStore.PendingVersion> pendingFor(List<Long> versions,
      Map<Long, BlobId> roots, Map<Long, List<BlobId>> childrenByVersion) {
    Map<Long, NamedBlobManifestStore.PendingVersion> pending = new LinkedHashMap<>();
    for (long version : versions) {
      List<BlobId> children = childrenByVersion.get(version);
      pending.put(version,
          new NamedBlobManifestStore.PendingVersion(roots.get(version), children.size(), children.iterator()));
    }
    return pending;
  }

  /** An iterator that must never be consumed: used to prove a rejection happens before any descriptor is read. */
  private static Iterator<BlobId> poisonIterator() {
    return new Iterator<BlobId>() {
      @Override
      public boolean hasNext() {
        throw new AssertionError("An oversized request must be rejected before any iterator is touched");
      }

      @Override
      public BlobId next() {
        throw new AssertionError("An oversized request must be rejected before any iterator is touched");
      }
    };
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

    // Retirement only ever covers a contiguous prefix of the unfenced range: an unrequested, still-live version
    // blocks retiring an older one, exactly as it did before capture and retire were merged.
    expectDataError("Unrepresented or ineligible",
        () -> retireOne(named, 7, old, children.size(), children.iterator()));
    assertEquals(0, scalar(db, "SELECT COUNT(*) FROM named_blob_manifests_v1"));

    sql(db, "DELETE FROM named_blobs_v2 WHERE version=8");

    expectDataError("Unrepresented or ineligible",
        () -> retireOne(named, 8, replacement, children.size(), children.iterator()));
    expectDataError("Unsupported named version range",
        () -> retireOneNamed(named, NAME + " ", 7, old, children.size(), children.iterator()));
    expectDataError("Unrepresented or ineligible",
        () -> retireOne(named, 7, replacement, children.size(), children.iterator()));
    expectDataError("Missing or unsupported child", () -> retireOne(named, 7, old, children.size(),
        children.subList(0, children.size() - 1).iterator()));
    assertEquals(0, scalar(db, "SELECT COUNT(*) FROM named_blob_manifests_v1"));

    retireOne(named, 7, old, children.size(), children.iterator());
    // A retry with the same exact tuple, root and content is idempotent: it must not fail or re-publish.
    retireOne(named, 7, old, children.size(), children.iterator());
    List<BlobId> reordered = new ArrayList<>(children);
    Collections.swap(reordered, 0, 1);
    expectDataError("Conflicting manifest for named version",
        () -> retireOne(named, 7, old, reordered.size(), reordered.iterator()));

    try {
      named.get("account", "container", NAME).get(10, TimeUnit.SECONDS);
      fail("Ordinary GET must report Deleted after retirement");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() instanceof RestServiceException);
      assertEquals(RestServiceErrorCode.Deleted, ((RestServiceException) e.getCause()).getErrorCode());
    }
    insertNamed(db, 8, replacement, NamedBlobState.READY);
    retireOne(named, 7, old, children.size(), children.iterator());
    // isPublished resolves a lost commit reply from the exact content-bound intent, not merely the version set.
    assertTrue(writer.isPublished(NamedBlobManifestStore.prepare(ACCOUNT, CONTAINER, NAME,
        Collections.singletonMap(7L, new NamedBlobManifestStore.PendingVersion(old, children.size(),
            children.iterator())))));
    Map<Long, NamedBlobManifestStore.PendingVersion> unpublishedPair = new LinkedHashMap<>();
    unpublishedPair.put(7L, new NamedBlobManifestStore.PendingVersion(old, children.size(), children.iterator()));
    unpublishedPair.put(8L, new NamedBlobManifestStore.PendingVersion(replacement, 1,
        Collections.singletonList(children.get(0)).iterator()));
    assertFalse(writer.isPublished(NamedBlobManifestStore.prepare(ACCOUNT, CONTAINER, NAME, unpublishedPair)));
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
    retireOne(named, 8, replacement, 1, Collections.singletonList(children.get(0)).iterator());
    NamedBlobManifestStore.Manifest second = restarted.getPublished(ACCOUNT, CONTAINER, NAME, 8);
    assertEquals(replacement.getID(), second.root);
    assertEquals(Collections.singletonList(ids.get(0)), restarted.read(second));
    assertEquals(2, scalar(db, "SELECT COUNT(*) FROM named_blob_manifests_v1"));

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
  public void testStaleSnapshotRetireReplayObservesCommittedPublication() throws Exception {
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
        retireOne(named, 1, root, 1, children.iterator());
        // A racer whose snapshot predates the winner's commit must still observe the published row when its own
        // locking read runs: that read is not snapshot-isolated, so the replay is idempotent rather than blind.
        NamedBlobManifestStore.retire(loser, ACCOUNT, CONTAINER, NAME, Collections.singletonMap(1L,
            new NamedBlobManifestStore.PendingVersion(root, 1, children.iterator())));
        expectDataError("Conflicting manifest for named version",
            () -> NamedBlobManifestStore.retire(loser, ACCOUNT, CONTAINER, NAME, Collections.singletonMap(1L,
                new NamedBlobManifestStore.PendingVersion(root, 1,
                    Collections.singletonList(blob(BlobId.BlobDataType.DATACHUNK)).iterator()))));
        loser.rollback();
      }
    });
  }

  @Test
  public void testLegacyOperationsAcrossMigration() throws Exception {
    BlobId root = blob(BlobId.BlobDataType.METADATA);
    withDatabase(db -> {
      try (MySqlNamedBlobDb legacy = namedDb(db)) {
        legacyPutAndFinalize(legacy, "before", root);
      }
    }, (db, named) -> {
      assertEquals(root.getID(), named.get("account", "container", "before").get(10, TimeUnit.SECONDS).getBlobId());
      legacyPutAndFinalize(named, "after", root);
    });
  }

  private static void legacyPutAndFinalize(MySqlNamedBlobDb named, String name, BlobId root) throws Exception {
    NamedBlobRecord record = new NamedBlobRecord("account", "container", name, root.getID(), -1, 1);
    named.put(record, NamedBlobState.IN_PROGRESS, true).get(10, TimeUnit.SECONDS);
    named.updateBlobTtlAndStateToReady(NamedBlobRecord.forUpdate("account", "container", name, 1))
        .get(10, TimeUnit.SECONDS);
    assertEquals(root.getID(), named.get("account", "container", name).get(10, TimeUnit.SECONDS).getBlobId());
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
      retireOne(named, 3, root, 1, Collections.singletonList(blob(BlobId.BlobDataType.DATACHUNK)).iterator());
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
      assertEquals(1, scalar(db, "SELECT COUNT(*) FROM named_blob_manifests_v1"));
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
        retireOne(named, version, root, 1, Collections.singletonList(blob(BlobId.BlobDataType.DATACHUNK)).iterator());
      }
      assertEquals(33, scalar(db, "SELECT COUNT(*) FROM named_blobs_v2 WHERE retirement_fenced=TRUE"));
    });
  }

  @Test
  public void testRetirementLocksGapForReplacement() throws Exception {
    withDatabase((db, named) -> {
      BlobId root = blob(BlobId.BlobDataType.METADATA);
      insertNamed(db, 7, root, NamedBlobState.READY);
      List<BlobId> children = Collections.singletonList(blob(BlobId.BlobDataType.DATACHUNK));
      try (Connection publisher = db.getConnection(); Connection replacement = db.getConnection();
          Statement insert = replacement.createStatement()) {
        publisher.setAutoCommit(false);
        NamedBlobManifestStore.retire(publisher, ACCOUNT, CONTAINER, NAME, Collections.singletonMap(7L,
            new NamedBlobManifestStore.PendingVersion(root, 1, children.iterator())));
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
      // Replay after commit, via the high-level API, with the same exact content: still idempotent.
      retireOne(named, 7, root, 1, children.iterator());
      assertEquals(1, scalar(db, "SELECT COUNT(*) FROM named_blobs_v2 WHERE version=8 AND retirement_fenced=FALSE"
          + " AND deleted_ts IS NULL"));
    });
  }

  @Test
  public void testPublicationRollsBackWhenNameMutationFails() throws Exception {
    withDatabase((db, named) -> {
      Map<Long, BlobId> roots = new LinkedHashMap<>();
      Map<Long, List<BlobId>> childrenByVersion = new LinkedHashMap<>();
      for (long version = 1; version <= 2; version++) {
        BlobId root = blob(BlobId.BlobDataType.METADATA);
        insertNamed(db, version, root, NamedBlobState.READY);
        roots.put(version, root);
        childrenByVersion.put(version, Collections.singletonList(blob(BlobId.BlobDataType.DATACHUNK)));
      }
      List<Long> jobs = new ArrayList<>(roots.keySet());
      // Publication processes the canonical (ascending-by-version) order; make the trigger fail on the last
      // version in that order so the test still proves the earlier publish within the same transaction rolls back.
      long lastVersion = Collections.max(jobs);
      sql(db, "CREATE TRIGGER reject_retirement AFTER UPDATE ON named_blobs_v2 FOR EACH ROW BEGIN"
          + " IF NEW.version=" + lastVersion
          + " THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='fixture rejection'; END IF; END");
      try {
        retireMany(named, pendingFor(jobs, roots, childrenByVersion));
        fail("Expected mutation to fail");
      } catch (SQLException expected) {
        assertEquals("45000", expected.getSQLState());
        assertTrue(expected.getMessage(), expected.getMessage().contains("fixture rejection"));
      }
      // The whole transaction -- including the manifest rows this retire would have inserted -- rolled back: there
      // is no durable pre-publication row left behind, and no fence was applied either.
      assertEquals(0, scalar(db, "SELECT COUNT(*) FROM named_blob_manifests_v1"));
      assertEquals(2, scalar(db, "SELECT COUNT(*) FROM named_blobs_v2 WHERE deleted_ts IS NULL"
          + " AND retirement_fenced=FALSE"));
      sql(db, "DROP TRIGGER reject_retirement");
      retireMany(named, pendingFor(jobs, roots, childrenByVersion));
      List<Long> reversed = new ArrayList<>(jobs);
      reversed.sort(Collections.reverseOrder());
      try (Connection replay = db.getConnection()) {
        replay.setAutoCommit(false);
        NamedBlobManifestStore.retire(replay, ACCOUNT, CONTAINER, NAME,
            pendingFor(reversed, roots, childrenByVersion));
        try {
          NamedBlobManifestStore.retire(replay, ACCOUNT, CONTAINER, NAME, Collections.emptyMap());
          fail("An empty set is not a publication receipt");
        } catch (IllegalArgumentException expected) {
          assertEquals("Invalid number of manifests", expected.getMessage());
        }
        replay.commit();
      }
      expectDataError("Manifest belongs to a different publication",
          () -> retireMany(named, pendingFor(jobs.subList(0, 1), roots, childrenByVersion)));
    });
  }

  @Test
  public void testWaitingTtlCannotReviveRetiredVersion() throws Exception {
    withDatabase((db, named) -> {
      BlobId root = blob(BlobId.BlobDataType.METADATA);
      insertNamed(db, 7, root, NamedBlobState.READY);
      List<BlobId> children = Collections.singletonList(blob(BlobId.BlobDataType.DATACHUNK));
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
        NamedBlobManifestStore.retire(publisher, ACCOUNT, CONTAINER, NAME, Collections.singletonMap(7L,
            new NamedBlobManifestStore.PendingVersion(root, 1, children.iterator())));
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
   * The concretely reported gap: a retire only re-validates root and content when retire itself is called again.
   * Nothing used to fence a direct SQL write to named_blobs_v2, so once a cleanup job physically removes the
   * retired row, nothing stopped a fresh INSERT from rebinding the same tuple -- even to its own original root --
   * while the published manifest still exists. A published tuple can never go live again.
   */
  @Test
  public void testDirectReinsertAfterPhysicalRemovalRejectsReuse() throws Exception {
    withDatabase((db, named) -> {
      BlobId root = blob(BlobId.BlobDataType.METADATA);
      BlobId impostor = blob(BlobId.BlobDataType.METADATA);
      insertNamed(db, 7, root, NamedBlobState.READY);
      List<BlobId> children = Collections.singletonList(blob(BlobId.BlobDataType.DATACHUNK));
      retireOne(named, 7, root, 1, children.iterator());
      sql(db, "DELETE FROM named_blobs_v2 WHERE version=7");

      expectSqlRejection("Named version is reserved", () -> insertNamed(db, 7, impostor, NamedBlobState.READY));
      expectSqlRejection("Named version is reserved", () -> insertNamed(db, 7, root, NamedBlobState.READY));
      // A replay against the purged row still succeeds: the published manifest is proof enough on its own.
      retireOne(named, 7, root, 1, children.iterator());
      assertEquals(0, scalar(db, "SELECT COUNT(*) FROM named_blobs_v2 WHERE version=7"));
      // A different version was never part of this manifest's key and must be entirely unaffected.
      insertNamed(db, 9, impostor, NamedBlobState.READY);
      expectSqlRejection("Named version is reserved", () -> sql(db, "UPDATE named_blobs_v2 SET version=7,"
          + "blob_id=X'" + bytesToHex(root.toBytes()) + "' WHERE version=9"));
      assertEquals(1, scalar(db, "SELECT COUNT(*) FROM named_blobs_v2 WHERE version=9 AND blob_id=X'"
          + bytesToHex(impostor.toBytes()) + "'"));
    });
  }

  /**
   * An ordinary state-only update (no identity column change, e.g. the TTL path) never consults the manifest
   * table and must succeed even on a row that already carries a published manifest and is already fenced.
   */
  @Test
  public void testOrdinaryStateUpdateIgnoresExistingManifest() throws Exception {
    withDatabase((db, named) -> {
      BlobId root = blob(BlobId.BlobDataType.METADATA);
      insertNamed(db, 7, root, NamedBlobState.READY);
      retireOne(named, 7, root, 1, Collections.singletonList(blob(BlobId.BlobDataType.DATACHUNK)).iterator());
      sql(db, "UPDATE named_blobs_v2 SET modified_ts=UTC_TIMESTAMP(6) WHERE version=7");
      assertEquals(1, scalar(db, "SELECT COUNT(*) FROM named_blobs_v2 WHERE version=7 AND retirement_fenced=TRUE"));
    });
  }

  /**
   * Deterministically proves non-contention rather than merely hoping two threads race: the first PUT's own AFTER
   * INSERT trigger has already run and its transaction is deliberately left open (uncommitted) while a second PUT
   * for an unrelated name is attempted. A bounded lock-wait timeout on the second connection turns a reintroduced
   * cross-name gap lock into a prompt test failure instead of a hang.
   */
  @Test
  public void testConcurrentUnrelatedNamedPutsDoNotContendOnTheGuardTrigger() throws Exception {
    withDatabase((db, named) -> {
      String nameA = NAME + "/A";
      String nameB = NAME + "/B";
      BlobId rootA = blob(BlobId.BlobDataType.METADATA);
      BlobId rootB = blob(BlobId.BlobDataType.METADATA);
      try (Connection first = db.getConnection(); Connection second = db.getConnection()) {
        first.setAutoCommit(false);
        try (PreparedStatement insert = first.prepareStatement(
            "INSERT INTO named_blobs_v2 (account_id,container_id,blob_name,version,blob_id,blob_state)"
                + " VALUES (?,?,?,?,?,?)")) {
          insert.setShort(1, ACCOUNT);
          insert.setShort(2, CONTAINER);
          insert.setString(3, nameA);
          insert.setLong(4, 1);
          insert.setBytes(5, rootA.toBytes());
          insert.setInt(6, NamedBlobState.READY.ordinal());
          assertEquals(1, insert.executeUpdate());
        }
        try (Statement bound = second.createStatement();
            PreparedStatement insert = second.prepareStatement(
                "INSERT INTO named_blobs_v2 (account_id,container_id,blob_name,version,blob_id,blob_state)"
                    + " VALUES (?,?,?,?,?,?)")) {
          bound.execute("SET SESSION innodb_lock_wait_timeout=2");
          insert.setShort(1, ACCOUNT);
          insert.setShort(2, CONTAINER);
          insert.setString(3, nameB);
          insert.setLong(4, 1);
          insert.setBytes(5, rootB.toBytes());
          insert.setInt(6, NamedBlobState.READY.ordinal());
          // This would fail with InnoDB lock wait timeout (1205) here if the AFTER trigger introduced cross-name
          // contention: it only ever locks its own tuple, and only after the base row's own insert lock landed.
          assertEquals(1, insert.executeUpdate());
        }
        first.commit();
      }
    });
  }

  /**
   * The concretely reported hazard: two unrelated publishers' locking reads against absent manifest keys could
   * land in the same InnoDB index gap and deadlock (MySQL error 1213), even though they never touch the same
   * name. retire() never takes a locking read against a key that might not exist -- it inserts directly, and only
   * locks a key once a duplicate-key signal proves it exists -- so two fresh, concurrent publications for
   * different names must both complete without waiting on each other.
   */
  @Test
  public void testTwoUnrelatedFreshPublicationsDoNotContendOnTheManifestTable() throws Exception {
    withDatabase((db, named) -> {
      String nameA = NAME + "/A";
      String nameB = NAME + "/B";
      BlobId rootA = blob(BlobId.BlobDataType.METADATA);
      BlobId rootB = blob(BlobId.BlobDataType.METADATA);
      insertNamed(db, nameA, 1, rootA, NamedBlobState.READY);
      insertNamed(db, nameB, 1, rootB, NamedBlobState.READY);
      List<BlobId> childrenA = Collections.singletonList(blob(BlobId.BlobDataType.DATACHUNK));
      List<BlobId> childrenB = Collections.singletonList(blob(BlobId.BlobDataType.DATACHUNK));
      try (Connection first = db.getConnection(); Connection second = db.getConnection()) {
        first.setAutoCommit(false);
        second.setAutoCommit(false);
        try (Statement bound = second.createStatement()) {
          bound.execute("SET SESSION innodb_lock_wait_timeout=2");
        }
        // Both are genuinely fresh inserts into the same, otherwise empty manifest table; a preceding locking
        // read on either absent key would gap-lock the other, and the two inserts would then deadlock (1213).
        NamedBlobManifestStore.retire(first, ACCOUNT, CONTAINER, nameA, Collections.singletonMap(1L,
            new NamedBlobManifestStore.PendingVersion(rootA, 1, childrenA.iterator())));
        NamedBlobManifestStore.retire(second, ACCOUNT, CONTAINER, nameB, Collections.singletonMap(1L,
            new NamedBlobManifestStore.PendingVersion(rootB, 1, childrenB.iterator())));
        first.commit();
        second.commit();
      }
      assertEquals(2, scalar(db, "SELECT COUNT(*) FROM named_blob_manifests_v1"));
    });
  }

  /**
   * The concretely reported gap: a valid exact replay used to fail once the named range accumulated more unrelated,
   * unfenced replacement versions than a fresh publish could ever tolerate, even though the originally requested
   * tuple was already fully published. The non-locking replay check runs first and never touches named_blobs_v2 at
   * all, so it is never subject to that cap.
   */
  @Test
  public void testExactReplaySucceedsDespiteNamedRangeExceedingTheVersionCap() throws Exception {
    withDatabase((db, named) -> {
      BlobId root = blob(BlobId.BlobDataType.METADATA);
      insertNamed(db, 1, root, NamedBlobState.READY);
      List<BlobId> children = Collections.singletonList(blob(BlobId.BlobDataType.DATACHUNK));
      retireOne(named, 1, root, 1, children.iterator());
      // Pile up far more unrelated, unfenced replacement versions than a fresh publish could ever tolerate.
      for (long version = 2; version <= 40; version++) {
        insertNamed(db, version, blob(BlobId.BlobDataType.METADATA), NamedBlobState.READY);
      }
      // Replay the original, already-complete publication with the exact same content: it must still succeed.
      retireOne(named, 1, root, 1, children.iterator());
    });
  }

  /**
   * The concretely reported gap: isPublished and a replay used to confirm only a coarse version-set digest, so a
   * second intent with a different root for the same version could falsely confirm the first intent as committed.
   * Both the fast, non-locking replay path and the authoritative duplicate-key path bind content -- not just the
   * version set -- so a different root is rejected rather than silently confirmed.
   */
  @Test
  public void testDifferentRootForTheSameVersionCannotImpersonateAPublishedIntent() throws Exception {
    withDatabase((db, named) -> {
      BlobId publishedRoot = blob(BlobId.BlobDataType.METADATA);
      BlobId impostorRoot = blob(BlobId.BlobDataType.METADATA);
      insertNamed(db, 1, publishedRoot, NamedBlobState.READY);
      List<BlobId> children = Collections.singletonList(blob(BlobId.BlobDataType.DATACHUNK));
      retireOne(named, 1, publishedRoot, 1, children.iterator());
      NamedBlobManifestStore writer = new NamedBlobManifestStore(db);
      assertFalse(writer.isPublished(NamedBlobManifestStore.prepare(ACCOUNT, CONTAINER, NAME,
          Collections.singletonMap(1L, new NamedBlobManifestStore.PendingVersion(impostorRoot, 1,
              Collections.singletonList(blob(BlobId.BlobDataType.DATACHUNK)).iterator())))));
      expectDataError("Conflicting manifest for named version", () -> retireOne(named, 1, impostorRoot, 1,
          Collections.singletonList(blob(BlobId.BlobDataType.DATACHUNK)).iterator()));
    });
  }

  /**
   * The concretely reported gap: retireWithManifests used to capture the caller's own mutable map by reference
   * into a lambda queued for a background thread. Everything is now validated and every iterator fully consumed
   * into an immutable, caller-retainable description before that dispatch, so mutating or clearing the caller's
   * map immediately after the call returns must not change what gets published.
   */
  @Test
  public void testMutatingCallerMapAfterDispatchDoesNotAffectThePublishedContent() throws Exception {
    withDatabase((db, named) -> {
      BlobId root = blob(BlobId.BlobDataType.METADATA);
      BlobId decoy = blob(BlobId.BlobDataType.METADATA);
      insertNamed(db, 1, root, NamedBlobState.READY);
      List<BlobId> children = Collections.singletonList(blob(BlobId.BlobDataType.DATACHUNK));
      Map<Long, NamedBlobManifestStore.PendingVersion> pending = new LinkedHashMap<>();
      pending.put(1L, new NamedBlobManifestStore.PendingVersion(root, 1, children.iterator()));
      Future<Void> future = named.retireWithManifests("account", "container", NAME, pending);
      // Mutate the caller's own map immediately after dispatch: a captured reference to this map would let this
      // mutation race the background transaction and silently change what it publishes.
      pending.clear();
      pending.put(1L, new NamedBlobManifestStore.PendingVersion(decoy, 1,
          Collections.singletonList(blob(BlobId.BlobDataType.DATACHUNK)).iterator()));
      future.get(10, TimeUnit.SECONDS);
      NamedBlobManifestStore worker = new NamedBlobManifestStore(db);
      NamedBlobManifestStore.Manifest manifest = worker.getPublished(ACCOUNT, CONTAINER, NAME, 1);
      assertEquals(root.getID(), manifest.root);
    });
  }

  /**
   * The actual race the guard and the lock order exist for: a retire that has locked the named range (but not yet
   * committed its manifest insert) must block a concurrent direct replace for the same tuple. Once it commits,
   * reinsertion is rejected regardless of which root the racer uses.
   */
  @Test
  public void testConcurrentRetireBlocksRacingDirectReplaceThenRejectsReuse() throws Exception {
    withDatabase((db, named) -> {
      BlobId root = blob(BlobId.BlobDataType.METADATA);
      BlobId impostor = blob(BlobId.BlobDataType.METADATA);
      insertNamed(db, 7, root, NamedBlobState.READY);
      List<BlobId> children = Collections.singletonList(blob(BlobId.BlobDataType.DATACHUNK));
      try (Connection retiring = db.getConnection(); Connection replacing = db.getConnection();
          Statement replace = replacing.createStatement()) {
        retiring.setAutoCommit(false);
        NamedBlobManifestStore.retire(retiring, ACCOUNT, CONTAINER, NAME, Collections.singletonMap(7L,
            new NamedBlobManifestStore.PendingVersion(root, 1, children.iterator())));
        replace.execute("SET SESSION innodb_lock_wait_timeout=1");
        try {
          replace.executeUpdate("DELETE FROM named_blobs_v2 WHERE version=7");
          fail("Purge must wait for an in-flight retire");
        } catch (SQLException blocked) {
          assertEquals(1205, blocked.getErrorCode());
        }
        retiring.commit();
        replace.executeUpdate("DELETE FROM named_blobs_v2 WHERE version=7");
        expectSqlRejection("Named version is reserved", () -> insertNamed(db, 7, impostor, NamedBlobState.READY));
        expectSqlRejection("Named version is reserved", () -> insertNamed(db, 7, root, NamedBlobState.READY));
      }
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
   * Retires and reads back the declared cap of real child descriptors in a single manifest row, at the declared
   * 127-byte-per-child bound, and asserts the full ordered list survives exactly -- including repeats, since
   * only two distinct BlobId instances are reused across the whole cap to bound test heap -- under the same JDBC
   * settings and server-side prepared-statement mode as production.
   */
  @Test
  public void testFullScaleManifestRoundTripAtTheDeclaredChildCap() throws Exception {
    withDatabase((db, named) -> {
      BlobId root = blob(BlobId.BlobDataType.METADATA);
      insertNamed(db, 1, root, NamedBlobState.READY);
      BlobId even = maxSizedBlob(BlobId.BlobDataType.DATACHUNK, 'A');
      BlobId odd = maxSizedBlob(BlobId.BlobDataType.DATACHUNK, 'B');
      assertEquals(127, even.sizeInBytes());
      assertEquals(127, odd.sizeInBytes());
      Iterator<BlobId> children = new Iterator<BlobId>() {
        int produced = 0;

        @Override
        public boolean hasNext() {
          return produced < MAX_CHILDREN;
        }

        @Override
        public BlobId next() {
          return (produced++ % 2 == 0) ? even : odd;
        }
      };

      retireOne(named, 1, root, MAX_CHILDREN, children);

      NamedBlobManifestStore worker = new NamedBlobManifestStore(db);
      NamedBlobManifestStore.Manifest manifest = worker.getPublished(ACCOUNT, CONTAINER, NAME, 1);
      assertEquals(MAX_CHILDREN, manifest.count);

      List<String> recovered = worker.read(manifest);
      assertEquals(MAX_CHILDREN, recovered.size());
      for (int i = 0; i < MAX_CHILDREN; i++) {
        assertEquals("Mismatch at index " + i, (i % 2 == 0) ? even.getID() : odd.getID(), recovered.get(i));
      }
      // 262144 * (1 length byte + 127 id bytes) = 33,554,432 bytes = 32 MiB exactly.
      assertEquals(MAX_CHILDREN * (1 + 127), scalar(db, "SELECT OCTET_LENGTH(payload) FROM named_blob_manifests_v1"));
      assertEquals(1, scalar(db, "SELECT COUNT(*) FROM named_blob_manifests_v1"));
    });
  }

  /** One more than the declared per-version cap must be rejected before any descriptor is read. */
  @Test
  public void testRetireRejectsOneVersionOverTheDeclaredChildCapBeforeReadingDescriptors() throws Exception {
    withDatabase((db, named) -> {
      BlobId root = blob(BlobId.BlobDataType.METADATA);
      insertNamed(db, 1, root, NamedBlobState.READY);
      expectDataError("Unsupported named composite capture",
          () -> retireOne(named, 1, root, MAX_CHILDREN + 1, poisonIterator()));
      assertEquals(0, scalar(db, "SELECT COUNT(*) FROM named_blob_manifests_v1"));
    });
  }

  /**
   * Each individual version's declared count is within the per-version cap, but the request's aggregate count
   * across both versions is not; the rejection must happen purely from the declared counts, before either
   * version's iterator is ever touched.
   */
  @Test
  public void testRetireRejectsOversizedAggregateBeforeReadingAnyIterator() throws Exception {
    withDatabase((db, named) -> {
      BlobId rootA = blob(BlobId.BlobDataType.METADATA);
      BlobId rootB = blob(BlobId.BlobDataType.METADATA);
      insertNamed(db, 1, rootA, NamedBlobState.READY);
      insertNamed(db, 2, rootB, NamedBlobState.READY);
      int perVersion = (MAX_CHILDREN / 2) + 1;
      Map<Long, NamedBlobManifestStore.PendingVersion> pending = new LinkedHashMap<>();
      pending.put(1L, new NamedBlobManifestStore.PendingVersion(rootA, perVersion, poisonIterator()));
      pending.put(2L, new NamedBlobManifestStore.PendingVersion(rootB, perVersion, poisonIterator()));
      expectDataError("Mixed or oversized publication", () -> retireMany(named, pending));
      assertEquals(0, scalar(db, "SELECT COUNT(*) FROM named_blob_manifests_v1"));
    });
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
    // Mirror production's Hikari useServerPrepStmts=true so large-payload tests exercise the same statement mode.
    db.setUseServerPrepStmts(true);
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

  /** A V5 BlobId whose uuid string is long enough to reach the declared 127-byte-per-child cap exactly. */
  private static BlobId maxSizedBlob(BlobId.BlobDataType type, char marker) {
    char[] uuidChars = new char[MAX_ID_UUID_LENGTH];
    Arrays.fill(uuidChars, marker);
    return new BlobId(BlobId.BLOB_ID_V5, BlobId.BlobIdType.NATIVE, (byte) 0, ACCOUNT, CONTAINER,
        new MockPartitionId(), false, type, new String(uuidChars));
  }

  private static void insertNamed(DataSource db, long version, BlobId root, NamedBlobState state)
      throws SQLException {
    insertNamed(db, NAME, version, root, state);
  }

  private static void insertNamed(DataSource db, String name, long version, BlobId root, NamedBlobState state)
      throws SQLException {
    try (Connection c = db.getConnection(); PreparedStatement s = c.prepareStatement(
        "INSERT INTO named_blobs_v2 (account_id,container_id,blob_name,version,blob_id,blob_state)"
            + " VALUES (?,?,?,?,?,?)")) {
      s.setShort(1, ACCOUNT);
      s.setShort(2, CONTAINER);
      s.setString(3, name);
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
