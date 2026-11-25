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
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.json.JSONArray;
import org.junit.Test;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.Account;
import com.github.ambry.account.AccountBuilder;
import com.github.ambry.account.Container;
import com.github.ambry.account.ContainerBuilder;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.MySqlNamedBlobDbConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.frontend.Page;
import com.github.ambry.mysql.MySqlUtils.DbEndpoint;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.NamedBlobState;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import com.mysql.cj.jdbc.MysqlDataSource;
import com.zaxxer.hikari.HikariDataSource;

public class NamedBlobManifestStoreIntegrationTest {
  private static final short ACCOUNT = 17;
  private static final short CONTAINER = 4;
  private static final String NAME = "folder/MixedCase";

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
    return namedDb(db, new Properties(), SystemTime.getInstance());
  }

  private static MySqlNamedBlobDb namedDb(DataSource db, Properties properties, Time time) throws Exception {
    properties.setProperty(MySqlNamedBlobDbConfig.DB_INFO,
        "[{\"url\":\"jdbc:mysql://[::1]\",\"datacenter\":\"local\",\"isWriteable\":true,"
            + "\"username\":\"unused\",\"password\":\"unused\",\"sslMode\":\"NONE\"}]");
    return new MySqlNamedBlobDb(fixtureAccounts(), new MySqlNamedBlobDbConfig(new VerifiableProperties(properties)),
        endpoint -> db, "local", new Metrics(new MetricRegistry(), ""), time);
  }

  private static InMemAccountService fixtureAccounts() throws Exception {
    InMemAccountService accounts = new InMemAccountService(false, false);
    accounts.updateAccounts(Collections.singletonList(new AccountBuilder(ACCOUNT, "account", Account.AccountStatus.ACTIVE)
        .addOrUpdateContainer(new ContainerBuilder(CONTAINER, "container", Container.ContainerStatus.ACTIVE, "",
            ACCOUNT).build()).build()));
    return accounts;
  }

  @Test
  public void testFenceAwareGetWithSlowFrontend() throws Exception {
    withDatabase((db, writer) -> {
      BlobId root = blob(BlobId.BlobDataType.METADATA);
      long now = System.currentTimeMillis();
      long expires = now + TimeUnit.DAYS.toMillis(1);
      long fence = writer.put(NamedBlobRecord.forPut("account", "container", NAME, root.getID(), expires, 1),
          NamedBlobState.READY, false).get(10, TimeUnit.SECONDS).getInsertedRecord().getVersion();
      retire(writer, Collections.singletonList(stage(writer, NAME, fence, root, 1,
          Collections.singletonList(blob(BlobId.BlobDataType.DATACHUNK)).iterator())));
      // A lower-version insert after publication must not become the visible root.
      insertNamed(db, 6, blob(BlobId.BlobDataType.METADATA), NamedBlobState.READY);
      MockTime frontend = new MockTime(now - TimeUnit.SECONDS.toMillis(5));
      String replacement = blob(BlobId.BlobDataType.METADATA).getID();
      NamedBlobRecord put = NamedBlobRecord.forPut("account", "container", NAME, replacement, expires, 1);
      Properties props = new Properties();
      try (MySqlNamedBlobDb legacy = namedDb(db, props, frontend)) {
        assertEquals(root.getID(), legacy.get("account", "container", NAME).get(10, TimeUnit.SECONDS).getBlobId());
        try {
          legacy.put(put, NamedBlobState.READY, false).get(10, TimeUnit.SECONDS);
          fail("Legacy non-upsert must conflict while the deletion timestamp appears to be in the future");
        } catch (ExecutionException e) {
          assertEquals(RestServiceErrorCode.Conflict, ((RestServiceException) e.getCause()).getErrorCode());
        }
      }
      props.setProperty(MySqlNamedBlobDbConfig.HONOR_RETIREMENT_FENCE, "true");
      try (MySqlNamedBlobDb reader = namedDb(db, props, frontend)) {
        for (GetOption option : Arrays.asList(GetOption.None, GetOption.Include_Expired_Blobs)) {
          try {
            reader.get("account", "container", NAME, option, false).get(10, TimeUnit.SECONDS);
            fail("Expected Deleted for " + option);
          } catch (ExecutionException e) {
            assertEquals(RestServiceErrorCode.Deleted, ((RestServiceException) e.getCause()).getErrorCode());
          }
        }
        for (GetOption option : Arrays.asList(GetOption.Include_Deleted_Blobs, GetOption.Include_All)) {
          NamedBlobRecord record = reader.get("account", "container", NAME, option, false).get(10, TimeUnit.SECONDS);
          assertEquals(root.getID(), record.getBlobId());
          assertEquals(fence, record.getVersion());
        }
        for (boolean upsert : Arrays.asList(false, true)) {
          try {
            PutResult result = reader.put(put, NamedBlobState.READY, upsert).get(10, TimeUnit.SECONDS);
            fail("Acknowledged version " + result.getInsertedRecord().getVersion() + " behind fence " + fence);
          } catch (ExecutionException e) {
            assertEquals(RestServiceErrorCode.ServiceUnavailable, ((RestServiceException) e.getCause()).getErrorCode());
          }
        }
        // Supplied versions use the same floor, including equality and the first phase of a permanent PUT.
        try {
          reader.put(new NamedBlobRecord("account", "container", NAME, replacement, -1, fence),
              NamedBlobState.IN_PROGRESS, true).get(10, TimeUnit.SECONDS);
          fail("A version equal to the fence must be rejected before insert");
        } catch (ExecutionException e) {
          assertEquals(RestServiceErrorCode.ServiceUnavailable, ((RestServiceException) e.getCause()).getErrorCode());
        }
        assertEquals(2, scalar(db, "SELECT COUNT(*) FROM named_blobs_v2"));
        frontend.setCurrentMilliseconds(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(5));
        long accepted = reader.put(put, NamedBlobState.READY, false).get(10, TimeUnit.SECONDS)
            .getInsertedRecord().getVersion();
        assertTrue(accepted > fence);
        assertEquals(replacement, reader.get("account", "container", NAME).get(10, TimeUnit.SECONDS).getBlobId());
        assertPage(reader, null, null, NAME, accepted, null);
      }
    });
  }

  @Test
  public void testListAcrossRetirementFenceMigration() throws Exception {
    BlobId root = blob(BlobId.BlobDataType.METADATA);
    withDatabase(db -> {
      insertNamed(db, 1, root, NamedBlobState.READY);
      for (int option = 2; option <= 4; option++) {
        Properties props = new Properties();
        props.setProperty(MySqlNamedBlobDbConfig.LIST_NAMED_BLOBS_SQL_OPTION, Integer.toString(option));
        try (MySqlNamedBlobDb reader = namedDb(db, props, SystemTime.getInstance())) {
          assertEquals(root.getID(), reader.get("account", "container", NAME).get(10, TimeUnit.SECONDS).getBlobId());
          for (String prefix : Arrays.asList(null, "folder/")) {
            assertPage(reader, prefix, null, NAME, 1, null);
          }
        }
      }
    }, (db, writer) -> {
      String first = "folder/A", elapsed = "folder/B", legacy = "folder/C", last = "folder/Z";
      insertNamed(db, first, 1, root, NamedBlobState.READY);
      insertNamed(db, elapsed, 1, root, NamedBlobState.READY);
      insertNamed(db, elapsed, 2, root, NamedBlobState.READY);
      insertNamed(db, legacy, 1, root, NamedBlobState.READY);
      insertNamed(db, legacy, 2, root, NamedBlobState.READY);
      insertNamed(db, 2, root, NamedBlobState.READY);
      insertNamed(db, last, 1, root, NamedBlobState.READY);
      sql(db, "UPDATE named_blobs_v2 SET retirement_fenced=TRUE WHERE blob_name='folder/B' AND version=2");
      sql(db, "UPDATE named_blobs_v2 SET deleted_ts='2000-01-01' WHERE blob_name='folder/C' AND version=2");
      // The writer clock is ahead of LIST's database clock. deleted_ts alone would pass the outer filter.
      try (Connection c = db.getConnection(); Statement s = c.createStatement()) {
        s.execute("SET SESSION timestamp = UNIX_TIMESTAMP() + 3600");
        s.executeUpdate("UPDATE named_blobs_v2 SET retirement_fenced=TRUE WHERE blob_name='folder/MixedCase'"
            + " AND version=2");
      }
      for (int option = 2; option <= 4; option++) {
        Properties props = new Properties();
        props.setProperty(MySqlNamedBlobDbConfig.LIST_NAMED_BLOBS_SQL_OPTION, Integer.toString(option));
        props.setProperty(MySqlNamedBlobDbConfig.HONOR_RETIREMENT_FENCE, "true");
        try (MySqlNamedBlobDb reader = namedDb(db, props, SystemTime.getInstance())) {
          for (String prefix : Arrays.asList(null, "folder/")) {
            // Only legacy no-prefix options 2/3 fall back past an ordinary, unfenced expired row.
            boolean legacyFallback = prefix == null && option != 4;
            assertPage(reader, prefix, null, first, 1, legacyFallback ? legacy : last);
            if (legacyFallback) {
              assertPage(reader, prefix, legacy, legacy, 1, last);
            }
            assertPage(reader, prefix, last, last, 1, null);
          }
          insertNamed(db, 3, root, NamedBlobState.READY);
          for (String prefix : Arrays.asList(null, "folder/")) {
            assertPage(reader, prefix, NAME, NAME, 3, last);
          }
          sql(db, "DELETE FROM named_blobs_v2 WHERE version=3");
        }
      }
    });
  }

  private static void assertPage(MySqlNamedBlobDb reader, String prefix, String token, String name, long version,
      String next) throws Exception {
    Page<NamedBlobRecord> page = reader.list("account", "container", prefix, token, 1).get(10, TimeUnit.SECONDS);
    assertEquals(1, page.getEntries().size());
    assertEquals(name, page.getEntries().get(0).getBlobName());
    assertEquals(version, page.getEntries().get(0).getVersion());
    assertEquals(next, page.getNextPageToken());
  }

  private static String stage(MySqlNamedBlobDb db, String name, long version, BlobId root, int count,
      Iterator<BlobId> children) throws Exception {
    try {
      return db.stageDeleteManifest("account", "container", name, version, root, count, children)
          .get(10, TimeUnit.SECONDS);
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
    for (int i = 0; i <= NamedBlobManifestStore.PAGE_SIZE; i++) {
      children.add(blob(BlobId.BlobDataType.DATACHUNK));
    }
    List<String> ids = children.stream().map(BlobId::getID).collect(Collectors.toList());
    int boundary = NamedBlobManifestStore.PAGE_SIZE;
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
        stage(named, NAME, 7, old, children.size(), children.subList(0, boundary).iterator()));
    assertEquals(0, scalar(db, "SELECT COUNT(*) FROM named_blob_manifests_v1"));

    String job = stage(named, NAME, 7, old, children.size(), children.iterator());
    assertEquals(job, stage(named, NAME, 7, old, children.size(), children.iterator()));
    List<BlobId> reordered = new ArrayList<>(children);
    Collections.swap(reordered, 0, 1);
    expectDataError("Capture retry disagrees", () ->
        stage(named, NAME, 7, old, reordered.size(), reordered.iterator()));

    assertNull("Staging alone does not publish work", writer.getPublished(job));
    expectDataError("Unrepresented or ineligible", () -> retire(named, Collections.singletonList(job)));
    sql(db, "DELETE FROM named_blobs_v2 WHERE version=8");
    retire(named, Collections.singletonList(job));
    insertNamed(db, 8, replacement, NamedBlobState.READY);
    retire(named, Collections.singletonList(job));
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
    NamedBlobManifestStore.Manifest recovered = worker.getPublished(job);
    assertEquals(job, recovered.id);
    assertEquals(old.getID(), recovered.root);
    assertEquals(7L, recovered.version);
    assertEquals(ids.subList(0, boundary), worker.read(recovered, 0));

    try (Connection dirty = db.getConnection()) {
      dirty.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
      doReturn(dirty).when(readers).getConnection();
      try {
        worker.read(recovered, 0);
        fail("A valid header checkout must not authorize a dirty page checkout");
      } catch (IllegalStateException expected) {
        assertTrue(expected.getMessage(), expected.getMessage().contains("READ COMMITTED"));
      }
      assertTrue(dirty.isClosed());
    }

    NamedBlobManifestStore restarted = new NamedBlobManifestStore(db);
    NamedBlobManifestStore.Manifest again = restarted.getPublished(job);
    assertEquals(job, again.id);
    assertEquals(ids.subList(0, boundary), restarted.read(again, 0));
    assertEquals(ids.subList(boundary, ids.size()), restarted.read(again, boundary));
    assertNull(restarted.getPublished(UUID.randomUUID().toString()));

    // A second root can legitimately reference the same child. Both records remain evidence.
    String other = stage(named, NAME, 8, replacement, 1,
        Collections.singletonList(children.get(0)).iterator());
    assertNotEquals(job, other);
    retire(named, Collections.singletonList(other));
    NamedBlobManifestStore.Manifest second = restarted.getPublished(other);
    assertEquals(other, second.id);
    assertEquals(replacement.getID(), second.root);
    assertEquals(Collections.singletonList(ids.get(0)), restarted.read(second, 0));
    assertEquals(2, scalar(db, "SELECT COUNT(*) FROM named_blob_manifests_v1 WHERE publication_key IS NOT NULL"));

    try (Connection c = db.getConnection(); PreparedStatement s = c.prepareStatement(
        "UPDATE named_blob_manifest_pages_v1 SET payload=X'00' WHERE manifest_id=? AND first_child=0")) {
      s.setString(1, other);
      assertEquals(1, s.executeUpdate());
    }
    expectDataError("Manifest checksum mismatch", () -> restarted.read(second, 0));
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
        String winner = stage(named, NAME, 1, root, 1, children.iterator());
        assertEquals(winner, NamedBlobManifestStore.capture(loser, ACCOUNT, CONTAINER, NAME, 1, root, 1,
            children.iterator()));
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
      String job = stage(named, NAME, 3, root, 1, Collections.singletonList(blob(BlobId.BlobDataType.DATACHUNK))
          .iterator());
      retire(named, Collections.singletonList(job));
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
        retire(named, Collections.singletonList(stage(named, NAME, version, root, 1,
            Collections.singletonList(blob(BlobId.BlobDataType.DATACHUNK)).iterator())));
      }
      assertEquals(33, scalar(db, "SELECT COUNT(*) FROM named_blobs_v2 WHERE retirement_fenced=TRUE"));
    });
  }

  @Test
  public void testFenceAwarePutDuringPublication() throws Exception {
    withDatabase((db, named) -> {
      BlobId root = blob(BlobId.BlobDataType.METADATA);
      insertNamed(db, 100, root, NamedBlobState.READY);
      List<String> jobs = Collections.singletonList(stage(named, NAME, 100, root, 1,
          Collections.singletonList(blob(BlobId.BlobDataType.DATACHUNK)).iterator()));
      NamedBlobRecord lower = new NamedBlobRecord("account", "container", NAME, root.getID(), -1, 50);
      Properties props = new Properties();
      props.setProperty(MySqlNamedBlobDbConfig.HONOR_RETIREMENT_FENCE, "true");
      DataSource checkouts = mock(DataSource.class);
      try (Connection publisher = db.getConnection(); Connection rr = db.getConnection();
          Connection rc = db.getConnection();
          MySqlNamedBlobDb writer = namedDb(checkouts, props, SystemTime.getInstance())) {
        rr.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        rc.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        when(checkouts.getConnection()).thenReturn(rr, rc);
        publisher.setAutoCommit(false);
        NamedBlobManifestStore.retire(publisher, ACCOUNT, CONTAINER, NAME, jobs);
        CompletableFuture<PutResult> waiting = writer.put(lower, NamedBlobState.READY, true);
        awaitCount(publisher, "SELECT COUNT(*) FROM performance_schema.data_lock_waits"
            + " WHERE BLOCKING_THREAD_ID=PS_CURRENT_THREAD_ID()", 1);
        // A different checkout at RC must fail before the publisher releases the insert gap.
        assertTrue(unavailable(writer.put(lower, NamedBlobState.READY, true)).getMessage().contains("REPEATABLE READ"));
        publisher.commit();
        assertTrue(unavailable(waiting).getMessage().contains("newer than the retirement fence"));
      }
      assertEquals(0, scalar(db, "SELECT COUNT(*) FROM named_blobs_v2 WHERE version=50"));
    });
  }

  @Test
  public void testFirstPutIsolationAfterConnectionInit() throws Exception {
    withDatabase((db, named) -> {
      sql(db, "CREATE TABLE put_isolation (level_name VARCHAR(32) NOT NULL) ENGINE=InnoDB");
      sql(db, "CREATE TRIGGER record_put_isolation BEFORE INSERT ON named_blobs_v2 FOR EACH ROW"
          + " INSERT INTO put_isolation SELECT ISOLATION_LEVEL FROM performance_schema.events_transactions_current"
          + " WHERE THREAD_ID=PS_CURRENT_THREAD_ID()");
      // Simulate a server logon hook on each new physical connection without changing global CI server settings.
      DataSource physical = mock(DataSource.class);
      when(physical.getConnection()).thenAnswer(invocation -> {
        Connection c = db.getConnection();
        try (Statement s = c.createStatement()) {
          s.execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ");
          s.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED");
        }
        return c;
      });
      Properties props = new Properties();
      props.setProperty(MySqlNamedBlobDbConfig.HONOR_RETIREMENT_FENCE, "true");
      for (boolean initializeFenceAwarePool : Arrays.asList(false, true)) {
        try (HikariDataSource pool = new HikariDataSource()) {
          pool.setDataSource(physical);
          pool.setMaximumPoolSize(1);
          MySqlNamedBlobDbFactory.configureIsolation(pool, TransactionIsolationLevel.TRANSACTION_REPEATABLE_READ,
              initializeFenceAwarePool);
          try (MySqlNamedBlobDb writer = namedDb(pool, props, SystemTime.getInstance())) {
            NamedBlobRecord record = NamedBlobRecord.forPut("account", "container", NAME + initializeFenceAwarePool,
                blob(BlobId.BlobDataType.METADATA).getID(), -1, 1);
            writer.put(record, NamedBlobState.READY, true).get(10, TimeUnit.SECONDS);
          }
          String expected = initializeFenceAwarePool ? "REPEATABLE READ" : "READ COMMITTED";
          assertEquals(1, scalar(db, "SELECT COUNT(*) FROM put_isolation WHERE level_name='" + expected + "'"));
          sql(db, "DELETE FROM put_isolation");
        }
      }
    });
  }

  @Test
  public void testFenceAwarePutChecksServerInsteadOfCachedSessionSettings() throws Exception {
    withDatabase((db, named) -> {
      MysqlDataSource physical;
      try (Connection c = db.getConnection()) {
        physical = source(c.getCatalog());
      }
      physical.setUseConfigs("maxPerformance");
      BlobId root = blob(BlobId.BlobDataType.METADATA);
      insertNamed(db, 7, root, NamedBlobState.READY);
      String job = stage(named, NAME, 7, root, 1,
          Collections.singletonList(blob(BlobId.BlobDataType.DATACHUNK)).iterator());
      sql(db, "CREATE TABLE eviction_sentinel (id INT PRIMARY KEY) ENGINE=InnoDB");
      Properties props = new Properties();
      props.setProperty(MySqlNamedBlobDbConfig.HONOR_RETIREMENT_FENCE, "true");
      try (HikariDataSource pool = repeatableReadPool(physical, 1);
          MySqlNamedBlobDb writer = namedDb(pool, props, SystemTime.getInstance())) {
        NamedBlobRecord put = NamedBlobRecord.forPut("account", "container", NAME,
            blob(BlobId.BlobDataType.METADATA).getID(), -1, 1);
        // Change only this pooled session, not the test server's defaults or Connector/J's local state.
        for (String settings : Arrays.asList("autocommit=1, transaction_isolation='READ-COMMITTED'",
            "autocommit=0, transaction_isolation='REPEATABLE-READ'")) {
          int rejectedId;
          try (Connection c = pool.getConnection(); Statement s = c.createStatement()) {
            s.execute("SET SESSION " + settings);
            rejectedId = scalar(c, "SELECT CONNECTION_ID()");
            assertTrue(c.getAutoCommit());
            assertEquals(Connection.TRANSACTION_REPEATABLE_READ, c.getTransactionIsolation());
            assertEquals(0, scalar(c, "SELECT @@session.autocommit=1 AND @@session.transaction_isolation='REPEATABLE-READ'"));
            if (scalar(c, "SELECT @@session.autocommit") == 0) {
              assertEquals(0, scalar(c, "SELECT retirement_fenced FROM named_blobs_v2 WHERE version=7"));
              s.executeUpdate("INSERT INTO eviction_sentinel VALUES (1)");
            }
          }
          assertTrue(unavailable(writer.put(put, NamedBlobState.READY, false)).getMessage().contains("REPEATABLE READ"));
          try (Connection next = pool.getConnection()) {
            assertNotEquals(rejectedId, scalar(next, "SELECT CONNECTION_ID()"));
          }
        }
        retire(named, Collections.singletonList(job));
        try {
          writer.get("account", "container", NAME).get(10, TimeUnit.SECONDS);
          fail("A subsequent GET must not reuse a pre-retirement snapshot");
        } catch (ExecutionException e) {
          assertEquals(RestServiceErrorCode.Deleted, ((RestServiceException) e.getCause()).getErrorCode());
        }
        assertTrue(writer.list("account", "container", null, null, 1).get(10, TimeUnit.SECONDS).getEntries().isEmpty());
      }
      assertEquals(0, scalar(db, "SELECT COUNT(*) FROM eviction_sentinel"));
      assertEquals(1, scalar(db, "SELECT COUNT(*) FROM named_blobs_v2"));
    });
  }

  @Test
  public void testFenceAwarePutRejectsActiveTransaction() throws Exception {
    checkActiveTransactionRejected(false);
  }

  @Test
  public void testFenceAwarePutRejectsActiveTransactionWithUncachedServerPrepares() throws Exception {
    checkActiveTransactionRejected(true);
  }

  private void checkActiveTransactionRejected(boolean serverPrepares) throws Exception {
    withDatabase((db, named) -> {
      MysqlDataSource physical;
      try (Connection c = db.getConnection()) {
        physical = source(c.getCatalog());
      }
      physical.setUseServerPrepStmts(serverPrepares);
      physical.setCachePrepStmts(false);
      sql(db, "CREATE TABLE transaction_sentinel (id INT PRIMARY KEY) ENGINE=InnoDB");
      Properties props = new Properties();
      props.setProperty(MySqlNamedBlobDbConfig.HONOR_RETIREMENT_FENCE, "true");
      try (HikariDataSource pool = repeatableReadPool(physical, 1);
          MySqlNamedBlobDb writer = namedDb(pool, props, SystemTime.getInstance())) {
        MySqlNamedBlobDbFactory.configureIsolation(pool, TransactionIsolationLevel.TRANSACTION_REPEATABLE_READ, true);
        int rejectedId;
        try (Connection c = pool.getConnection(); Statement s = c.createStatement()) {
          s.execute("START TRANSACTION");
          s.executeUpdate("INSERT INTO transaction_sentinel VALUES (1)");
          assertEquals(1, scalar(c, "SELECT @@session.autocommit=1 AND @@session.transaction_isolation='REPEATABLE-READ'"));
          rejectedId = scalar(c, "SELECT CONNECTION_ID()");
        }
        NamedBlobRecord put = NamedBlobRecord.forPut("account", "container", NAME,
            blob(BlobId.BlobDataType.METADATA).getID(), -1, 1);
        assertTrue(unavailable(writer.put(put, NamedBlobState.READY, true)).getMessage().contains("active transaction"));
        try (Connection next = pool.getConnection()) {
          assertNotEquals(rejectedId, scalar(next, "SELECT CONNECTION_ID()"));
        }
        assertEquals(0, scalar(db, "SELECT COUNT(*) FROM transaction_sentinel"));
        assertEquals(0, scalar(db, "SELECT COUNT(*) FROM named_blobs_v2"));
        writer.put(put, NamedBlobState.READY, true).get(10, TimeUnit.SECONDS);
        assertEquals(1, scalar(db, "SELECT COUNT(*) FROM named_blobs_v2"));
      }
    });
  }

  @Test
  public void testFenceAwarePutRejectsSwitchableSessions() throws Exception {
    withDatabase((db, named) -> {
      Properties props = new Properties();
      props.setProperty(MySqlNamedBlobDbConfig.HONOR_RETIREMENT_FENCE, "true");
      try (Connection c = db.getConnection()) {
        for (boolean loadBalanced : Arrays.asList(true, false)) {
          MysqlDataSource physical = source(c.getCatalog());
          if (loadBalanced) {
            physical.setURL(physical.getURL().replace("jdbc:mysql://", "jdbc:mysql:loadbalance://"));
            physical.setLoadBalanceAutoCommitStatementThreshold(1);
          } else {
            physical.setAutoReconnect(true);
          }
          try (HikariDataSource pool = repeatableReadPool(physical, 1);
              MySqlNamedBlobDb writer = namedDb(pool, props, SystemTime.getInstance())) {
            NamedBlobRecord put = NamedBlobRecord.forPut("account", "container", NAME,
                blob(BlobId.BlobDataType.METADATA).getID(), -1, 1);
            assertTrue(unavailable(writer.put(put, NamedBlobState.READY, true)).getMessage().contains("fixed"));
          }
        }
        assertEquals(0, scalar(c, "SELECT COUNT(*) FROM named_blobs_v2"));
      }
    });
  }

  private static HikariDataSource repeatableReadPool(DataSource source, int size) {
    HikariDataSource pool = new HikariDataSource();
    pool.setDataSource(source);
    pool.setMaximumPoolSize(size);
    pool.setTransactionIsolation("TRANSACTION_REPEATABLE_READ");
    return pool;
  }

  @Test
  public void testFactoryAppliesFencePolicyToWritableEndpoints() throws Exception {
    MysqlDataSource db = source("");
    Properties props = new Properties();
    props.setProperty(ClusterMapConfig.CLUSTERMAP_DATACENTER_NAME, "local");
    props.setProperty(MySqlNamedBlobDbConfig.DB_INFO, "[]");
    for (boolean enabled : Arrays.asList(false, true)) {
      props.setProperty(MySqlNamedBlobDbConfig.HONOR_RETIREMENT_FENCE, Boolean.toString(enabled));
      MySqlNamedBlobDbFactory factory = new MySqlNamedBlobDbFactory(new VerifiableProperties(props),
          new MetricRegistry(), new InMemAccountService(false, false));
      for (boolean writable : Arrays.asList(false, true)) {
        DbEndpoint endpoint = new DbEndpoint(db.getURL(), "remote", writable, db.getUser(), db.getPassword());
        try (HikariDataSource pool = factory.buildDataSource(endpoint)) {
          assertEquals(enabled && writable ? "TRANSACTION_REPEATABLE_READ" : null, pool.getTransactionIsolation());
          assertEquals(enabled && writable ? "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ" : null,
              pool.getConnectionInitSql());
        }
      }
    }
  }

  @Test
  public void testFenceAwareUpsertsRetryDeadlockOnAdjacentNames() throws Exception {
    checkContendingPuts(Arrays.asList("a", "b"), true);
  }

  @Test
  public void testFenceAwareCreateOnlyDoesNotReplayAfterDeadlock() throws Exception {
    checkContendingPuts(Arrays.asList("a", "a"), false);
  }

  private void checkContendingPuts(List<String> names, boolean isUpsert) throws Exception {
    withDatabase((db, named) -> {
      Properties props = new Properties();
      props.setProperty(MySqlNamedBlobDbConfig.HONOR_RETIREMENT_FENCE, "true");
      // Pause both inserts after the fence lookup but before either can insert into the shared gap.
      sql(db, "CREATE TRIGGER wait_for_puts BEFORE INSERT ON named_blobs_v2 FOR EACH ROW BEGIN"
          + " IF GET_LOCK(DATABASE(),30) <> 1 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='barrier timeout'; END IF;"
          + " DO RELEASE_LOCK(DATABASE()); END");
      MysqlDataSource physical = (MysqlDataSource) db;
      DbEndpoint endpoint = new DbEndpoint(physical.getURL(), "local", true, physical.getUser(), physical.getPassword());
      props.setProperty(MySqlNamedBlobDbConfig.LOCAL_POOL_SIZE, "2");
      props.setProperty(MySqlNamedBlobDbConfig.DB_INFO, new JSONArray().put(endpoint.toJson()).toString());
      props.setProperty(ClusterMapConfig.CLUSTERMAP_DATACENTER_NAME, "local");
      MetricRegistry registry = new MetricRegistry();
      AtomicLong now = new AtomicLong(System.currentTimeMillis());
      Time clock = mock(Time.class);
      when(clock.milliseconds()).thenAnswer(invocation -> now.get());
      MySqlNamedBlobDbFactory factory = new MySqlNamedBlobDbFactory(new VerifiableProperties(props), registry,
          fixtureAccounts(), clock, "");
      try (Connection gate = db.getConnection(); MySqlNamedBlobDb writer = factory.getNamedBlobDb()) {
        assertEquals(1, scalar(gate, "SELECT GET_LOCK(DATABASE(),0)"));
        List<CompletableFuture<PutResult>> puts = new ArrayList<>();
        for (String name : names) {
          puts.add(writer.put(NamedBlobRecord.forPut("account", "container", name,
              blob(BlobId.BlobDataType.METADATA).getID(), -1, 1), NamedBlobState.READY, isUpsert));
        }
        awaitCount(gate, "SELECT COUNT(*) FROM performance_schema.metadata_locks WHERE OBJECT_TYPE='USER LEVEL LOCK'"
            + " AND OBJECT_NAME=DATABASE() AND LOCK_STATUS='PENDING'", 2);
        // Both versions are bound. Any version generated after releasing the lock must exceed this boundary.
        long boundary = MySqlNamedBlobDb.buildVersion(new MockTime(now.incrementAndGet()));
        now.incrementAndGet();
        assertEquals(1, scalar(gate, "SELECT RELEASE_LOCK(DATABASE())"));
        List<NamedBlobRecord> acknowledged = new ArrayList<>();
        int victims = 0;
        for (int i = 0; i < puts.size(); i++) {
          try {
            NamedBlobRecord record = puts.get(i).get(10, TimeUnit.SECONDS).getInsertedRecord();
            assertEquals(names.get(i), record.getBlobName());
            acknowledged.add(record);
          } catch (ExecutionException e) {
            RestServiceException failure = (RestServiceException) e.getCause();
            assertEquals(RestServiceErrorCode.ServiceUnavailable, failure.getErrorCode());
            assertEquals(1213, ((SQLException) failure.getCause()).getErrorCode());
            victims++;
          }
        }
        assertEquals(isUpsert ? 0 : 1, victims);
        assertEquals(1, registry.getCounters().get(MetricRegistry.name(MySqlNamedBlobDb.class,
            "FenceAwarePutDeadlockCount")).getCount());
        assertEquals(victims, registry.getCounters().get(MetricRegistry.name(MySqlNamedBlobDb.class,
            "NamedBlobDBInsertErrorCount")).getCount());
        try (Statement read = gate.createStatement();
            ResultSet stored = read.executeQuery("SELECT blob_name, version FROM named_blobs_v2 ORDER BY blob_name")) {
          for (NamedBlobRecord record : acknowledged) {
            assertTrue("Missing acknowledged PUT", stored.next());
            assertEquals(record.getBlobName(), stored.getString("blob_name"));
            long version = stored.getLong("version");
            assertEquals(record.getVersion(), version);
            assertTrue("Retry generated a new version", version < boundary);
          }
          assertFalse("Unexpected stored PUT", stored.next());
        }
      }
    });
  }

  private static RestServiceException unavailable(CompletableFuture<PutResult> put) throws Exception {
    try {
      put.get(10, TimeUnit.SECONDS);
      throw new AssertionError("PUT must not be acknowledged");
    } catch (ExecutionException e) {
      RestServiceException failure = (RestServiceException) e.getCause();
      assertEquals(RestServiceErrorCode.ServiceUnavailable, failure.getErrorCode());
      return failure;
    }
  }

  private static void awaitCount(Connection c, String query, int count) throws SQLException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (scalar(c, query) < count) {
      assertTrue("Did not observe " + count + " lock waiter(s)", System.nanoTime() < deadline);
    }
  }

  @Test
  public void testRetirementLocksGapForReplacement() throws Exception {
    withDatabase((db, named) -> {
      BlobId root = blob(BlobId.BlobDataType.METADATA);
      insertNamed(db, 7, root, NamedBlobState.READY);
      List<String> jobs = Collections.singletonList(stage(named, NAME, 7, root, 1,
          Collections.singletonList(blob(BlobId.BlobDataType.DATACHUNK)).iterator()));
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
      List<String> jobs = new ArrayList<>();
      for (int version = 1; version <= 2; version++) {
        BlobId root = blob(BlobId.BlobDataType.METADATA);
        insertNamed(db, version, root, NamedBlobState.READY);
        jobs.add(stage(named, NAME, version, root, 1,
            Collections.singletonList(blob(BlobId.BlobDataType.DATACHUNK)).iterator()));
      }
      // The transaction mutates these versions in manifest-ID order, not version order.
      int lastVersion = jobs.indexOf(Collections.max(jobs)) + 1;
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
      List<String> jobs = Collections.singletonList(stage(named, NAME, 7, root, 1,
          Collections.singletonList(blob(BlobId.BlobDataType.DATACHUNK)).iterator()));
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

  private static void retire(MySqlNamedBlobDb db, List<String> jobs) throws Exception {
    try {
      db.retireWithManifests("account", "container", NAME, jobs).get(10, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof SQLException) {
        throw (SQLException) e.getCause();
      }
      throw e;
    }
  }

  private static int scalar(DataSource db, String sql) throws SQLException {
    try (Connection c = db.getConnection()) {
      return scalar(c, sql);
    }
  }

  private static int scalar(Connection c, String sql) throws SQLException {
    try (Statement s = c.createStatement(); ResultSet r = s.executeQuery(sql)) {
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
