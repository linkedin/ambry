/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 */
package com.github.ambry.named;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.MySqlNamedBlobDbConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.frontend.Page;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.NamedBlobState;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Integration tests for {@link MySqlNamedBlobDb}.
 */
public class MySqlNamedBlobDbIntegrationTest {
  private static final String LOCAL_DC = "dc1";
  // Please note that we are using mock time for time travel. Need to reset time (call setCurrentMilliseconds) when the
  // tests depends on time sequences
  private static final MockTime time = new MockTime(System.currentTimeMillis());
  private final MySqlNamedBlobDb namedBlobDb;
  private final InMemAccountService accountService;
  private final PartitionId partitionId;
  private final MySqlNamedBlobDbConfig config;

  public MySqlNamedBlobDbIntegrationTest() throws Exception {
    Properties properties = Utils.loadPropsFromResource("mysql.properties");
    properties.setProperty(ClusterMapConfig.CLUSTERMAP_DATACENTER_NAME, LOCAL_DC);
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    config = new MySqlNamedBlobDbConfig(verifiableProperties);
    accountService = new InMemAccountService(false, false);
    for (int i = 0; i < 5; i++) {
      accountService.createAndAddRandomAccount();
    }
    MockClusterMap clusterMap = new MockClusterMap();
    partitionId = clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0);
    MySqlNamedBlobDbFactory namedBlobDbFactory =
        new MySqlNamedBlobDbFactory(verifiableProperties, new MetricRegistry(), accountService, time);
    namedBlobDb = namedBlobDbFactory.getNamedBlobDb();

    cleanup();
  }

  /**
   * Tests sequences of puts, gets, lists, and deletes across multiple containers.
   * @throws Exception
   */
  @Test
  public void testPutGetListDeleteSequence() throws Exception {
    int blobsPerContainer = 5;

    List<NamedBlobRecord> records = new ArrayList<>();
    for (Account account : accountService.getAllAccounts()) {
      for (Container container : account.getAllContainers()) {
        for (int i = 0; i < blobsPerContainer; i++) {
          String blobId = getBlobId(account, container);
          String blobName = "name/" + i + "/more path segments--";
          long expirationTime =
              i % 2 == 0 ? Utils.Infinite_Time : System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1);
          NamedBlobRecord record =
              new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId, expirationTime);

          namedBlobDb.put(record).get();
          records.add(record);
        }
      }
    }

    // get records just inserted
    time.setCurrentMilliseconds(System.currentTimeMillis());
    for (NamedBlobRecord record : records) {
      NamedBlobRecord recordFromStore =
          namedBlobDb.get(record.getAccountName(), record.getContainerName(), record.getBlobName()).get();
      assertEquals("Record does not match expectations.", record, recordFromStore);
    }

    // list records in each container
    time.setCurrentMilliseconds(System.currentTimeMillis());
    for (Account account : accountService.getAllAccounts()) {
      for (Container container : account.getAllContainers()) {
        Page<NamedBlobRecord> page = namedBlobDb.list(account.getName(), container.getName(), "name", null).get();
        assertNull("No continuation token expected", page.getNextPageToken());
        assertEquals("Unexpected number of blobs in container", blobsPerContainer, page.getEntries().size());
      }
    }

    // check that puts to the same keys fail.
    time.setCurrentMilliseconds(System.currentTimeMillis());
    for (Account account : accountService.getAllAccounts()) {
      for (Container container : account.getAllContainers()) {
        for (int i = 0; i < blobsPerContainer; i++) {
          String blobId = getBlobId(account, container);
          String blobName = "name/" + i + "/more path segments--";
          NamedBlobRecord record =
              new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId, Utils.Infinite_Time);
          checkErrorCode(() -> namedBlobDb.put(record), RestServiceErrorCode.Conflict);
        }
      }
    }

    // delete the records and check that they cannot be fetched with a get call.
    for (NamedBlobRecord record : records) {
      time.setCurrentMilliseconds(System.currentTimeMillis());
      DeleteResult deleteResult =
          namedBlobDb.delete(record.getAccountName(), record.getContainerName(), record.getBlobName()).get();
      assertEquals("Unexpected deleted ID", record.getBlobId(), deleteResult.getBlobId());
      assertFalse("Unexpected alreadyDeleted value", deleteResult.isAlreadyDeleted());
      time.setCurrentMilliseconds(System.currentTimeMillis());
      checkErrorCode(() -> namedBlobDb.get(record.getAccountName(), record.getContainerName(), record.getBlobName()),
          RestServiceErrorCode.Deleted);
      NamedBlobRecord recordFromStore =
          namedBlobDb.get(record.getAccountName(), record.getContainerName(), record.getBlobName(),
              GetOption.Include_Deleted_Blobs).get();
      checkRecordsEqual(record, recordFromStore);
      recordFromStore = namedBlobDb.get(record.getAccountName(), record.getContainerName(), record.getBlobName(),
          GetOption.Include_All).get();
      checkRecordsEqual(record, recordFromStore);
    }

    // deletes should be idempotent and additional delete calls should succeed
    time.setCurrentMilliseconds(System.currentTimeMillis());
    for (NamedBlobRecord record : records) {
      DeleteResult deleteResult =
          namedBlobDb.delete(record.getAccountName(), record.getContainerName(), record.getBlobName()).get();
      assertEquals("Unexpected deleted ID", record.getBlobId(), deleteResult.getBlobId());
      assertTrue("Unexpected alreadyDeleted value", deleteResult.isAlreadyDeleted());
    }

    // delete and get for non existent blobs should return not found.
    time.setCurrentMilliseconds(System.currentTimeMillis());
    for (NamedBlobRecord record : records) {
      String nonExistentName = record.getBlobName() + "-other";
      checkErrorCode(() -> namedBlobDb.get(record.getAccountName(), record.getContainerName(), nonExistentName),
          RestServiceErrorCode.NotFound);
      checkErrorCode(() -> namedBlobDb.delete(record.getAccountName(), record.getContainerName(), nonExistentName),
          RestServiceErrorCode.NotFound);
    }

    records.clear();
    // should be able to put new records again after deletion
    time.setCurrentMilliseconds(System.currentTimeMillis());
    for (Account account : accountService.getAllAccounts()) {
      for (Container container : account.getAllContainers()) {
        for (int i = 0; i < blobsPerContainer; i++) {
          String blobId = getBlobId(account, container);
          String blobName = "name/" + i + "/more path segments--";
          long expirationTime =
              i % 2 == 1 ? Utils.Infinite_Time : System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1);
          NamedBlobRecord record =
              new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId, expirationTime);
          namedBlobDb.put(record).get();
          records.add(record);
        }
      }
    }
  }

  /**
   * Test behavior with expired blobs
   */
  @Test
  public void testExpiredBlobs() throws Exception {
    time.setCurrentMilliseconds(System.currentTimeMillis());
    Account account = accountService.getAllAccounts().iterator().next();
    Container container = account.getAllContainers().iterator().next();

    String blobId = getBlobId(account, container);
    String blobName = "name";
    long expirationTime = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(24);
    NamedBlobRecord record =
        new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId, expirationTime);
    namedBlobDb.put(record).get();

    time.setCurrentMilliseconds(System.currentTimeMillis());

    Thread.sleep(100);
    checkErrorCode(() -> namedBlobDb.get(account.getName(), container.getName(), blobName),
        RestServiceErrorCode.Deleted);
    NamedBlobRecord recordFromStore =
        namedBlobDb.get(account.getName(), container.getName(), blobName, GetOption.Include_All).get();
    assertEquals("Record does not match expectations.", record, recordFromStore);
    recordFromStore =
        namedBlobDb.get(account.getName(), container.getName(), blobName, GetOption.Include_Expired_Blobs).get();
    assertEquals("Record does not match expectations.", record, recordFromStore);

    // replacement should succeed
    blobId = getBlobId(account, container);
    record = new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId, Utils.Infinite_Time);
    namedBlobDb.put(record).get();
    assertEquals("Record should have been replaced", record,
        namedBlobDb.get(account.getName(), container.getName(), blobName).get());
  }

  /**
   * Test behavior with blob cleanup main pipeline
   */
  @Test
  public void testCleanupBlobsPipeline() throws Exception {
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    calendar.add(Calendar.DATE, -config.staleDataRetentionDays);
    long staleCutoffTime = calendar.getTimeInMillis();
    long staleCutoffTimePlusOneMillisecond = staleCutoffTime + 1;

    Account account = accountService.getAllAccounts().iterator().next();
    Container container = account.getAllContainers().iterator().next();

    int staleCount = 10;
    int needCleanupCount = 0;
    List<NamedBlobRecord> records = new ArrayList<>();

    // Create stale named blob records
    for (int i = 0; i < staleCount; i++) {
      String blobId = getBlobId(account, container);
      String blobName = "stale/" + i + "/more path segments--";
      long expirationTime = i % 2 == 0 ? Utils.Infinite_Time : staleCutoffTimePlusOneMillisecond;
      NamedBlobRecord record =
          new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId, expirationTime);

      NamedBlobState blob_state = expirationTime == Utils.Infinite_Time ? NamedBlobState.IN_PROGRESS : NamedBlobState.READY;

      time.setCurrentMilliseconds(staleCutoffTime);
      namedBlobDb.put(record, blob_state, true).get();

      time.setCurrentMilliseconds(staleCutoffTimePlusOneMillisecond);
      namedBlobDb.put(record, NamedBlobState.READY, true).get();

      if (expirationTime == Utils.Infinite_Time) {
        needCleanupCount += 1;
      }
      records.add(record);
    }

    Thread.sleep(100);

    // Confirm the pullStaleBlobs indeed pulled out the stale blob cases
    Set<String> staleInputSet = records.stream().filter((r) -> r.getExpirationTimeMs() == Utils.Infinite_Time).map((r) ->
        String.join("|", r.getBlobName(), r.getBlobId())
    ).collect(Collectors.toSet());

    List<StaleNamedBlob> staleNamedBlobs = namedBlobDb.pullStaleBlobs().get();
    Set<String> staleResultSet = staleNamedBlobs.stream().map((s) ->
        String.join("|", s.getBlobName(), s.getBlobId())
    ).collect(Collectors.toSet());
    assertEquals("Stale records count does not match!", needCleanupCount, staleNamedBlobs.size());
    assertEquals("Stale records pulled out does not meet expectation", staleInputSet, staleResultSet);

    // Confirm pullStaleBlobs return empty list after cleanupStaleData is called
    Integer cleanedUpStaleCount = namedBlobDb.cleanupStaleData(staleNamedBlobs).get();
    List<StaleNamedBlob> staleNamedBlobsNew = namedBlobDb.pullStaleBlobs().get();

    assertEquals("Cleaned Stale records count does not match!", needCleanupCount, cleanedUpStaleCount.intValue());
    assertTrue("Still pulled out stale blobs after cleanup!", staleNamedBlobsNew.isEmpty());


    // Verify we can still pull out the valid named blobs after cleanup
    for (NamedBlobRecord record : records) {
      if (record.getExpirationTimeMs() == Utils.Infinite_Time) {
        NamedBlobRecord recordFromDb =
            namedBlobDb.get(record.getAccountName(), record.getContainerName(), record.getBlobName()).get();
        assertEquals("After stale cleanup, the record does not match expectation.", record, recordFromDb);
      }
    }

    time.setCurrentMilliseconds(System.currentTimeMillis());
  }

  /**
   * Test behavior with blob cleanup for stale blob case
   * Case 1. created more than staleDataRetentionDays ago, InProgress, is Largest (Don't care about the ttl and delete)
   */
  @Test
  public void testCleanupBlobStaleCase1() throws Exception {
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    calendar.add(Calendar.DATE, -config.staleDataRetentionDays);
    long staleCutoffTime = calendar.getTimeInMillis();

    Account account = accountService.getAllAccounts().iterator().next();
    Container container = account.getAllContainers().iterator().next();
    String blobId = getBlobId(account, container);
    String blobName = "stale/" + "case1"  + "/more path segments--";
    NamedBlobRecord record =
        new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId, Utils.Infinite_Time);

    time.setCurrentMilliseconds(staleCutoffTime);
    namedBlobDb.put(record,  NamedBlobState.IN_PROGRESS, true).get();

    List<StaleNamedBlob> staleNamedBlobs = namedBlobDb.pullStaleBlobs().get();
    assertEquals("Stale blob case 1 count does not match!", 1, staleNamedBlobs.size());
    assertEquals("Stale blob case 1 pulled out blob name does not meet expectation",
        staleNamedBlobs.get(0).getBlobName(), record.getBlobName());
    assertEquals("Stale blob case 1 pulled out blob id does not meet expectation",
        staleNamedBlobs.get(0).getBlobId(), record.getBlobId());
  }

  /**
   * Test behavior with blob cleanup for stale blob case
   * Case 2. created more than staleDataRetentionDays ago, Ready, not Largest and bigger version is ready
   */
  @Test
  public void testCleanupBlobStaleCase2() throws Exception {
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    calendar.add(Calendar.DATE, -config.staleDataRetentionDays);
    long staleCutoffTime = calendar.getTimeInMillis();

    Account account = accountService.getAllAccounts().iterator().next();
    Container container = account.getAllContainers().iterator().next();
    String blobId = getBlobId(account, container);
    String blobName = "stale/" + "case2"  + "/more path segments--";
    NamedBlobRecord record =
        new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId, Utils.Infinite_Time);

    time.setCurrentMilliseconds(staleCutoffTime);
    namedBlobDb.put(record,  NamedBlobState.READY, true).get();

    time.setCurrentMilliseconds(staleCutoffTime+1);
    String blobIdNew = getBlobId(account, container);
    NamedBlobRecord recordNew =
        new NamedBlobRecord(account.getName(), container.getName(), blobName, blobIdNew, Utils.Infinite_Time);
    namedBlobDb.put(recordNew,  NamedBlobState.READY, true).get();

    List<StaleNamedBlob> staleNamedBlobs = namedBlobDb.pullStaleBlobs().get();
    assertEquals("Stale blob case 2 count does not match!", 1, staleNamedBlobs.size());
    assertEquals("Stale blob case 2 pulled out blob name does not meet expectation",
        staleNamedBlobs.get(0).getBlobName(), record.getBlobName());
    assertEquals("Stale blob case 2 pulled out blob id does not meet expectation",
        staleNamedBlobs.get(0).getBlobId(), record.getBlobId());
  }


  /**
   * Test behavior with blob cleanup for good blob case
   * Case 1, created less than staleDataRetentionDays ago, InProgress, is Largest (Don't care about ttl and delete)
   */
  @Test
  public void testCleanupBlobGoodCase1() throws Exception {
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    calendar.add(Calendar.DATE, -config.staleDataRetentionDays);
    long afterStaleCutoffTime = calendar.getTimeInMillis() + 5000;

    Account account = accountService.getAllAccounts().iterator().next();
    Container container = account.getAllContainers().iterator().next();
    String blobId = getBlobId(account, container);
    String blobName = "good/" + "case1" + "/more path segments--";
    NamedBlobRecord record =
        new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId, Utils.Infinite_Time);

    time.setCurrentMilliseconds(afterStaleCutoffTime + 5000);
    namedBlobDb.put(record, NamedBlobState.IN_PROGRESS, true).get();
    List<StaleNamedBlob> staleNamedBlobs = namedBlobDb.pullStaleBlobs().get();
    assertTrue("Good blob case 1 pull stale blob result should be empty!", staleNamedBlobs.isEmpty());
  }

  /**
   * Test behavior with blob cleanup for good blob case
   * Case 2, created less than staleDataRetentionDays ago, Ready, not largest, and bigger version is ready
   */
  @Test
  public void testCleanupBlobGoodCase2() throws Exception {
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    calendar.add(Calendar.DATE, -config.staleDataRetentionDays);
    long afterStaleCutoffTime = calendar.getTimeInMillis() + 5000;

    Account account = accountService.getAllAccounts().iterator().next();
    Container container = account.getAllContainers().iterator().next();
    String blobId = getBlobId(account, container);
    String blobName = "good/" + "case2" + "/more path segments--";
    NamedBlobRecord record =
        new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId, Utils.Infinite_Time);

    time.setCurrentMilliseconds(afterStaleCutoffTime);
    namedBlobDb.put(record, NamedBlobState.READY, true).get();

    time.setCurrentMilliseconds(afterStaleCutoffTime+1);
    String blobIdNew = getBlobId(account, container);
    NamedBlobRecord recordNew =
        new NamedBlobRecord(account.getName(), container.getName(), blobName, blobIdNew, Utils.Infinite_Time);
    namedBlobDb.put(recordNew,  NamedBlobState.READY, true).get();

    List<StaleNamedBlob> staleNamedBlobs = namedBlobDb.pullStaleBlobs().get();
    assertTrue("Good blob case 2 pull stale blob result should be empty!", staleNamedBlobs.isEmpty());
  }

  /**
   * Test behavior with blob cleanup for good blob case
   * Case 3, created more than staleDataRetentionDays ago, Ready, is Largest
   */
  @Test
  public void testCleanupBlobGoodCase3() throws Exception {
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    calendar.add(Calendar.DATE, -config.staleDataRetentionDays);
    long staleCutoffTime = calendar.getTimeInMillis();

    Account account = accountService.getAllAccounts().iterator().next();
    Container container = account.getAllContainers().iterator().next();
    String blobId = getBlobId(account, container);
    String blobName = "good/" + "case3" + "/more path segments--";
    NamedBlobRecord record =
        new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId, Utils.Infinite_Time);

    time.setCurrentMilliseconds(staleCutoffTime);
    namedBlobDb.put(record, NamedBlobState.READY, true).get();

    List<StaleNamedBlob> staleNamedBlobs = namedBlobDb.pullStaleBlobs().get();
    assertTrue("Good blob case 3 pull stale blob result should be empty!", staleNamedBlobs.isEmpty());
  }


  /**
   * Test behavior with blob cleanup for good blob case
   * Case 4, created more than staleDataRetentionDays ago, Ready, not Largest, but bigger version is InProgress.
   */
  @Test
  public void testCleanupBlobGoodCase4() throws Exception {
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    calendar.add(Calendar.DATE, -config.staleDataRetentionDays);
    long staleCutoffTime = calendar.getTimeInMillis();

    Account account = accountService.getAllAccounts().iterator().next();
    Container container = account.getAllContainers().iterator().next();
    String blobId = getBlobId(account, container);
    String blobName = "good/" + "case4" + "/more path segments--";
    NamedBlobRecord record =
        new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId, Utils.Infinite_Time);

    time.setCurrentMilliseconds(staleCutoffTime);
    namedBlobDb.put(record, NamedBlobState.READY, true).get();

    time.setCurrentMilliseconds(staleCutoffTime+5000);
    String blobIdNew = getBlobId(account, container);
    NamedBlobRecord recordNew =
        new NamedBlobRecord(account.getName(), container.getName(), blobName, blobIdNew, Utils.Infinite_Time);
    namedBlobDb.put(recordNew,  NamedBlobState.IN_PROGRESS, true).get();

    List<StaleNamedBlob> staleNamedBlobs = namedBlobDb.pullStaleBlobs().get();
    assertTrue("Good blob case 4 pull stale blob result should be empty!", staleNamedBlobs.isEmpty());
  }

  /**
   * Test behavior with blob cleanup for good blob case
   * Case 5, two rows created for the same blobId more than staleDataRetentionDays ago,
   * one is In Progress, the other is Ready & Largest
   */
  @Test
  public void testCleanupBlobGoodCase5() throws Exception {
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    calendar.add(Calendar.DATE, -config.staleDataRetentionDays);
    long staleCutoffTime = calendar.getTimeInMillis();

    Account account = accountService.getAllAccounts().iterator().next();
    Container container = account.getAllContainers().iterator().next();
    String blobId = getBlobId(account, container);
    String blobName = "good/" + "case3" + "/more path segments--";
    NamedBlobRecord record =
        new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId, Utils.Infinite_Time);

    time.setCurrentMilliseconds(staleCutoffTime);
    namedBlobDb.put(record, NamedBlobState.IN_PROGRESS, false).get();
    namedBlobDb.put(record, NamedBlobState.READY, true).get();

    List<StaleNamedBlob> staleNamedBlobs = namedBlobDb.pullStaleBlobs().get();
    final Set<String> validBlobIds = namedBlobDb.pullValidBlobIds().get();

    assertEquals("Good blob case 5 pull stale blob result should be empty!", 0,
        staleNamedBlobs.stream().filter(s -> !validBlobIds.contains(s.getBlobId())).count());
  }

  /**
   * Get a sample blob ID.
   * @param account the account of the blob.
   * @param container the container of the blob.
   * @return the base64 blob ID.
   */
  private String getBlobId(Account account, Container container) {
    return new BlobId(BlobId.BLOB_ID_V6, BlobId.BlobIdType.NATIVE, (byte) 0, account.getId(), container.getId(),
        partitionId, false, BlobId.BlobDataType.SIMPLE).getID();
  }

  /**
   * @param callable an async call, where the {@link Future} is expected to be completed with an exception.
   * @param errorCode the expected {@link RestServiceErrorCode}.
   */
  private void checkErrorCode(Callable<Future<?>> callable, RestServiceErrorCode errorCode) throws Exception {
    TestUtils.assertException(ExecutionException.class, () -> callable.call().get(), e -> {
      RestServiceException rse = (RestServiceException) e.getCause();
      assertEquals("Unexpected error code for get after delete", errorCode, rse.getErrorCode());
    });
  }

  /**
   * Empties the accounts and containers tables.
   * @throws SQLException throw any SQL related exception
   */
  private void cleanup() throws SQLException {
    for (DataSource dataSource : namedBlobDb.getDataSources().values()) {
      try (Connection connection = dataSource.getConnection()) {
        try (Statement statement = connection.createStatement()) {
          statement.executeUpdate("DELETE FROM named_blobs_v2");
        }
      }
    }
  }

  private void checkRecordsEqual(NamedBlobRecord record1, NamedBlobRecord record2) {
    assertEquals("AccountName mismatch", record1.getAccountName(), record2.getAccountName());
    assertEquals("ContainerName mismatch", record1.getContainerName(), record2.getContainerName());
    assertEquals("BlobName mismatch", record1.getBlobName(), record2.getBlobName());
    assertEquals("BlobId mismatch", record1.getBlobId(), record2.getBlobId());
  }
}
