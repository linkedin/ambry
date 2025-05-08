/**
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
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;


/**
 * Integration tests for {@link MySqlNamedBlobDb}.
 */
@RunWith(Parameterized.class)
public class MySqlNamedBlobDbIntegrationTest {
  private static final String LOCAL_DC = "dc1";
  // Please note that we are using mock time for time travel. Need to reset time (call setCurrentMilliseconds) when the
  // tests depends on time sequences
  private static final MockTime time = new MockTime(System.currentTimeMillis());
  private final InMemAccountService accountService;
  private final PartitionId partitionId;
  private final boolean enableHardDelete;
  private MySqlNamedBlobDbConfig config;
  private MySqlNamedBlobDb namedBlobDb;

  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{false}, {true}});
  }

  public MySqlNamedBlobDbIntegrationTest(boolean enableHardDelete) throws Exception {
    this.enableHardDelete = enableHardDelete;
    Properties properties = createProperties(MySqlNamedBlobDbConfig.DEFAULT_LIST_NAMED_BLOBS_SQL_OPTION);
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
  }

  private Properties createProperties(int listSqlOptions) throws Exception {
    Properties properties = Utils.loadPropsFromResource("mysql.properties");
    properties.setProperty(ClusterMapConfig.CLUSTERMAP_DATACENTER_NAME, LOCAL_DC);
    if (enableHardDelete) {
      properties.setProperty(MySqlNamedBlobDbConfig.ENABLE_HARD_DELETE, Boolean.toString(true));
    }
    properties.setProperty(MySqlNamedBlobDbConfig.LIST_NAMED_BLOBS_SQL_OPTION, Integer.toString(listSqlOptions));
    return properties;
  }

  @Before
  public void beforeTest() throws Exception {
    cleanup();
  }

  @After
  public void afterTest() throws Exception {
    if (namedBlobDb != null) {
      namedBlobDb.close();
    }
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
          long blobSize = 20;
          NamedBlobRecord record =
              new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId, expirationTime, 0,
                  blobSize);
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
        Page<NamedBlobRecord> page = namedBlobDb.list(account.getName(), container.getName(), "name", null, null).get();
        assertNull("No continuation token expected", page.getNextPageToken());
        assertEquals("Unexpected number of blobs in container", blobsPerContainer, page.getEntries().size());
      }
    }

    // list all records in each container.
    time.setCurrentMilliseconds(System.currentTimeMillis());
    for (Account account : accountService.getAllAccounts()) {
      for (Container container : account.getAllContainers()) {
        //page with no token
        Page<NamedBlobRecord> page = namedBlobDb.list(account.getName(), container.getName(), null, null, null).get();
        assertNull("No continuation token expected", page.getNextPageToken());
        assertEquals("Unexpected number of blobs in container", blobsPerContainer, page.getEntries().size());
        //page with token
        Page<NamedBlobRecord> pageWithToken =
            namedBlobDb.list(account.getName(), container.getName(), null, "name/4", null).get();
        assertEquals("Unexpected number of blobs in container", blobsPerContainer / 5,
            pageWithToken.getEntries().size());
        //page with maxKeys
        Page<NamedBlobRecord> pageWithMaxKey =
            namedBlobDb.list(account.getName(), container.getName(), null, null, 1).get();
        assertEquals("Unexpected number of blobs in container", blobsPerContainer / 5,
            pageWithMaxKey.getEntries().size());
        // Verify that blob size and modified ts is returned for all blobs
        for (NamedBlobRecord record : pageWithToken.getEntries()) {
          assertEquals("Mismatch in blob size", 20, record.getBlobSize());
          assertNotEquals("Modified timestamp must be present", 0L, record.getModifiedTimeMs());
        }
        for (NamedBlobRecord record : pageWithMaxKey.getEntries()) {
          assertEquals("Mismatch in blob size", 20, record.getBlobSize());
          assertNotEquals("Modified timestamp must be present", 0L, record.getModifiedTimeMs());
        }
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
      assertEquals("Expect exactly one blob version to be deleted", 1, deleteResult.getBlobVersions().size());
      assertEquals("Unexpected deleted ID", record.getBlobId(), deleteResult.getBlobVersions().get(0).getBlobId());
      assertFalse("Unexpected alreadyDeleted value", deleteResult.getBlobVersions().get(0).isAlreadyDeleted());
      time.setCurrentMilliseconds(System.currentTimeMillis());
      checkErrorCode(() -> namedBlobDb.get(record.getAccountName(), record.getContainerName(), record.getBlobName()),
          enableHardDelete ? RestServiceErrorCode.NotFound : RestServiceErrorCode.Deleted);
      if (!enableHardDelete) {
        NamedBlobRecord recordFromStore =
            namedBlobDb.get(record.getAccountName(), record.getContainerName(), record.getBlobName(),
                GetOption.Include_Deleted_Blobs, false).get();
        checkRecordsEqual(record, recordFromStore);

        recordFromStore = namedBlobDb.get(record.getAccountName(), record.getContainerName(), record.getBlobName(),
            GetOption.Include_All, false).get();
        checkRecordsEqual(record, recordFromStore);
      }
    }

    if (!enableHardDelete) {
      // deletes should be idempotent and additional delete calls should succeed
      time.setCurrentMilliseconds(System.currentTimeMillis());
      for (NamedBlobRecord record : records) {
        DeleteResult deleteResult =
            namedBlobDb.delete(record.getAccountName(), record.getContainerName(), record.getBlobName()).get();
        assertEquals("Expect exactly one blob version to be deleted", 1, deleteResult.getBlobVersions().size());
        assertEquals("Unexpected deleted ID", record.getBlobId(), deleteResult.getBlobVersions().get(0).getBlobId());
        assertTrue("Unexpected alreadyDeleted value", deleteResult.getBlobVersions().get(0).isAlreadyDeleted());
      }
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
        namedBlobDb.get(account.getName(), container.getName(), blobName, GetOption.Include_All, false).get();
    assertEquals("Record does not match expectations.", record, recordFromStore);
    recordFromStore =
        namedBlobDb.get(account.getName(), container.getName(), blobName, GetOption.Include_Expired_Blobs, false).get();
    assertEquals("Record does not match expectations.", record, recordFromStore);

    // replacement should succeed
    blobId = getBlobId(account, container);
    record = new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId, Utils.Infinite_Time);
    namedBlobDb.put(record).get();
    assertEquals("Record should have been replaced", record,
        namedBlobDb.get(account.getName(), container.getName(), blobName).get());
  }

  @Test
  public void testDeleteWithMultipleVersions() throws Exception {
    Account account = accountService.getAllAccounts().iterator().next();
    Container container = account.getAllContainers().iterator().next();
    String blobName = "testDeleteWithMultipleVersions-" + TestUtils.getRandomKey(10);

    String blobId1 = getBlobId(account, container);
    String blobId2 = getBlobId(account, container);
    String blobId3 = getBlobId(account, container);
    String blobId4 = getBlobId(account, container);

    // insert version 1 with state = ready, deletedts = -1
    NamedBlobRecord record1 =
        new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId1, Utils.Infinite_Time);
    namedBlobDb.put(record1, NamedBlobState.READY, true).get();
    // insert version 2 with state = in_progress, deletedts = -1
    time.sleep(100);
    NamedBlobRecord record2 =
        new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId2, Utils.Infinite_Time);
    namedBlobDb.put(record2, NamedBlobState.IN_PROGRESS, true).get();
    time.sleep(100);
    // insert version 3 with state = ready, deletedts = now() + 1 hour
    time.setCurrentMilliseconds(System.currentTimeMillis());
    NamedBlobRecord record3 = new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId3,
        time.milliseconds() + TimeUnit.HOURS.toMillis(1));
    namedBlobDb.put(record3, NamedBlobState.READY, true).get();
    time.sleep(100);
    // insert version 4 with state = in_progress, deletedts = now() + 1 hour
    time.setCurrentMilliseconds(System.currentTimeMillis());
    NamedBlobRecord record4 = new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId4,
        time.milliseconds() + TimeUnit.HOURS.toMillis(1));
    namedBlobDb.put(record4, NamedBlobState.IN_PROGRESS, true).get();
    time.sleep(100);
    // call get, it should return blob3
    NamedBlobRecord getRecord = namedBlobDb.get(account.getName(), container.getName(), blobName).get();
    assertEquals(record3, getRecord);

    BiFunction<DeleteResult, NamedBlobRecord[], Void> validateDeleteResult = (deleteResult, records) -> {
      assertEquals("Delete result should contain " + records.length + " records", records.length,
          deleteResult.getBlobVersions().size());
      for (int i = 0; i < records.length; i++) {
        assertEquals("BlobId mismatch", records[i].getBlobId(), deleteResult.getBlobVersions().get(i).getBlobId());
      }
      Set<String> blobIds = new HashSet<>(Arrays.asList(deleteResult.getBlobIds().split(",")));
      for (NamedBlobRecord record : records) {
        assertTrue("BlobId " + record.getBlobId() + " not found in delete result",
            blobIds.contains(record.getBlobId()));
      }
      return null;
    };

    // call delete should return all four blob ids
    DeleteResult deleteResult = namedBlobDb.delete(account.getName(), container.getName(), blobName).get();
    validateDeleteResult.apply(deleteResult, new NamedBlobRecord[]{record4, record3, record2, record1});

    // Adding new versions
    String blobId5 = getBlobId(account, container);
    String blobId6 = getBlobId(account, container);
    // insert version 5 with state = ready, deletedts = -1
    NamedBlobRecord record5 =
        new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId5, Utils.Infinite_Time);
    namedBlobDb.put(record5, NamedBlobState.READY, true).get();
    // insert version 6 with state = in_progress, deletedts = -1
    time.sleep(100);
    NamedBlobRecord record6 =
        new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId6, Utils.Infinite_Time);
    namedBlobDb.put(record6, NamedBlobState.IN_PROGRESS, true).get();
    time.sleep(100);

    deleteResult = namedBlobDb.delete(account.getName(), container.getName(), blobName).get();
    if (enableHardDelete) {
      validateDeleteResult.apply(deleteResult, new NamedBlobRecord[]{record6, record5});
    } else {
      validateDeleteResult.apply(deleteResult,
          new NamedBlobRecord[]{record6, record5, record4, record3, record2, record1});
      for (int i = 2; i < deleteResult.getBlobVersions().size(); i++) {
        assertTrue(deleteResult.getBlobVersions().get(i).isAlreadyDeleted());
      }
    }
  }

  /**
   * Test behavior with list named blob
   */
  @Test
  public void testListNamedBlobsWithStaleRecords() throws Exception {
    Iterator<Account> accountIter = accountService.getAllAccounts().iterator();
    Account a1 = accountIter.next();
    Iterator<Container> a1containerIter = a1.getAllContainers().iterator();
    Container a1c1 = a1containerIter.next();
    Container a1c2 = a1containerIter.next();
    Account a2 = accountIter.next();
    Iterator<Container> a2containerIter = a2.getAllContainers().iterator();
    Container a2c1 = a2containerIter.next();
    String blobName = "testListNamedBlobsWithStaleRecords";
    NamedBlobRecord v1, v1_other, v2, v2_other;
    Page<NamedBlobRecord> page;

    // put blob Ready and list should return the blob
    v1 = new NamedBlobRecord(a1.getName(), a1c1.getName(), blobName, getBlobId(a1, a1c1),
        Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTimeInMillis() + TimeUnit.HOURS.toMillis(1));
    v1_other = new NamedBlobRecord(a1.getName(), a1c1.getName(), blobName + "-other", getBlobId(a1, a1c1),
        Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTimeInMillis() + TimeUnit.HOURS.toMillis(1));
    // add some extra blobs in other accounts and containers for testing
    NamedBlobRecord a1c2Blob = new NamedBlobRecord(a1.getName(), a1c2.getName(), blobName, getBlobId(a1, a1c2),
        Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTimeInMillis() + TimeUnit.HOURS.toMillis(1));
    NamedBlobRecord a2c1Blob = new NamedBlobRecord(a2.getName(), a2c1.getName(), blobName, getBlobId(a2, a2c1),
        Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTimeInMillis() + TimeUnit.HOURS.toMillis(1));
    namedBlobDb.put(a1c2Blob, NamedBlobState.READY, true).get();
    namedBlobDb.put(a2c1Blob, NamedBlobState.READY, true).get();
    namedBlobDb.put(v1, NamedBlobState.READY, true).get();
    NamedBlobRecord v1_get = namedBlobDb.get(a1.getName(), a1c1.getName(), blobName).get();
    assertEquals(v1, v1_get);
    namedBlobDb.put(v1_other, NamedBlobState.READY, true).get();
    NamedBlobRecord v1_other_get = namedBlobDb.get(a1.getName(), a1c1.getName(), blobName + "-other").get();
    assertEquals(v1_other, v1_other_get);
    page = namedBlobDb.list(a1.getName(), a1c1.getName(), blobName, null, null).get();
    assertEquals(2, page.getEntries().size());
    assertEquals(v1_get, page.getEntries().get(0));
    assertEquals(v1_other_get, page.getEntries().get(1));
    time.sleep(100);

    // put blob in-progress and list should return the Ready blob
    v2 = new NamedBlobRecord(a1.getName(), a1c1.getName(), blobName, getBlobId(a1, a1c1),
        Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTimeInMillis() + TimeUnit.HOURS.toMillis(1));
    namedBlobDb.put(v2, NamedBlobState.IN_PROGRESS, true).get();
    page = namedBlobDb.list(a1.getName(), a1c1.getName(), blobName, null, null).get();
    assertEquals(2, page.getEntries().size());
    assertEquals(v1_get, page.getEntries().get(0));
    assertEquals(v1_other_get, page.getEntries().get(1));
    time.sleep(100);

    // update blob and list should return the new blob
    v2 = new NamedBlobRecord(a1.getName(), a1c1.getName(), blobName, getBlobId(a1, a1c1),
        Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTimeInMillis() + TimeUnit.HOURS.toMillis(1));
    v2_other = new NamedBlobRecord(a1.getName(), a1c1.getName(), blobName + "-other", getBlobId(a1, a1c1),
        Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTimeInMillis() + TimeUnit.HOURS.toMillis(1));
    namedBlobDb.put(v2, NamedBlobState.READY, true).get();
    namedBlobDb.put(v2_other, NamedBlobState.READY, true).get();
    page = namedBlobDb.list(a1.getName(), a1c1.getName(), blobName, null, null).get();
    assertEquals(2, page.getEntries().size());
    assertEquals(v2, page.getEntries().get(0));
    assertEquals(v2_other, page.getEntries().get(1));
    time.sleep(100);

    // delete blob and list should return empty
    namedBlobDb.delete(a1.getName(), a1c1.getName(), blobName).get();
    page = namedBlobDb.list(a1.getName(), a1c1.getName(), blobName, null, null).get();
    assertEquals("got " + page.getEntries(), 1, page.getEntries().size());
    assertEquals(v2_other, page.getEntries().get(0));
    time.sleep(100);
    namedBlobDb.delete(a1.getName(), a1c1.getName(), blobName + "-other").get();
    page = namedBlobDb.list(a1.getName(), a1c1.getName(), blobName, null, null).get();
    assertEquals(0, page.getEntries().size());
  }

  /**
   * Test behavior with list named blob
   */
  @Test
  public void testListNamedBlobs() throws Exception {
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

    time.setCurrentMilliseconds(calendar.getTimeInMillis());
    Account account = accountService.getAllAccounts().iterator().next();
    Container container = account.getAllContainers().iterator().next();

    final String blobNamePrefix = "blobNameForList";

    // Create expired records
    final long expiredTimeMs = calendar.getTimeInMillis() - TimeUnit.HOURS.toMillis(1);
    final Set<String> expiredNames = new HashSet<>(Arrays.asList("name1", "name2", "name3", "name4", "name5"));
    expiredNames.forEach(name -> {
      final NamedBlobRecord record = new NamedBlobRecord(account.getName(), container.getName(), blobNamePrefix + name,
          getBlobId(account, container), expiredTimeMs);
      try {
        namedBlobDb.put(record, NamedBlobState.READY, true).get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    // Create valid records
    final long validimeMs = calendar.getTimeInMillis() + TimeUnit.HOURS.toMillis(1);
    final Set<String> validNames = new HashSet<>(Arrays.asList("name6", "name7", "name8", "name9", "name10"));
    final Set<NamedBlobRecord> validRecords = new HashSet<>();
    validNames.forEach(name -> {
      final NamedBlobRecord record = new NamedBlobRecord(account.getName(), container.getName(), blobNamePrefix + name,
          getBlobId(account, container), validimeMs);
      validRecords.add(record);
      try {
        namedBlobDb.put(record, NamedBlobState.READY, true).get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    // List named blob should only put out valid ones without empty entries.
    Page<NamedBlobRecord> page =
        namedBlobDb.list(account.getName(), container.getName(), blobNamePrefix, null, null).get();
    assertEquals("List named blob entries should match the valid records", validRecords,
        new HashSet<>(page.getEntries()));
    assertNull("Next page token should be null", page.getNextPageToken());
  }

  /**
   * Test case for list named blobs with prefix.
   * @throws Exception
   */
  @Test
  public void testListNamedBlobsWithPrefix() throws Exception {
    for (int i = MySqlNamedBlobDbConfig.MIN_LIST_NAMED_BLOBS_SQL_OPTION;
        i <= MySqlNamedBlobDbConfig.MAX_LIST_NAMED_BLOBS_SQL_OPTION; i++) {
      time.setCurrentMilliseconds(SystemTime.getInstance().milliseconds());
      for (long deleted_ts : new long[]{Utils.Infinite_Time, time.milliseconds() + TimeUnit.HOURS.toMillis(1)}) {
        testListNamedBlobsWithPrefix(i, deleted_ts);
      }
    }
  }

  private void testListNamedBlobsWithPrefix(int listSqlOption, long deleted_ts) throws Exception {
    final int NUM_RECORD = 100;
    ListOperationTestParam param = setupTestParamForListOperation(listSqlOption, NUM_RECORD, deleted_ts);
    // Test two different case with different page sizes.
    // First page size equals to the number of records, so we expect all blob names returned in one list operation.
    // Second page size is less than the number of records, so we expect pagination.
    for (int pageSize : new int[]{NUM_RECORD, NUM_RECORD / 10}) {
      List<NamedBlobRecord> obtainedRecords = new ArrayList<>();
      Page<NamedBlobRecord> page = null;
      String token = null;
      int nextRecord = 0;
      do {
        page =
            namedBlobDb.list(param.account.getName(), param.container.getName(), param.blobNamePrefix, token, pageSize)
                .get();
        obtainedRecords.addAll(page.getEntries());
        token = page.getNextPageToken();
        if (pageSize == NUM_RECORD) {
          assertNull(token);
          break;
        } else {
          if (token != null) {
            nextRecord += pageSize;
            String expectedToken = param.blobNamePrefix + String.format("%02d", nextRecord);
            assertEquals(expectedToken, token);
          }
        }
      } while (page.getNextPageToken() != null);
      validateListResult(obtainedRecords, param.records);
    }
  }

  /**
   * Test case for list named blobs with prefix, with multiple versions on blob names
   * @throws Exception
   */
  @Test
  public void testListNamedBlobsWithPrefixWithMultipleVersions() throws Exception {
    for (int i = MySqlNamedBlobDbConfig.MIN_LIST_NAMED_BLOBS_SQL_OPTION;
        i <= MySqlNamedBlobDbConfig.MAX_LIST_NAMED_BLOBS_SQL_OPTION; i++) {
      time.setCurrentMilliseconds(SystemTime.getInstance().milliseconds());
      for (long deleted_ts : new long[]{Utils.Infinite_Time, time.milliseconds() + TimeUnit.HOURS.toMillis(1)}) {
        testListNamedBlobsWithPrefixWithMultipleVersions(i, deleted_ts);
      }
    }
  }

  private void testListNamedBlobsWithPrefixWithMultipleVersions(int listSqlOption, long deleted_ts) throws Exception {
    ListOperationTestParam param = setupTestParamForListOperation(listSqlOption, 10, deleted_ts);
    Account account = param.account;
    Container container = param.container;

    // There are 10 different blob names and they all have a valid base version. After base version, we will
    // 1. Add a new version with deleted_ts = -1, state = ready to first record.
    //    New version is expected in the list opertion
    // 2. Add a new version with deleted_ts = now() + 1 day, state = ready to second record
    //    New version is expected in the list opertion
    // 3. Add a new version with deleted_ts = -1, state = wip to third record
    //    Base version is expected in the list opertion
    // 4. Add a new version with deleted_ts = now() + 1 day, state = wip to fourth record
    //    Base version is expected in the list opertion
    // 5. Add a new version with deleted_ts = now() - 1 day, state = ready to fifth record
    //    Blob name is deleted, not expected from list opration
    // 6. Add a new version with deleted_ts = now() - 1 day, state = wip to sixth record
    //    Base version is expected in the list opertion
    // 7. Add two new versions to seventh record, one with deleted_ts = -1, state = ready and another with deleted_ts = -1 and state = wip
    //    First new version is expected in the list opertion
    // 8. Add two new versions to eighth record, both with deleted_ts = -1, state = wip
    //    Base version is expected in the list opertion
    // 9. Delete ninth record
    //    Blob name is deleted, not expected from list opration
    // 10. Add a new version with deleted_ts = -1 and state = ready and then delete this blob mame
    //     Blob name is deleted, not expected from list opration

    List<NamedBlobRecord> expectedRecords = new ArrayList<>();
    List<NamedBlobRecord> removedRecords = new ArrayList<>();
    // we need to sleep for some time and set the current time to mock time instance before put and delete so the timestamp
    // these operations saved to database is the same as the real time.

    Thread.sleep(10);
    time.setCurrentMilliseconds(SystemTime.getInstance().milliseconds());

    // Case 1
    NamedBlobRecord record = param.records.get(0);
    NamedBlobRecord newRecord =
        new NamedBlobRecord(account.getName(), container.getName(), record.getBlobName(), getBlobId(account, container),
            Utils.Infinite_Time, 0, 1024);
    namedBlobDb.put(newRecord, NamedBlobState.READY, true).get();
    expectedRecords.add(newRecord);

    // Case 2
    record = param.records.get(1);
    newRecord =
        new NamedBlobRecord(account.getName(), container.getName(), record.getBlobName(), getBlobId(account, container),
            time.milliseconds() + TimeUnit.DAYS.toMillis(1), 0, 1024);
    namedBlobDb.put(newRecord, NamedBlobState.READY, true).get();
    expectedRecords.add(newRecord);

    // Case 3
    record = param.records.get(2);
    newRecord =
        new NamedBlobRecord(account.getName(), container.getName(), record.getBlobName(), getBlobId(account, container),
            Utils.Infinite_Time, 0, 1024);
    namedBlobDb.put(newRecord, NamedBlobState.IN_PROGRESS, true).get();
    expectedRecords.add(record);

    // Case 4
    record = param.records.get(3);
    newRecord =
        new NamedBlobRecord(account.getName(), container.getName(), record.getBlobName(), getBlobId(account, container),
            time.milliseconds() + TimeUnit.DAYS.toMillis(1), 0, 1024);
    namedBlobDb.put(newRecord, NamedBlobState.IN_PROGRESS, true).get();
    expectedRecords.add(record);

    // Case 5
    record = param.records.get(4);
    newRecord =
        new NamedBlobRecord(account.getName(), container.getName(), record.getBlobName(), getBlobId(account, container),
            time.milliseconds() - TimeUnit.DAYS.toMillis(1), 0, 1024);
    namedBlobDb.put(newRecord, NamedBlobState.READY, true).get();
    removedRecords.add(record);

    // Case 6
    record = param.records.get(5);
    newRecord =
        new NamedBlobRecord(account.getName(), container.getName(), record.getBlobName(), getBlobId(account, container),
            time.milliseconds() - TimeUnit.DAYS.toMillis(1), 0, 1024);
    namedBlobDb.put(newRecord, NamedBlobState.IN_PROGRESS, true).get();
    expectedRecords.add(record);

    // Case 7
    record = param.records.get(6);
    newRecord =
        new NamedBlobRecord(account.getName(), container.getName(), record.getBlobName(), getBlobId(account, container),
            Utils.Infinite_Time, 0, 1024);
    namedBlobDb.put(newRecord, NamedBlobState.READY, true).get();
    Thread.sleep(10);
    time.setCurrentMilliseconds(SystemTime.getInstance().milliseconds());
    NamedBlobRecord secondNewRecord =
        new NamedBlobRecord(account.getName(), container.getName(), record.getBlobName(), getBlobId(account, container),
            Utils.Infinite_Time, 0, 1024);
    namedBlobDb.put(secondNewRecord, NamedBlobState.IN_PROGRESS, true).get();
    expectedRecords.add(newRecord);

    // Case 8
    record = param.records.get(7);
    newRecord =
        new NamedBlobRecord(account.getName(), container.getName(), record.getBlobName(), getBlobId(account, container),
            Utils.Infinite_Time, 0, 1024);
    namedBlobDb.put(newRecord, NamedBlobState.IN_PROGRESS, true).get();
    Thread.sleep(10);
    time.setCurrentMilliseconds(SystemTime.getInstance().milliseconds());
    newRecord =
        new NamedBlobRecord(account.getName(), container.getName(), record.getBlobName(), getBlobId(account, container),
            Utils.Infinite_Time, 0, 1024);
    namedBlobDb.put(newRecord, NamedBlobState.IN_PROGRESS, true).get();
    expectedRecords.add(record);

    // Case 9
    Thread.sleep(10);
    time.setCurrentMilliseconds(SystemTime.getInstance().milliseconds());
    record = param.records.get(8);
    namedBlobDb.delete(account.getName(), container.getName(), record.getBlobName()).get();
    removedRecords.add(record);

    // Case 10
    record = param.records.get(9);
    newRecord =
        new NamedBlobRecord(account.getName(), container.getName(), record.getBlobName(), getBlobId(account, container),
            Utils.Infinite_Time, 0, 1024);
    namedBlobDb.put(newRecord, NamedBlobState.READY, true).get();
    Thread.sleep(10);
    time.setCurrentMilliseconds(SystemTime.getInstance().milliseconds());
    namedBlobDb.delete(account.getName(), container.getName(), record.getBlobName()).get();
    removedRecords.add(record);

    Thread.sleep(100);
    time.setCurrentMilliseconds(SystemTime.getInstance().milliseconds());
    // First remove those records from the expected list and make sure get returns Delete or NotFound
    for (NamedBlobRecord removedRecord : removedRecords) {
      try {
        namedBlobDb.get(account.getName(), container.getName(), removedRecord.getBlobName()).get();
        fail("Expected blob name " + removedRecord.getBlobName() + " to be deleted");
      } catch (ExecutionException e) {
        RestServiceException rse = Utils.getRootCause(e, RestServiceException.class);
        assertNotNull(rse);
        assertTrue(
            rse.getErrorCode() == RestServiceErrorCode.NotFound || rse.getErrorCode() == RestServiceErrorCode.Deleted);
      }
    }

    Page<NamedBlobRecord> page =
        page = namedBlobDb.list(account.getName(), container.getName(), param.blobNamePrefix, null, null).get();
    assertNull(page.getNextPageToken());
    validateListResult(page.getEntries(), expectedRecords);

    for (NamedBlobRecord expectedRecord : expectedRecords) {
      NamedBlobRecord recordFromStore =
          namedBlobDb.get(account.getName(), container.getName(), expectedRecord.getBlobName()).get();
      assertEquals("List result does not match with get for blob_name " + expectedRecord.getBlobName(), expectedRecord,
          recordFromStore);
    }
  }

  /**
   * Data structure to pass some test parameters for list operation back to the test method.
   */
  static class ListOperationTestParam {
    final Account account;
    final Container container;
    final String blobNamePrefix;
    final List<NamedBlobRecord> records;

    ListOperationTestParam(Account account, Container container, String blobNamePrefix, List<NamedBlobRecord> records) {
      this.account = account;
      this.container = container;
      this.blobNamePrefix = blobNamePrefix;
      this.records = records;
    }
  }

  /**
   * Set up namedBlobDb instance based on the listSqlOption.
   * @param listSqlOption The list sql query option.
   * @throws Exception
   */
  private void setupNamedBlobDb(int listSqlOption) throws Exception {
    if (namedBlobDb != null) {
      namedBlobDb.close();
      namedBlobDb = null;
    }
    Properties properties = createProperties(listSqlOption);
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    config = new MySqlNamedBlobDbConfig(verifiableProperties);
    MySqlNamedBlobDbFactory namedBlobDbFactory =
        new MySqlNamedBlobDbFactory(verifiableProperties, new MetricRegistry(), accountService, time);
    namedBlobDb = namedBlobDbFactory.getNamedBlobDb();
  }

  /**
   * Setup test for list operation.
   * @param listSqlOption The list sql option
   * @param numberOfRecords The number of named blob record to create before test
   * @param deleted_ts The deleted_ts timestamp for each records
   * @return An instance of ListOperationTestParam that carries the test parameters
   * @throws Exception
   */
  private ListOperationTestParam setupTestParamForListOperation(int listSqlOption, int numberOfRecords, long deleted_ts)
      throws Exception {
    setupNamedBlobDb(listSqlOption);
    Account account = accountService.getAllAccounts().iterator().next();
    Container container = account.getAllContainers().iterator().next();
    String blobNamePrefix = "testListNamedBlobsWithOrder-" + TestUtils.getRandomKey(10) + "-";
    List<NamedBlobRecord> records = new ArrayList<>();
    for (int i = 0; i < numberOfRecords; i++) {
      String blobName = blobNamePrefix + String.format("%02d", i); // padding with 0 so we keep the order
      NamedBlobRecord record =
          new NamedBlobRecord(account.getName(), container.getName(), blobName, getBlobId(account, container),
              deleted_ts, 0, 1024);
      namedBlobDb.put(record, NamedBlobState.READY, true).get();
      records.add(record);
    }
    return new ListOperationTestParam(account, container, blobNamePrefix, records);
  }

  /**
   * Validate the result from list operation.
   * @param obtained the obtained list
   * @param expected the expected list
   */
  private void validateListResult(List<NamedBlobRecord> obtained, List<NamedBlobRecord> expected) {
    assertEquals("List result size does not match", expected.size(), obtained.size());
    for (int i = 0; i < expected.size(); i++) {
      assertEquals("List result does not match at index " + i, expected.get(i), obtained.get(i));
    }
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
    List<NamedBlobRecord> staleRecords = new ArrayList<>();
    List<NamedBlobRecord> records = new ArrayList<>();

    // Create stale named blob records
    for (int i = 0; i < staleCount; i++) {
      String blobId = getBlobId(account, container);
      String blobName = "stale/" + i + "/more path segments--";
      long expirationTime = i % 2 == 0 ? Utils.Infinite_Time : staleCutoffTimePlusOneMillisecond;
      NamedBlobRecord record =
          new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId, expirationTime);

      NamedBlobState blob_state =
          expirationTime == Utils.Infinite_Time ? NamedBlobState.IN_PROGRESS : NamedBlobState.READY;

      time.setCurrentMilliseconds(staleCutoffTime);
      namedBlobDb.put(record, blob_state, true).get();

      time.setCurrentMilliseconds(staleCutoffTimePlusOneMillisecond);
      String blobId2 = getBlobId(account, container);
      NamedBlobRecord record2 =
          new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId2, expirationTime);
      namedBlobDb.put(record2, NamedBlobState.READY, true).get();

      if (expirationTime == Utils.Infinite_Time) {
        needCleanupCount += 1;
      }
      staleRecords.add(record);
      records.add(record2);
    }

    Thread.sleep(100);

    // Confirm the pullStaleBlobs indeed pulled out the stale blob cases
    Set<String> staleInputSet = staleRecords.stream()
        .filter((r) -> r.getExpirationTimeMs() == Utils.Infinite_Time)
        .map((r) -> String.join("|", r.getBlobName(), r.getBlobId()))
        .collect(Collectors.toSet());

    List<StaleNamedBlob> staleNamedBlobs = namedBlobDb.pullStaleBlobs().get();
    Set<String> staleResultSet = staleNamedBlobs.stream()
        .map((s) -> String.join("|", s.getBlobName(), s.getBlobId()))
        .collect(Collectors.toSet());
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
    String blobName = "stale/" + "case1" + "/more path segments--";
    NamedBlobRecord record =
        new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId, Utils.Infinite_Time);

    time.setCurrentMilliseconds(staleCutoffTime);
    namedBlobDb.put(record, NamedBlobState.IN_PROGRESS, true).get();

    List<StaleNamedBlob> staleNamedBlobs = namedBlobDb.pullStaleBlobs().get();
    assertEquals("Stale blob case 1 count does not match!", 1, staleNamedBlobs.size());
    assertEquals("Stale blob case 1 pulled out blob name does not meet expectation",
        staleNamedBlobs.get(0).getBlobName(), record.getBlobName());
    assertEquals("Stale blob case 1 pulled out blob id does not meet expectation", staleNamedBlobs.get(0).getBlobId(),
        record.getBlobId());
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
    String blobName = "stale/" + "case2" + "/more path segments--";
    NamedBlobRecord record =
        new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId, Utils.Infinite_Time);

    time.setCurrentMilliseconds(staleCutoffTime);
    namedBlobDb.put(record, NamedBlobState.READY, true).get();

    time.setCurrentMilliseconds(staleCutoffTime + 1);
    String blobIdNew = getBlobId(account, container);
    NamedBlobRecord recordNew =
        new NamedBlobRecord(account.getName(), container.getName(), blobName, blobIdNew, Utils.Infinite_Time);
    namedBlobDb.put(recordNew, NamedBlobState.READY, true).get();

    List<StaleNamedBlob> staleNamedBlobs = namedBlobDb.pullStaleBlobs().get();
    assertEquals("Stale blob case 2 count does not match!", 1, staleNamedBlobs.size());
    assertEquals("Stale blob case 2 pulled out blob name does not meet expectation",
        staleNamedBlobs.get(0).getBlobName(), record.getBlobName());
    assertEquals("Stale blob case 2 pulled out blob id does not meet expectation", staleNamedBlobs.get(0).getBlobId(),
        record.getBlobId());
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

    time.setCurrentMilliseconds(afterStaleCutoffTime + 1);
    String blobIdNew = getBlobId(account, container);
    NamedBlobRecord recordNew =
        new NamedBlobRecord(account.getName(), container.getName(), blobName, blobIdNew, Utils.Infinite_Time);
    namedBlobDb.put(recordNew, NamedBlobState.READY, true).get();

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

    time.setCurrentMilliseconds(staleCutoffTime + 5000);
    String blobIdNew = getBlobId(account, container);
    NamedBlobRecord recordNew =
        new NamedBlobRecord(account.getName(), container.getName(), blobName, blobIdNew, Utils.Infinite_Time);
    namedBlobDb.put(recordNew, NamedBlobState.IN_PROGRESS, true).get();

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
    PutResult putResult = namedBlobDb.put(record, NamedBlobState.IN_PROGRESS, false).get();
    NamedBlobRecord updatedRecord =
        new NamedBlobRecord(record.getAccountName(), record.getContainerName(), record.getBlobName(),
            record.getBlobId(), record.getExpirationTimeMs(), putResult.getInsertedRecord().getVersion());
    namedBlobDb.updateBlobTtlAndStateToReady(updatedRecord).get();

    List<StaleNamedBlob> staleNamedBlobs = namedBlobDb.pullStaleBlobs().get();

    assertEquals("Good blob case 5 pull stale blob result should be empty!", 0, staleNamedBlobs.size());
  }

  /**
   * Test behavior with blob cleanup for good blob case
   * Case 6, one row created for a blobId more than staleDataRetentionDays ago via TTL Update process,
   * Initially it's created with IN_PROGRESS state, later its state updated to be READY
   */
  @Test
  public void testCleanupBlobGoodCase6() throws Exception {
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    calendar.add(Calendar.DATE, -config.staleDataRetentionDays);
    long staleCutoffTime = calendar.getTimeInMillis();

    Account account = accountService.getAllAccounts().iterator().next();
    Container container = account.getAllContainers().iterator().next();
    String blobId = getBlobId(account, container);
    String blobName = "good/" + "case6" + "/more path segments--";
    NamedBlobRecord record = new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId,
        staleCutoffTime + TimeUnit.HOURS.toMillis(1));

    time.setCurrentMilliseconds(staleCutoffTime);
    PutResult putResult = namedBlobDb.put(record, NamedBlobState.IN_PROGRESS, true).get();
    checkErrorCode(() -> namedBlobDb.get(record.getAccountName(), record.getContainerName(), record.getBlobName()),
        RestServiceErrorCode.NotFound);
    NamedBlobRecord updatedRecord =
        new NamedBlobRecord(record.getAccountName(), record.getContainerName(), record.getBlobName(),
            record.getBlobId(), record.getExpirationTimeMs(), putResult.getInsertedRecord().getVersion());
    PutResult updateResult = namedBlobDb.updateBlobTtlAndStateToReady(updatedRecord).get();

    List<StaleNamedBlob> staleNamedBlobs = namedBlobDb.pullStaleBlobs().get();

    NamedBlobRecord recordFromDb =
        namedBlobDb.get(updatedRecord.getAccountName(), updatedRecord.getContainerName(), updatedRecord.getBlobName())
            .get();

    assertEquals("AccountName: TTL Updated record should match with get record", recordFromDb.getAccountName(),
        updateResult.getInsertedRecord().getAccountName());
    assertEquals("ContainerName: TTL Updated record should match with get record", recordFromDb.getContainerName(),
        updateResult.getInsertedRecord().getContainerName());
    assertEquals("BlobName: TTL Updated record should match with get record", recordFromDb.getBlobName(),
        updateResult.getInsertedRecord().getBlobName());
    assertEquals("BlobId: TTL Updated record should match with get record", recordFromDb.getBlobId(),
        updateResult.getInsertedRecord().getBlobId());
    assertEquals("Version: TTL Updated record should match with get record", recordFromDb.getVersion(),
        updateResult.getInsertedRecord().getVersion());
    assertEquals("TTL Updated record should have Infinite_Time (-1) as expiration time", -1,
        recordFromDb.getExpirationTimeMs());

    assertEquals("Updated row's version should match with original put row", putResult.getInsertedRecord().getVersion(),
        updateResult.getInsertedRecord().getVersion());
    assertTrue("Good blob case 6 pull stale blob result should be empty!", staleNamedBlobs.isEmpty());
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
