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

import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.config.MySqlNamedBlobDbConfig;
import com.github.ambry.frontend.Page;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.NamedBlobState;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.commons.codec.binary.Base64;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.apache.commons.codec.binary.Hex;

import static java.lang.Thread.*;
import static org.junit.Assert.*;


/**
 * Integration tests for {@link MySqlNamedBlobDb}.
 */
@RunWith(Parameterized.class)
public class MySqlNamedBlobDbIntegrationTest extends MySqlNamedBlobDbIntergrationBase {

  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{false}, {true}});
  }

  public MySqlNamedBlobDbIntegrationTest(boolean enableHardDelete) throws Exception {
    super(enableHardDelete, MySqlNamedBlobDbConfig.DEFAULT_LIST_NAMED_BLOBS_SQL_OPTION);
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

    List<StaleNamedBlob> staleNamedBlobs = getStaleBlobList();
    Set<String> staleResultSet = staleNamedBlobs.stream()
        .map((s) -> String.join("|", s.getBlobName(), s.getBlobId()))
        .collect(Collectors.toSet());
    assertEquals("Stale records count does not match!", needCleanupCount, staleNamedBlobs.size());
    assertEquals("Stale records pulled out does not meet expectation", staleInputSet, staleResultSet);

    // Confirm pullStaleBlobs return empty list after cleanupStaleData is called
    Integer cleanedUpStaleCount = namedBlobDb.cleanupStaleData(staleNamedBlobs).get();
    List<StaleNamedBlob> staleNamedBlobsNew = getStaleBlobList();

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

    Account account = accountService.getAllAccounts().iterator().next();
    Container container = account.getAllContainers().iterator().next();
    String blobId = getBlobId(account, container);
    String blobName = "stale/" + "case1" + "/more path segments--";
    NamedBlobRecord record =
        new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId, Utils.Infinite_Time);

    namedBlobDb.put(record, NamedBlobState.IN_PROGRESS, true).get();
    updateModifiedTimestampByBlobName(blobName, 20);

    List<StaleNamedBlob> staleNamedBlobs = getStaleBlobList();
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
    Account account = accountService.getAllAccounts().iterator().next();
    Container container = account.getAllContainers().iterator().next();
    String blobId = getBlobId(account, container);
    String blobName = "stale/" + "case2" + "/more path segments--";
    NamedBlobRecord record =
        new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId, Utils.Infinite_Time);

    namedBlobDb.put(record, NamedBlobState.READY, true).get();
    updateModifiedTimestampByBlobName(blobName, 20);
    time.sleep(5);

    String blobIdNew = getBlobId(account, container);
    NamedBlobRecord recordNew =
        new NamedBlobRecord(account.getName(), container.getName(), blobName, blobIdNew, Utils.Infinite_Time);
    namedBlobDb.put(recordNew, NamedBlobState.READY, true).get();

    List<StaleNamedBlob> staleNamedBlobs = getStaleBlobList();
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
    Account account = accountService.getAllAccounts().iterator().next();
    Container container = account.getAllContainers().iterator().next();
    String blobId = getBlobId(account, container);
    String blobName = "good/" + "case1" + "/more path segments--";
    NamedBlobRecord record =
        new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId, Utils.Infinite_Time);

    namedBlobDb.put(record, NamedBlobState.IN_PROGRESS, true).get();
    List<StaleNamedBlob> staleNamedBlobs = getStaleBlobList();
    assertTrue("Good blob case 1 pull stale blob result should be empty!", staleNamedBlobs.isEmpty());
  }

  /**
   * Test behavior with blob cleanup for good blob case
   * Case 2, created less than staleDataRetentionDays ago, Ready, not largest, and bigger version is ready
   */
  @Test
  public void testCleanupBlobGoodCase2() throws Exception {
    Account account = accountService.getAllAccounts().iterator().next();
    Container container = account.getAllContainers().iterator().next();
    String blobId = getBlobId(account, container);
    String blobName = "good/" + "case2" + "/more path segments--";
    NamedBlobRecord record =
        new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId, Utils.Infinite_Time);

    namedBlobDb.put(record, NamedBlobState.READY, true).get();

    String blobIdNew = getBlobId(account, container);
    NamedBlobRecord recordNew =
        new NamedBlobRecord(account.getName(), container.getName(), blobName, blobIdNew, Utils.Infinite_Time);
    namedBlobDb.put(recordNew, NamedBlobState.READY, true).get();

    List<StaleNamedBlob> staleNamedBlobs = getStaleBlobList();
    assertEquals(staleNamedBlobs.size(), 1);
  }

  /**
   * Test behavior with blob cleanup for good blob case
   * Case 3, created more than staleDataRetentionDays ago, Ready, is Largest
   */
  @Test
  public void testCleanupBlobGoodCase3() throws Exception {

    Account account = accountService.getAllAccounts().iterator().next();
    Container container = account.getAllContainers().iterator().next();
    String blobId = getBlobId(account, container);
    String blobName = "good/" + "case3" + "/more path segments--";
    NamedBlobRecord record =
        new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId, Utils.Infinite_Time);

    namedBlobDb.put(record, NamedBlobState.READY, true).get();

    updateModifiedTimestampByBlobName(blobName, 20);
    List<StaleNamedBlob> staleNamedBlobs = getStaleBlobList();
    assertTrue("Good blob case 3 pull stale blob result should be empty!", staleNamedBlobs.isEmpty());
  }

  /**
   * Test behavior with blob cleanup for good blob case
   * Case 4, created more than staleDataRetentionDays ago, Ready, not Largest, but bigger version is InProgress.
   */
  @Test
  public void testCleanupBlobGoodCase4() throws Exception {
    Account account = accountService.getAllAccounts().iterator().next();
    Container container = account.getAllContainers().iterator().next();
    String blobId = getBlobId(account, container);
    String blobName = "good/" + "case4" + "/more path segments--";
    NamedBlobRecord record =
        new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId, Utils.Infinite_Time);

    namedBlobDb.put(record, NamedBlobState.READY, true).get();
    time.sleep(100);
    updateModifiedTimestampByBlobName(blobName, 20);

    String blobIdNew = getBlobId(account, container);
    NamedBlobRecord recordNew =
        new NamedBlobRecord(account.getName(), container.getName(), blobName, blobIdNew, Utils.Infinite_Time);
    namedBlobDb.put(recordNew, NamedBlobState.IN_PROGRESS, true).get();

    List<StaleNamedBlob> staleNamedBlobs = getStaleBlobList();
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

    List<StaleNamedBlob> staleNamedBlobs = getStaleBlobList();

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

    List<StaleNamedBlob> staleNamedBlobs = getStaleBlobList();

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

  @Test
  public void testRemovesOneOlderStaleInProgressBlob() throws Exception {
    // Arrange: create an IN_PROGRESS blob and mark it stale by updating modified_ts
    NamedBlobRecord record1 = createAndPutNamedBlob(getBlobIdFromService(), NamedBlobState.IN_PROGRESS, "new_cleaner");
    updateModifiedTimestampByBlobName("new_cleaner", 20);
    time.sleep(5);

    // Add a newer IN_PROGRESS blob with same blobId but fresh modified_ts
    NamedBlobRecord record2 = createAndPutNamedBlob(getBlobIdFromService(), NamedBlobState.IN_PROGRESS, "new_cleaner");

    List<StaleNamedBlob> staleNamedBlobs = getStaleBlobList();

    assertEquals("Should remove the older one, total 1 stale blob", 1, staleNamedBlobs.size());
    assertEquals("Blob-id from record1 should be removed", staleNamedBlobs.get(0).getBlobId(), record1.getBlobId());
  }

  @Test
  public void testRemovesTwoStaleInProgressBlobs() throws Exception {
    // Arrange: create two IN_PROGRESS blob records with different blob IDs
    NamedBlobRecord record1 = createAndPutNamedBlob("blob-id1", NamedBlobState.IN_PROGRESS, "new_cleaner");
    time.sleep(5);
    NamedBlobRecord record2 = createAndPutNamedBlob("blob-id2", NamedBlobState.IN_PROGRESS, "new_cleaner");

    // Manipulate modified timestamps to make both blobs stale (older than 21 days)
    updateModifiedTimestamp(base64BlobIdToHex(record1.getBlobId()), 22);
    updateModifiedTimestamp(base64BlobIdToHex(record2.getBlobId()), 21);

    List<StaleNamedBlob> staleNamedBlobs = getStaleBlobList();

    assertEquals("Should remove both, total 2 stale blobs", 2, staleNamedBlobs.size());
    assertEquals("Blob-id from record1 should be removed", staleNamedBlobs.get(1).getBlobId(), record1.getBlobId());
    assertEquals("Blob-id from record2 should be removed", staleNamedBlobs.get(0).getBlobId(), record2.getBlobId());
  }

  @Test
  public void testRemovesStaleInProgressBlobsAddedBeforeAndAfter() throws Exception {
    // Arrange: put one IN_PROGRESS record, then update its timestamp to stale (20 days ago)
    NamedBlobRecord record1 = createAndPutNamedBlob("blob-id1", NamedBlobState.IN_PROGRESS, "new_cleaner");
    updateModifiedTimestampByBlobName("new_cleaner", 20);
    time.sleep(5);

    // Add two more IN_PROGRESS records
    NamedBlobRecord record2 = createAndPutNamedBlob("blob-id2", NamedBlobState.IN_PROGRESS, "new_cleaner");
    time.sleep(5);
    NamedBlobRecord record3 = createAndPutNamedBlob("blob-id3", NamedBlobState.IN_PROGRESS, "new_cleaner");

    List<StaleNamedBlob> staleNamedBlobs = getStaleBlobList();

    assertEquals("Should remove both, total 2 stale blobs", 2, staleNamedBlobs.size());
    assertEquals("Blob-id from record1 should be removed", staleNamedBlobs.get(1).getBlobId(), record1.getBlobId());
    assertEquals("Blob-id from record2 should be removed", staleNamedBlobs.get(0).getBlobId(), record2.getBlobId());
  }

  @Test
  public void testIgnoresReadyBlobsWhenRemovingStale() throws Exception {
    NamedBlobRecord record1 = createAndPutNamedBlob("blob-id1", NamedBlobState.IN_PROGRESS, "new_cleaner");
    updateModifiedTimestampByBlobName("new_cleaner", 20);
    time.sleep(5);

    NamedBlobRecord record2 = createAndPutNamedBlob("blob-id2", NamedBlobState.IN_PROGRESS, "new_cleaner");
    time.sleep(5);
    NamedBlobRecord record3 = createAndPutNamedBlob("blob-id3", NamedBlobState.READY, "new_cleaner");

    List<StaleNamedBlob> staleNamedBlobs = getStaleBlobList();

    assertEquals("Should remove both, total 2 stale blobs", 2, staleNamedBlobs.size());
    assertEquals("Blob-id from record1 should be removed", staleNamedBlobs.get(1).getBlobId(), record1.getBlobId());
    assertEquals("Blob-id from record2 should be removed", staleNamedBlobs.get(0).getBlobId(), record2.getBlobId());
  }

  @Test
  public void testRemovesOneStaleBlobWithSameBlobIdDifferentStates() throws Exception {
    String blobId = getBlobIdFromService();
    NamedBlobRecord record1 = createAndPutNamedBlob(blobId, NamedBlobState.IN_PROGRESS, "new_cleaner");
    time.sleep(5);
    NamedBlobRecord record2 = createAndPutNamedBlob(blobId, NamedBlobState.READY, "new_cleaner");
    time.sleep(5);
    NamedBlobRecord record3 = createAndPutNamedBlob(blobId, NamedBlobState.IN_PROGRESS, "new_cleaner");

    List<StaleNamedBlob> staleNamedBlobs = getStaleBlobList();

    assertEquals("Should remove both, total 1 stale blob", 1, staleNamedBlobs.size());
    assertEquals("Blob-id from record1 should be removed", staleNamedBlobs.get(0).getBlobId(), record1.getBlobId());
  }

  @Test
  public void testRemovesReadyAndInProgressStaleBlobs() throws Exception {
    // Arrange: three blobs with mixed states (READY and IN_PROGRESS)
    NamedBlobRecord record1 = createAndPutNamedBlob("blob-id1", NamedBlobState.READY, "new_cleaner");
    time.sleep(5);
    NamedBlobRecord record2 = createAndPutNamedBlob("blob-id2", NamedBlobState.IN_PROGRESS, "new_cleaner");
    time.sleep(5);
    NamedBlobRecord record3 = createAndPutNamedBlob("blob-id3", NamedBlobState.READY, "new_cleaner");

    List<StaleNamedBlob> staleNamedBlobs = getStaleBlobList();

    assertEquals("Should remove both, total 2 stale blobs", 2, staleNamedBlobs.size());
    assertEquals("Blob-id from record1 should be removed", staleNamedBlobs.get(1).getBlobId(), record1.getBlobId());
    assertEquals("Blob-id from record2 should be removed", staleNamedBlobs.get(0).getBlobId(), record2.getBlobId());

  }

  /**
   * Helper method to create and put a NamedBlobRecord with given blobId and state.
   */
  private NamedBlobRecord createAndPutNamedBlob(String blobId, NamedBlobState state, String blobName) throws Exception {
    Account account = accountService.getAllAccounts().iterator().next();
    Container container = account.getAllContainers().iterator().next();
    NamedBlobRecord record = new NamedBlobRecord(account.getName(), container.getName(), blobName, blobId, Utils.Infinite_Time);
    namedBlobDb.put(record, state, true).get();
    return record;
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
   * Helper method to get blobId from account and container.
   */
  private String getBlobIdFromService() {
    Account account = accountService.getAllAccounts().iterator().next();
    Container container = account.getAllContainers().iterator().next();
    return getBlobId(account, container);
  }

  private void checkRecordsEqual(NamedBlobRecord record1, NamedBlobRecord record2) {
    assertEquals("AccountName mismatch", record1.getAccountName(), record2.getAccountName());
    assertEquals("ContainerName mismatch", record1.getContainerName(), record2.getContainerName());
    assertEquals("BlobName mismatch", record1.getBlobName(), record2.getBlobName());
    assertEquals("BlobId mismatch", record1.getBlobId(), record2.getBlobId());
  }

  /**
   * Helper method to update modified_ts column for a blob_id (hex string) to NOW() - interval days.
   */
  private void updateModifiedTimestamp(String hexBlobId, int daysAgo) throws Exception {
    String sql = String.format("UPDATE named_blobs_v2 SET modified_ts = NOW() - INTERVAL %d DAY WHERE blob_id = X'%s';", daysAgo, hexBlobId);
    try (Statement statement = getStatement()) {
      statement.executeUpdate(sql);
    }
  }

  /**
   * Helper method to update modified_ts column for all blobs with the given blob_name to NOW() - interval days.
   */
  private void updateModifiedTimestampByBlobName(String blobName, int daysAgo) throws Exception {
    String sql = String.format("UPDATE named_blobs_v2 SET modified_ts = NOW() - INTERVAL %d DAY WHERE blob_name = '%s';", daysAgo, blobName);
    try (Statement statement = getStatement()) {
      statement.executeUpdate(sql);
    }
  }

  /**
   * Helper method that gets a SQL Statement object from the first available DataSource in namedBlobDb.
   * This allows executing SQL queries or updates on the database.
   *
   */
  private Statement getStatement() throws Exception {
    DataSource dataSource = namedBlobDb.getDataSources().values().iterator().next();
    Connection conn = dataSource.getConnection();
    return conn.createStatement();
  }

  /**
   * Helper method that converts a Base64-encoded blob ID string into an uppercase hexadecimal string.
   *
   * @param base64BlobId the Base64-encoded blob ID string
   * @return the uppercase hexadecimal string representation
   */
  public static String base64BlobIdToHex(String base64BlobId) {
    byte[] decodedBytes = Base64.decodeBase64(base64BlobId);
    return Hex.encodeHexString(decodedBytes).toUpperCase();
  }

  /**
   * Helper method to run the for loop across containers to retrieve stale blobs
   */
  public List<StaleNamedBlob> getStaleBlobList() throws ExecutionException, InterruptedException {
    List<StaleNamedBlob> staleNamedBlobsList = new ArrayList<>();
    List<StaleNamedBlob> staleNamedBlobs;

    int pageIndex = 0;
    Set<Container> containers = accountService.getContainersByStatus(Container.ContainerStatus.ACTIVE);
    for (Container container : containers) {
      staleNamedBlobs = namedBlobDb.pullStaleBlobs(container,pageIndex).get();
      staleNamedBlobsList.addAll(staleNamedBlobs);
    }
    return staleNamedBlobsList;
  }
}
