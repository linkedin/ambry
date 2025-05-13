/**
 * Copyright 2025 LinkedIn Corp. All rights reserved.
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
 */
package com.github.ambry.named;

import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.config.MySqlNamedBlobDbConfig;
import com.github.ambry.frontend.Page;
import com.github.ambry.protocol.NamedBlobState;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;


@RunWith(Parameterized.class)
public class MySqlNamedBlobDbListOperationIntegrationTest extends MySqlNamedBlobDbIntergrationBase {

  @Parameterized.Parameters
  public static List<Object[]> data() {
    //@formatter:off
    return Arrays.asList(new Object[][]{
        {false, MySqlNamedBlobDbConfig.MIN_LIST_NAMED_BLOBS_SQL_OPTION},
        {true, MySqlNamedBlobDbConfig.MIN_LIST_NAMED_BLOBS_SQL_OPTION},
        {false, MySqlNamedBlobDbConfig.MAX_LIST_NAMED_BLOBS_SQL_OPTION},
        {true, MySqlNamedBlobDbConfig.MAX_LIST_NAMED_BLOBS_SQL_OPTION}
      });
    //@formatter:on
  }

  public MySqlNamedBlobDbListOperationIntegrationTest(boolean enableHardDelete, int listSqlOption) throws Exception {
    super(enableHardDelete, listSqlOption);
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
    final int NUM_RECORD = 100;
    ListOperationTestParam param = setupTestParamForListOperation(NUM_RECORD);
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
    ListOperationTestParam param = setupTestParamForListOperation(10);
    Account account = param.account;
    Container container = param.container;

    // There are 10 different blob names and they all have a valid base version. After base version, we will
    // 1. Add a new version with deleted_ts = -1, state = ready to first record.
    //    New version is expected in the list operation
    // 2. Add a new version with deleted_ts = now() + 1 day, state = ready to second record
    //    New version is expected in the list operation
    // 3. Add a new version with deleted_ts = -1, state = wip to third record
    //    Base version is expected in the list operation
    // 4. Add a new version with deleted_ts = now() + 1 day, state = wip to fourth record
    //    Base version is expected in the list operation
    // 5. Add a new version with deleted_ts = now() - 1 day, state = ready to fifth record
    //    Blob name is deleted, not expected from list operation
    // 6. Add a new version with deleted_ts = now() - 1 day, state = wip to sixth record
    //    Base version is expected in the list operation
    // 7. Add two new versions to seventh record, one with deleted_ts = -1, state = ready and another with deleted_ts = -1 and state = wip
    //    First new version is expected in the list operation
    // 8. Add two new versions to eighth record, both with deleted_ts = -1, state = wip
    //    Base version is expected in the list operation
    // 9. Delete ninth record
    //    Blob name is deleted, not expected from list operation
    // 10. Add a new version with deleted_ts = -1 and state = ready and then delete this blob mame
    //     Blob name is deleted, not expected from list operation

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

    Page<NamedBlobRecord> page =
        namedBlobDb.list(account.getName(), container.getName(), param.blobNamePrefix, null, null).get();
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
   * Setup test for list operation.
   * @param numberOfRecords The number of named blob record to create before test
   * @return An instance of ListOperationTestParam that carries the test parameters
   * @throws Exception
   */
  private ListOperationTestParam setupTestParamForListOperation(int numberOfRecords) throws Exception {
    Account account = accountService.getAllAccounts().iterator().next();
    Container container = account.getAllContainers().iterator().next();
    String blobNamePrefix = "testListNamedBlobsWithOrder-" + TestUtils.getRandomKey(10) + "-";
    List<NamedBlobRecord> records = new ArrayList<>();
    time.setCurrentMilliseconds(SystemTime.getInstance().milliseconds());
    for (int i = 0; i < numberOfRecords; i++) {
      String blobName = blobNamePrefix + String.format("%02d", i); // padding with 0 so we keep the order
      long deleted_ts = i % 2 == 0 ? Utils.Infinite_Time : time.milliseconds() + TimeUnit.HOURS.toMillis(1);
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
}
