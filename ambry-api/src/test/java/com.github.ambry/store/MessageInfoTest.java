/*
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.store;

import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import com.github.ambry.utils.UtilsTest;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests for {@link MessageInfo}
 */
public class MessageInfoTest {

  /**
   * Tests all constructors and getters
   */
  @Test
  public void basicAllConstructorsTest() {
    short[] accountIds = {100, 101, 102, 103};
    short[] containerIds = {10, 11, 12, 13};
    boolean[] isDeletedVals = {false, true, false, true};
    boolean[] isTtlUpdatedVals = {true, false, false, true};
    Long[] crcs = {null, 100L, Long.MIN_VALUE, Long.MAX_VALUE};
    StoreKey[] keys = {new MockId(UtilsTest.getRandomString(10), accountIds[0], containerIds[0]), new MockId(
        UtilsTest.getRandomString(10), accountIds[1], containerIds[1]), new MockId(UtilsTest.getRandomString(10),
        accountIds[2], containerIds[2]), new MockId(UtilsTest.getRandomString(10), accountIds[3], containerIds[3])};
    long[] blobSizes = {1024, 2048, 4096, 8192};
    long[] times = {SystemTime.getInstance().milliseconds() + 100,
        SystemTime.getInstance().milliseconds() - 1,
        SystemTime.getInstance().milliseconds() + 300, Utils.Infinite_Time};

    for (int i = 0; i < keys.length; i++) {
      MessageInfo info =
          new MessageInfo(keys[i], blobSizes[i], isDeletedVals[i], isTtlUpdatedVals[i], times[i], crcs[i],
              accountIds[i], containerIds[i], times[i]);
      checkGetters(info, keys[i], blobSizes[i], isDeletedVals[i], isTtlUpdatedVals[i], times[i], crcs[i], accountIds[i],
          containerIds[i], times[i]);

      info = new MessageInfo(keys[i], blobSizes[i], accountIds[i], containerIds[i], times[i]);
      checkGetters(info, keys[i], blobSizes[i], false, false, Utils.Infinite_Time, null, accountIds[i], containerIds[i],
          times[i]);

      info = new MessageInfo(keys[i], blobSizes[i], isDeletedVals[i], isTtlUpdatedVals[i], times[i], accountIds[i],
          containerIds[i], times[i]);
      checkGetters(info, keys[i], blobSizes[i], isDeletedVals[i], isTtlUpdatedVals[i], times[i], null, accountIds[i],
          containerIds[i], times[i]);

      info =
          new MessageInfo(keys[i], blobSizes[i], isDeletedVals[i], isTtlUpdatedVals[i], accountIds[i], containerIds[i],
              times[i]);
      checkGetters(info, keys[i], blobSizes[i], isDeletedVals[i], isTtlUpdatedVals[i], Utils.Infinite_Time, null,
          accountIds[i], containerIds[i], times[i]);

      info = new MessageInfo(keys[i], blobSizes[i], times[i], accountIds[i], containerIds[i], times[i]);
      checkGetters(info, keys[i], blobSizes[i], false, false, times[i], null, accountIds[i], containerIds[i], times[i]);
    }
  }

  /**
   * Checks getters of {@code info}
   * @param info the {@link MessageInfo} whose props need to checked
   * @param key the expected {@link StoreKey} in {@code info}.
   * @param size the expected {@link StoreKey} in {@code info}.
   * @param isDeleted the expected deleted state in {@code info}.
   * @param isTtlUpdated the expected ttl updated state in {@code info}.
   * @param expiresAtMs the expected expiry time (ms) in {@code info}.
   * @param crc the expected crc in {@code info}.
   * @param accountId the expected account id in {@code info}.
   * @param containerId the expected container id in {@code info}.
   * @param operationTimeMs the expected operation time (ms) in {@code info}.
   */
  public static void checkGetters(MessageInfo info, StoreKey key, long size, boolean isDeleted, boolean isTtlUpdated,
      long expiresAtMs, Long crc, short accountId, short containerId, long operationTimeMs) {
    assertEquals("Key not as expected", key, info.getStoreKey());
    assertEquals("Size not as expected", size, info.getSize());
    assertEquals("Expiration time not as expected", expiresAtMs, info.getExpirationTimeInMs());
    // this works assuming that the test takes < 100 ms to execute
    assertEquals("isExpired not as expected",
        expiresAtMs != Utils.Infinite_Time && expiresAtMs < System.currentTimeMillis(), info.isExpired());
    assertEquals("isDeleted not as expected", isDeleted, info.isDeleted());
    assertEquals("isTtlUpdated not as expected", isTtlUpdated, info.isTtlUpdated());
    assertEquals("Crc not as expected", crc, info.getCrc());
    assertEquals("Account ID not as expected", accountId, info.getAccountId());
    assertEquals("Container ID not as expected", containerId, info.getContainerId());
    assertEquals("Op time not as expected", operationTimeMs, info.getOperationTimeMs());
  }
}
