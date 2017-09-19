/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.github.ambry.account.Account.*;
import static com.github.ambry.account.Container.*;
import static org.junit.Assert.*;


/**
 * Tests for {@link IndexValue}.
 */
@RunWith(Parameterized.class)
public class IndexValueTest {
  private final short version;

  /**
   * Running for {@link PersistentIndex#VERSION_0}, {@link PersistentIndex#VERSION_1} and
   * {@link PersistentIndex#VERSION_2}
   * @return an array with versions ({@link PersistentIndex#VERSION_0}, {@link PersistentIndex#VERSION_1} and
   * {@link PersistentIndex#VERSION_2}).
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(
        new Object[][]{{PersistentIndex.VERSION_0}, {PersistentIndex.VERSION_1}, {PersistentIndex.VERSION_2}});
  }

  /**
   * Creates a temporary directory and sets up metrics.
   * @throws IOException
   */
  public IndexValueTest(short version) throws IOException {
    this.version = version;
  }

  /**
   * Tests an {@link IndexValue} that is representative of a PUT index entry value.
   */
  @Test
  public void putValueTest() throws InterruptedException {
    long pos = Utils.getRandomLong(TestUtils.RANDOM, 1000);
    long gen = Utils.getRandomLong(TestUtils.RANDOM, 1000);
    String logSegmentName = LogSegmentNameHelper.getName(pos, gen);
    long size = Utils.getRandomLong(TestUtils.RANDOM, 1000);
    long offset = Utils.getRandomLong(TestUtils.RANDOM, 1000);
    long operationTimeAtMs = Utils.getRandomLong(TestUtils.RANDOM, 1000000) + SystemTime.getInstance().milliseconds();
    long expectedOperationTimeV1 = Utils.getTimeInMsToTheNearestSec(operationTimeAtMs);
    short accountId = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId = Utils.getRandomShort(TestUtils.RANDOM);

    Map<Long, Long> expirationTimes = new HashMap<Long, Long>();
    // random value
    long expirationTimeAtMs = Utils.getRandomLong(TestUtils.RANDOM, 1000000) + SystemTime.getInstance().milliseconds();
    expirationTimes.put(expirationTimeAtMs, Utils.getTimeInMsToTheNearestSec(expirationTimeAtMs));
    // no expiry
    expirationTimes.put(Utils.Infinite_Time, Utils.Infinite_Time);
    // max value -1
    expirationTimeAtMs = TimeUnit.SECONDS.toMillis(Integer.MAX_VALUE - 1);
    expirationTimes.put(expirationTimeAtMs, Utils.getTimeInMsToTheNearestSec(expirationTimeAtMs));
    // max value
    expirationTimeAtMs = TimeUnit.SECONDS.toMillis(Integer.MAX_VALUE);
    expirationTimes.put(expirationTimeAtMs, Utils.getTimeInMsToTheNearestSec(expirationTimeAtMs));
    // expiry > Integer.MAX_VALUE, expected to be -1
    expirationTimeAtMs = TimeUnit.SECONDS.toMillis((long) Integer.MAX_VALUE + 1);
    expirationTimes.put(expirationTimeAtMs, Utils.Infinite_Time);
    // expiry < 0. This is to test how negative expiration values are treated in deser path.
    expirationTimeAtMs = -1 * TimeUnit.DAYS.toMillis(1);
    expirationTimes.put(expirationTimeAtMs, Utils.getTimeInMsToTheNearestSec(expirationTimeAtMs));
    // expiry < 0. This is to test how negative expiration values are treated in deser path.
    expirationTimeAtMs = (long) Integer.MIN_VALUE;
    expirationTimes.put(expirationTimeAtMs, Utils.getTimeInMsToTheNearestSec(expirationTimeAtMs));

    for (Map.Entry<Long, Long> expirationTime : expirationTimes.entrySet()) {
      long expiresAtMs = expirationTime.getKey();
      IndexValue value =
          getIndexValue(size, new Offset(logSegmentName, offset), expiresAtMs, operationTimeAtMs, accountId,
              containerId, version);
      switch (version) {
        case PersistentIndex.VERSION_0:
          verifyIndexValue(value, logSegmentName, size, offset, false, expiresAtMs, offset, Utils.Infinite_Time,
              UNKNOWN_ACCOUNT_ID, UNKNOWN_CONTAINER_ID);
          break;
        case PersistentIndex.VERSION_1:
        case PersistentIndex.VERSION_2:
          verifyIndexValue(value, logSegmentName, size, offset, false, expirationTime.getValue(), offset,
              expectedOperationTimeV1, accountId, containerId);
          break;
      }
    }
  }

  /**
   * Tests an {@link IndexValue} that is representative of a DELETE index entry value. Tests both when the DELETE
   * value is in the same log segment and a different one.
   */
  @Test
  public void deleteRecordTest() {
    long pos = Utils.getRandomLong(TestUtils.RANDOM, 1000);
    long gen = Utils.getRandomLong(TestUtils.RANDOM, 1000);
    String logSegmentName = LogSegmentNameHelper.getName(pos, gen);
    long oldSize = Utils.getRandomLong(TestUtils.RANDOM, 1000);
    long oldOffset = Utils.getRandomLong(TestUtils.RANDOM, 1000);
    long expiresAtMs = Utils.getRandomLong(TestUtils.RANDOM, 1000000);
    long expectedExpirationTimeV1 = Utils.getTimeInMsToTheNearestSec(expiresAtMs);
    long operationTimeAtMs = Utils.getRandomLong(TestUtils.RANDOM, 1000000);
    long expectedOperationTimeV1 = Utils.getTimeInMsToTheNearestSec(operationTimeAtMs);
    short accountId = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId = Utils.getRandomShort(TestUtils.RANDOM);
    IndexValue value =
        getIndexValue(oldSize, new Offset(logSegmentName, oldOffset), expiresAtMs, operationTimeAtMs, accountId,
            containerId, version);
    long newOffset = Utils.getRandomLong(TestUtils.RANDOM, 1000);
    long newSize = Utils.getRandomLong(TestUtils.RANDOM, 1000);
    IndexValue newValue = new IndexValue(logSegmentName, value.getBytes(), version);
    // delete in the same log segment
    newValue.setFlag(IndexValue.Flags.Delete_Index);
    newValue.setNewOffset(new Offset(logSegmentName, newOffset));
    newValue.setNewSize(newSize);
    switch (version) {
      case PersistentIndex.VERSION_0:
        verifyIndexValue(newValue, logSegmentName, newSize, newOffset, true, expiresAtMs, oldOffset,
            Utils.Infinite_Time, UNKNOWN_ACCOUNT_ID, UNKNOWN_CONTAINER_ID);
        break;
      case PersistentIndex.VERSION_1:
      case PersistentIndex.VERSION_2:
        verifyIndexValue(newValue, logSegmentName, newSize, newOffset, true, expectedExpirationTimeV1, oldOffset,
            expectedOperationTimeV1, accountId, containerId);
        break;
    }

    // original message offset cleared
    newValue.clearOriginalMessageOffset();
    switch (version) {
      case PersistentIndex.VERSION_0:
        verifyIndexValue(newValue, logSegmentName, newSize, newOffset, true, expiresAtMs, -1, Utils.Infinite_Time,
            UNKNOWN_ACCOUNT_ID, UNKNOWN_CONTAINER_ID);
        break;
      case PersistentIndex.VERSION_1:
      case PersistentIndex.VERSION_2:
        verifyIndexValue(newValue, logSegmentName, newSize, newOffset, true, expectedExpirationTimeV1, -1,
            expectedOperationTimeV1, accountId, containerId);
        break;
    }

    newValue = new IndexValue(logSegmentName, value.getBytes(), version);
    String newLogSegmentName = LogSegmentNameHelper.getNextPositionName(logSegmentName);
    // delete not in the same log segment
    newValue.setFlag(IndexValue.Flags.Delete_Index);
    newValue.setNewOffset(new Offset(newLogSegmentName, newOffset));
    newValue.setNewSize(newSize);

    switch (version) {
      case PersistentIndex.VERSION_0:
        verifyIndexValue(newValue, newLogSegmentName, newSize, newOffset, true, expiresAtMs, -1, Utils.Infinite_Time,
            UNKNOWN_ACCOUNT_ID, UNKNOWN_CONTAINER_ID);
        break;
      case PersistentIndex.VERSION_1:
      case PersistentIndex.VERSION_2:
        verifyIndexValue(newValue, newLogSegmentName, newSize, newOffset, true, expectedExpirationTimeV1, -1,
            expectedOperationTimeV1, accountId, containerId);
        break;
    }
  }

  /**
   * Verifies the given {@code value} for the returns of the getters. Also verifies that an {@link IndexValue} created
   * with {@link IndexValue#getBytes()} from {@code value} exports the same data.
   * @param value the {@link IndexValue} that needs to be checked.
   * @param logSegmentName the name of the log segment containing the record for which {@code value} is the
   * {@link IndexValue}.
   * @param size the size expected in {@code value}.
   * @param offset the offset expected in {@code value}.
   * @param isDeleted the expected record type referred to by {@code value}.
   * @param expiresAtMs the expected expiration time in {@code value}.
   * @param originalMessageOffset the original message offset expected in {@code value}.
   * @param operationTimeInMs the operation time in ms
   * @param accountId the accountId of the Index value
   * @param containerId the containerId of the Index value
   */
  private void verifyIndexValue(IndexValue value, String logSegmentName, long size, long offset, boolean isDeleted,
      long expiresAtMs, long originalMessageOffset, long operationTimeInMs, short accountId, short containerId) {
    verifyGetters(value, logSegmentName, size, offset, isDeleted, expiresAtMs, originalMessageOffset, operationTimeInMs,
        accountId, containerId);
    // serialize and deserialize might change the value of expiry for version1. Any expiry value < -1 after
    // deserialization is considered invalid and expiry value is set to -1
    long expectedExpiryValue = -1;
    switch (version) {
      case PersistentIndex.VERSION_0:
        expectedExpiryValue = expiresAtMs;
        break;
      case PersistentIndex.VERSION_1:
      case PersistentIndex.VERSION_2:
        expectedExpiryValue = expiresAtMs >= 0 ? expiresAtMs : Utils.Infinite_Time;
        break;
    }
    verifyGetters(new IndexValue(logSegmentName, value.getBytes(), version), logSegmentName, size, offset, isDeleted,
        expectedExpiryValue, originalMessageOffset, operationTimeInMs, accountId, containerId);
    verifyInvalidValueSize(value, logSegmentName);
  }

  /**
   * Verifies the given {@code value} for the returns of the getters.
   * @param value the {@link IndexValue} that needs to be checked.
   * @param logSegmentName the name of the log segment containing the record for which {@code value} is the
   * {@link IndexValue}.
   * @param size the size expected in {@code value}.
   * @param offset the offset expected in {@code value}.
   * @param isDeleted the expected record type referred to by {@code value}.
   * @param expiresAtMs the expected expiration time in {@code value}.
   * @param originalMessageOffset the original message offset expected in {@code value}.
   * @param operationTimeInMs the operation time in ms
   * @param accountId the accountId of the Index value
   * @param containerId the containerId of the Index value
   */
  private void verifyGetters(IndexValue value, String logSegmentName, long size, long offset, boolean isDeleted,
      long expiresAtMs, long originalMessageOffset, long operationTimeInMs, short accountId, short containerId) {
    assertEquals("Size is not as expected", size, value.getSize());
    assertEquals("Offset is not as expected", new Offset(logSegmentName, offset), value.getOffset());
    assertEquals("Delete status not as expected", isDeleted, value.isFlagSet(IndexValue.Flags.Delete_Index));
    assertEquals("ExpiresAtMs not as expected", expiresAtMs, value.getExpiresAtMs());
    assertEquals("Operation time mismatch", operationTimeInMs, value.getOperationTimeInMs());
    assertEquals("AccountId mismatch ", accountId, value.getAccountId());
    assertEquals("ContainerId mismatch ", containerId, value.getContainerId());
    assertEquals("Original message offset not as expected", originalMessageOffset, value.getOriginalMessageOffset());
  }

  /**
   * Verifies that construction of {@link IndexValue} fails with an invalid {@link ByteBuffer} value
   * @param value the source {@link IndexValue} to contruct the bad one
   * @param logSegmentName the log segment name to be used to construct the {@link IndexValue}
   */
  private void verifyInvalidValueSize(IndexValue value, String logSegmentName) {
    int capacity = TestUtils.RANDOM.nextInt(value.getBytes().capacity());
    ByteBuffer invalidValue = ByteBuffer.allocate(capacity);
    invalidValue.put(value.getBytes().array(), 0, capacity);
    try {
      new IndexValue(logSegmentName, invalidValue, version);
      fail(
          "Contruction of IndexValue expected to fail with invalid byte buffer capacity of " + invalidValue.capacity());
    } catch (IllegalArgumentException e) {
    }
  }

  /**
   * Constructs IndexValue based on the args passed and for the given version
   * @param size the size of the blob that this index value refers to
   * @param offset the {@link Offset} in the {@link Log} where the blob that this index value refers to resides
   * @param expirationTimeInMs the expiration time in ms at which the blob expires
   * @param operationTimeInMs operation time of the entry in ms
   * @param accountId the accountId that this blob belongs to
   * @param containerId the containerId that this blob belongs to
   * @param version the version with which to construct the {@link IndexValue}
   * @return the {@link IndexValue} thus constructed
   */
  static IndexValue getIndexValue(long size, Offset offset, long expirationTimeInMs, long operationTimeInMs,
      short accountId, short containerId, short version) {
    if (version == PersistentIndex.VERSION_0) {
      return getIndexValue(size, offset, IndexValue.FLAGS_DEFAULT_VALUE, expirationTimeInMs, offset.getOffset());
    } else {
      return new IndexValue(size, offset, IndexValue.FLAGS_DEFAULT_VALUE, expirationTimeInMs, operationTimeInMs,
          accountId, containerId);
    }
  }

  /**
   * Constructs IndexValue based on another {@link IndexValue}
   * @param value the {@link IndexValue} using which to create another {@link IndexValue}
   * @param version the version with which to construct the {@link IndexValue}
   * @return the {@link IndexValue} thus constructed
   */
  static IndexValue getIndexValue(IndexValue value, short version) {
    if (version == PersistentIndex.VERSION_0) {
      return getIndexValue(value.getSize(), value.getOffset(), value.getFlags(), value.getExpiresAtMs(),
          value.getOffset().getOffset());
    } else {
      return new IndexValue(value.getSize(), value.getOffset(), value.getFlags(), value.getExpiresAtMs(),
          value.getOperationTimeInMs(), value.getAccountId(), value.getContainerId());
    }
  }

  // Instantiation of {@link IndexValue} in version {@link PersistentIndex#VERSION_0}

  /**
   * Constructs IndexValue based on the args passed in version {@link PersistentIndex#VERSION_0}
   * @param size the size of the blob that this index value refers to
   * @param offset the {@link Offset} in the {@link Log} where the blob that this index value refers to resides
   * @param expiresAtMs the expiration time in ms at which the blob expires
   * @param originalMessageOffset the original message offset where the Put record pertaining to a delete record exists
   *                              in the same log segment. Set to -1 otherwise.
   * @return the {@link IndexValue} thus constructed
   */
  static IndexValue getIndexValue(long size, Offset offset, byte flags, long expiresAtMs, long originalMessageOffset) {
    ByteBuffer value = ByteBuffer.allocate(IndexValue.INDEX_VALUE_SIZE_IN_BYTES_V0);
    value.putLong(size);
    value.putLong(offset.getOffset());
    value.put(flags);
    value.putLong(expiresAtMs);
    value.putLong(originalMessageOffset);
    value.position(0);
    return new IndexValue(offset.getName(), value, PersistentIndex.VERSION_0);
  }
}

