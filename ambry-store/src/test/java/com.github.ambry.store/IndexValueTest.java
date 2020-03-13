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
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
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
        new Object[][]{{PersistentIndex.VERSION_0}, {PersistentIndex.VERSION_1}, {PersistentIndex.VERSION_2},
            {PersistentIndex.VERSION_3}});
  }

  /**
   * Creates a temporary directory and sets up metrics.
   * @throws IOException
   */
  public IndexValueTest(short version) {
    this.version = version;
  }

  /**
   * Tests an {@link IndexValue} that is representative of a PUT index entry value.
   */
  @Test
  public void putValueTest() {
    long pos = Utils.getRandomLong(TestUtils.RANDOM, 1000);
    long gen = Utils.getRandomLong(TestUtils.RANDOM, 1000);
    String logSegmentName = LogSegmentNameHelper.getName(pos, gen);
    long size = Utils.getRandomLong(TestUtils.RANDOM, 1000);
    long offset = Utils.getRandomLong(TestUtils.RANDOM, 1000);
    long operationTimeAtMs = Utils.getRandomLong(TestUtils.RANDOM, 1000000) + SystemTime.getInstance().milliseconds();
    long expectedOperationTimeV1 = Utils.getTimeInMsToTheNearestSec(operationTimeAtMs);
    short accountId = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId = Utils.getRandomShort(TestUtils.RANDOM);
    short lifeVersion = (short) 0;

    List<Long> testExpiresAtMs = new ArrayList<>();
    // random value
    long expirationTimeAtMs = Utils.getRandomLong(TestUtils.RANDOM, 1000000) + SystemTime.getInstance().milliseconds();
    testExpiresAtMs.add(expirationTimeAtMs);
    // no expiry
    testExpiresAtMs.add(Utils.Infinite_Time);
    // max value -1
    expirationTimeAtMs = TimeUnit.SECONDS.toMillis(Integer.MAX_VALUE - 1);
    testExpiresAtMs.add(expirationTimeAtMs);
    // max value
    expirationTimeAtMs = TimeUnit.SECONDS.toMillis(Integer.MAX_VALUE);
    testExpiresAtMs.add(expirationTimeAtMs);
    // expiry > Integer.MAX_VALUE, expected to be -1
    expirationTimeAtMs = TimeUnit.SECONDS.toMillis((long) Integer.MAX_VALUE + 1);
    testExpiresAtMs.add(expirationTimeAtMs);
    // expiry < 0. This is to test how negative expiration values are treated in deser path.
    expirationTimeAtMs = -1 * TimeUnit.DAYS.toMillis(1);
    testExpiresAtMs.add(expirationTimeAtMs);
    // expiry < 0. This is to test how negative expiration values are treated in deser path.
    expirationTimeAtMs = (long) Integer.MIN_VALUE;
    testExpiresAtMs.add(expirationTimeAtMs);

    for (long expiresAtMs : testExpiresAtMs) {
      IndexValue value =
          getIndexValue(size, new Offset(logSegmentName, offset), expiresAtMs, operationTimeAtMs, accountId,
              containerId, lifeVersion, version);
      verifyIndexValue(value, logSegmentName, size, offset, EnumSet.noneOf(IndexValue.Flags.class), expiresAtMs, offset,
          expectedOperationTimeV1, accountId, containerId, lifeVersion);
    }
  }

  /**
   * Tests an {@link IndexValue} that is representative of a update index entry value. Tests both when the update
   * value is in the same log segment and a different one.
   */
  @Test
  public void updateRecordTest() {
    long pos = Utils.getRandomLong(TestUtils.RANDOM, 1000);
    long gen = Utils.getRandomLong(TestUtils.RANDOM, 1000);
    String logSegmentName = LogSegmentNameHelper.getName(pos, gen);
    long size = Utils.getRandomLong(TestUtils.RANDOM, 1000);
    long offset = Utils.getRandomLong(TestUtils.RANDOM, 1000);
    long expiresAtMs = Utils.getRandomLong(TestUtils.RANDOM, 1000000);
    long operationTimeAtMs = Utils.getRandomLong(TestUtils.RANDOM, 1000000);
    long expectedOperationTimeMs = Utils.getTimeInMsToTheNearestSec(operationTimeAtMs);
    short accountId = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId = Utils.getRandomShort(TestUtils.RANDOM);
    short lifeVersion = (short) 0;
    IndexValue value =
        getIndexValue(size, new Offset(logSegmentName, offset), expiresAtMs, operationTimeAtMs, accountId, containerId,
            lifeVersion, version);
    long originalMsgOffset = Utils.getRandomLong(TestUtils.RANDOM, 1000);
    long originalMsgSize = Utils.getRandomLong(TestUtils.RANDOM, 1000);

    long expectedOffset;
    long expectedSize;
    long expectedOriginalMsgOffset;
    for (IndexValue.Flags flag : IndexValue.Flags.values()) {
      // update in the same log segment
      IndexValue newValue = new IndexValue(logSegmentName, value.getBytes(), version);
      newValue.setFlag(flag);

      Offset originalMsgOff = new Offset(logSegmentName, originalMsgOffset);
      if (flag.equals(IndexValue.Flags.Delete_Index)) {
        expectedOffset = originalMsgOffset;
        expectedSize = originalMsgSize;
        expectedOriginalMsgOffset = offset;

        newValue.setNewOffset(originalMsgOff);
        newValue.setNewSize(originalMsgSize);
      } else {
        expectedOffset = offset;
        expectedSize = size;
        expectedOriginalMsgOffset = originalMsgOffset;

        newValue.setOriginalMessageOffset(originalMsgOff);
      }
      verifyIndexValue(newValue, logSegmentName, expectedSize, expectedOffset, EnumSet.of(flag), expiresAtMs,
          expectedOriginalMsgOffset, expectedOperationTimeMs, accountId, containerId, lifeVersion);

      // original message offset cleared
      newValue.clearOriginalMessageOffset();
      verifyIndexValue(newValue, logSegmentName, expectedSize, expectedOffset, EnumSet.of(flag), expiresAtMs,
          IndexValue.UNKNOWN_ORIGINAL_MESSAGE_OFFSET, expectedOperationTimeMs, accountId, containerId, lifeVersion);

      newValue = new IndexValue(logSegmentName, value.getBytes(), version);
      String newLogSegmentName = LogSegmentNameHelper.getNextPositionName(logSegmentName);
      String expectedLogSegmentName;
      // update not in the same log segment
      newValue.setFlag(flag);
      originalMsgOff = new Offset(newLogSegmentName, originalMsgOffset);
      if (flag.equals(IndexValue.Flags.Delete_Index)) {
        expectedLogSegmentName = newLogSegmentName;
        // no need to set the other "expected" because they have already been set
        newValue.setNewOffset(originalMsgOff);
        newValue.setNewSize(originalMsgSize);
      } else {
        expectedLogSegmentName = logSegmentName;
        newValue.setOriginalMessageOffset(originalMsgOff);
      }
      verifyIndexValue(newValue, expectedLogSegmentName, expectedSize, expectedOffset, EnumSet.of(flag), expiresAtMs,
          IndexValue.UNKNOWN_ORIGINAL_MESSAGE_OFFSET, expectedOperationTimeMs, accountId, containerId, lifeVersion);
    }

    // clearFlag() test
    IndexValue newValue = new IndexValue(logSegmentName, value.getBytes(), version);
    // set two flags
    newValue.setFlag(IndexValue.Flags.Ttl_Update_Index);
    newValue.setFlag(IndexValue.Flags.Delete_Index);
    verifyIndexValue(newValue, logSegmentName, size, offset,
        EnumSet.of(IndexValue.Flags.Ttl_Update_Index, IndexValue.Flags.Delete_Index), expiresAtMs, offset,
        expectedOperationTimeMs, accountId, containerId, lifeVersion);
    // clear the delete flag
    newValue.clearFlag(IndexValue.Flags.Delete_Index);
    verifyIndexValue(newValue, logSegmentName, size, offset, EnumSet.of(IndexValue.Flags.Ttl_Update_Index), expiresAtMs,
        offset, expectedOperationTimeMs, accountId, containerId, lifeVersion);
    // clear again (to check that it does not cause problems)
    newValue.clearFlag(IndexValue.Flags.Delete_Index);
    // clear the ttl update flag
    newValue.clearFlag(IndexValue.Flags.Ttl_Update_Index);
    verifyIndexValue(newValue, logSegmentName, size, offset, EnumSet.noneOf(IndexValue.Flags.class), expiresAtMs,
        offset, expectedOperationTimeMs, accountId, containerId, lifeVersion);
  }

  /**
   * Verifies the given {@code value} for the returns of the getters. Also verifies that an {@link IndexValue} created
   * with {@link IndexValue#getBytes()} from {@code value} exports the same data.
   * @param value the {@link IndexValue} that needs to be checked.
   * @param logSegmentName the name of the log segment containing the record for which {@code value} is the
   * {@link IndexValue}.
   * @param size the size expected in {@code value}.
   * @param offset the offset expected in {@code value}.
   * @param flags the expected flags in {@code value}.
   * @param expiresAtMs the expected expiration time in {@code value}.
   * @param originalMessageOffset the original message offset expected in {@code value}.
   * @param operationTimeInMs the operation time in ms
   * @param accountId the accountId of the Index value
   * @param containerId the containerId of the Index value
   * @param lifeVersion the update version of the Index value
   */
  private void verifyIndexValue(IndexValue value, String logSegmentName, long size, long offset,
      EnumSet<IndexValue.Flags> flags, long expiresAtMs, long originalMessageOffset, long operationTimeInMs,
      short accountId, short containerId, short lifeVersion) {
    switch (version) {
      case PersistentIndex.VERSION_0:
        operationTimeInMs = Utils.Infinite_Time;
        accountId = UNKNOWN_ACCOUNT_ID;
        containerId = UNKNOWN_CONTAINER_ID;
        break;
      case PersistentIndex.VERSION_1:
      case PersistentIndex.VERSION_2:
      case PersistentIndex.VERSION_3:
        expiresAtMs = expiresAtMs >= 0 ? Utils.getTimeInMsToTheNearestSec(expiresAtMs) : Utils.Infinite_Time;
        expiresAtMs =
            TimeUnit.MILLISECONDS.toSeconds(expiresAtMs) > Integer.MAX_VALUE ? Utils.Infinite_Time : expiresAtMs;
        break;
    }
    verifyGetters(value, logSegmentName, size, offset, flags, expiresAtMs, originalMessageOffset, operationTimeInMs,
        accountId, containerId, lifeVersion);
    // check value after ser-deser
    verifyGetters(new IndexValue(logSegmentName, value.getBytes(), version), logSegmentName, size, offset, flags,
        expiresAtMs, originalMessageOffset, operationTimeInMs, accountId, containerId, lifeVersion);
    verifyInvalidValueSize(value, logSegmentName);
  }

  /**
   * Verifies the given {@code value} for the returns of the getters.
   * @param value the {@link IndexValue} that needs to be checked.
   * @param logSegmentName the name of the log segment containing the record for which {@code value} is the
   * {@link IndexValue}.
   * @param size the size expected in {@code value}.
   * @param offset the offset expected in {@code value}.
   * @param flags the flags set in {@code value}.
   * @param expiresAtMs the expected expiration time in {@code value}.
   * @param originalMessageOffset the original message offset expected in {@code value}.
   * @param operationTimeInMs the operation time in ms
   * @param accountId the accountId of the Index value
   * @param containerId the containerId of the Index value
   * @param lifeVersion the update version of the Index value
   */
  private void verifyGetters(IndexValue value, String logSegmentName, long size, long offset,
      EnumSet<IndexValue.Flags> flags, long expiresAtMs, long originalMessageOffset, long operationTimeInMs,
      short accountId, short containerId, short lifeVersion) {
    assertEquals("Version is not as expected", version, value.getFormatVersion());
    assertEquals("Size is not as expected", size, value.getSize());
    assertEquals("Offset is not as expected", new Offset(logSegmentName, offset), value.getOffset());
    for (IndexValue.Flags flagToCheck : IndexValue.Flags.values()) {
      assertEquals("Flag " + flagToCheck + " status not as expected", flags.contains(flagToCheck),
          value.isFlagSet(flagToCheck));
    }
    assertEquals("ExpiresAtMs not as expected", expiresAtMs, value.getExpiresAtMs());
    assertEquals("Operation time mismatch", operationTimeInMs, value.getOperationTimeInMs());
    assertEquals("AccountId mismatch ", accountId, value.getAccountId());
    assertEquals("ContainerId mismatch ", containerId, value.getContainerId());
    assertEquals("Original message offset not as expected", originalMessageOffset, value.getOriginalMessageOffset());
    assertEquals("UpdateVersion mismatch", lifeVersion, value.getLifeVersion());
  }

  /**
   * Verifies that construction of {@link IndexValue} fails with an invalid {@link ByteBuffer} value
   * @param value the source {@link IndexValue} to construct the bad one
   * @param logSegmentName the log segment name to be used to construct the {@link IndexValue}
   */
  private void verifyInvalidValueSize(IndexValue value, String logSegmentName) {
    int capacity = TestUtils.RANDOM.nextInt(value.getBytes().capacity());
    ByteBuffer invalidValue = ByteBuffer.allocate(capacity);
    invalidValue.put(value.getBytes().array(), 0, capacity);
    try {
      new IndexValue(logSegmentName, invalidValue, version);
      fail("Construction of IndexValue expected to fail with invalid byte buffer capacity of "
          + invalidValue.capacity());
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
   * @param lifeVersion the update version of the Index value
   * @param formatVersion the format version with which to construct the {@link IndexValue}
   * @return the {@link IndexValue} thus constructed
   */
  static IndexValue getIndexValue(long size, Offset offset, long expirationTimeInMs, long operationTimeInMs,
      short accountId, short containerId, short lifeVersion, short formatVersion) {
    return getIndexValue(size, offset, IndexValue.FLAGS_DEFAULT_VALUE, expirationTimeInMs, offset.getOffset(),
        operationTimeInMs, accountId, containerId, lifeVersion, formatVersion);
  }

  /**
   * Constructs IndexValue based on another {@link IndexValue}
   * @param value the {@link IndexValue} using which to create another {@link IndexValue}
   * @param version the version with which to construct the {@link IndexValue}
   * @return the {@link IndexValue} thus constructed
   */
  static IndexValue getIndexValue(IndexValue value, short version) {
    return getIndexValue(value.getSize(), value.getOffset(), value.getFlags(), value.getExpiresAtMs(),
        value.getOriginalMessageOffset(), value.getOperationTimeInMs(), value.getAccountId(), value.getContainerId(),
        (short) 0, version);
  }

  // Instantiation of {@link IndexValue} in version {@link PersistentIndex#VERSION_0}

  /**
   * Constructs IndexValue based on the args passed in version {@link PersistentIndex#VERSION_0}
   * @param size the size of the blob that this index value refers to
   * @param offset the {@link Offset} in the {@link Log} where the blob that this index value refers to resides
   * @param expiresAtMs the expiration time in ms at which the blob expires
   * @param originalMessageOffset the original message offset
   * @param operationTimeMs the time of the operation in ms (irrelevant to V0)
   * @param accountId the account that the blob represented by this value belongs to (irrelevant to V0)
   * @param containerId the container that the blob represented by this value belongs to (irrelevant to V0)
   * @param lifeVersion the update version of the Index value
   * @return the {@link IndexValue} thus constructed
   */
  static IndexValue getIndexValue(long size, Offset offset, byte flags, long expiresAtMs, long originalMessageOffset,
      long operationTimeMs, short accountId, short containerId, short lifeVersion, short persistentIndexVersion) {
    IndexValue indexValue;
    switch (persistentIndexVersion) {
      case PersistentIndex.VERSION_0:
        ByteBuffer value = ByteBuffer.allocate(IndexValue.INDEX_VALUE_SIZE_IN_BYTES_V0);
        value.putLong(size);
        value.putLong(offset.getOffset());
        value.put(flags);
        value.putLong(expiresAtMs);
        value.putLong(originalMessageOffset);
        value.position(0);
        indexValue = new IndexValue(offset.getName(), value, PersistentIndex.VERSION_0);
        break;
      case PersistentIndex.VERSION_1:
      case PersistentIndex.VERSION_2:
        value = ByteBuffer.allocate(IndexValue.INDEX_VALUE_SIZE_IN_BYTES_V1_V2);
        value.putLong(size);
        value.putLong(offset.getOffset());
        value.put(flags);
        value.putInt(expiresAtMs != Utils.Infinite_Time ? (int) (expiresAtMs / Time.MsPerSec) : (int) expiresAtMs);
        value.putLong(originalMessageOffset);
        value.putInt(
            operationTimeMs != Utils.Infinite_Time ? (int) (operationTimeMs / Time.MsPerSec) : (int) operationTimeMs);
        value.putShort(accountId);
        value.putShort(containerId);
        value.position(0);
        indexValue = new IndexValue(offset.getName(), value, persistentIndexVersion);
        break;
      case PersistentIndex.VERSION_3:
        value = ByteBuffer.allocate(IndexValue.INDEX_VALUE_SIZE_IN_BYTES_V3);
        value.putLong(size);
        value.putLong(offset.getOffset());
        value.put(flags);
        value.putInt(expiresAtMs != Utils.Infinite_Time ? (int) (expiresAtMs / Time.MsPerSec) : (int) expiresAtMs);
        value.putLong(originalMessageOffset);
        value.putInt(
            operationTimeMs != Utils.Infinite_Time ? (int) (operationTimeMs / Time.MsPerSec) : (int) operationTimeMs);
        value.putShort(accountId);
        value.putShort(containerId);
        value.putShort(lifeVersion);
        value.position(0);
        indexValue = new IndexValue(offset.getName(), value, PersistentIndex.VERSION_3);
        break;
      default:
        throw new IllegalArgumentException("Unrecognized version: " + persistentIndexVersion);
    }
    return indexValue;
  }
}

