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

import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;


/**
 * Tests for {@link IndexValue}.
 */
@RunWith(Parameterized.class)
public class IndexValueTest {

  final short version;
  static final short DEFAULT_SHORT_VALUE = 0;
  private final Time time = new MockTime();

  /**
   * Running for {@link PersistentIndex#VERSION_0} and {@link PersistentIndex#VERSION_1}
   * @return an array with both the versions ({@link PersistentIndex#VERSION_0} and {@link PersistentIndex#VERSION_1}).
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{PersistentIndex.VERSION_0}, {PersistentIndex.VERSION_1}});
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
  public void putValueTest() {
    long pos = Utils.getRandomLong(TestUtils.RANDOM, 1000);
    long gen = Utils.getRandomLong(TestUtils.RANDOM, 1000);
    String logSegmentName = LogSegmentNameHelper.getName(pos, gen);
    long size = Utils.getRandomLong(TestUtils.RANDOM, 1000);
    long offset = Utils.getRandomLong(TestUtils.RANDOM, 1000);
    long expiresAtMs = Utils.getRandomLong(TestUtils.RANDOM, 1000000);
    short serviceId = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId = Utils.getRandomShort(TestUtils.RANDOM);
    IndexValue value =
        IndexValueUtils.getIndexValue(size, new Offset(logSegmentName, offset), expiresAtMs, time.seconds(), serviceId,
            containerId, version);
    verifyIndexValue(value, logSegmentName, size, offset, false, expiresAtMs, offset, time.seconds(), serviceId,
        containerId);
    value =
        IndexValueUtils.getIndexValue(size, new Offset(logSegmentName, offset), time.seconds(), serviceId, containerId,
            version);
    verifyIndexValue(value, logSegmentName, size, offset, false, Utils.Infinite_Time, offset, time.seconds(), serviceId,
        containerId);
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
    short serviceId = Utils.getRandomShort(TestUtils.RANDOM);
    short containerId = Utils.getRandomShort(TestUtils.RANDOM);
    IndexValue value =
        IndexValueUtils.getIndexValue(oldSize, new Offset(logSegmentName, oldOffset), expiresAtMs, time.seconds(),
            serviceId, containerId, version);
    long newOffset = Utils.getRandomLong(TestUtils.RANDOM, 1000);
    long newSize = Utils.getRandomLong(TestUtils.RANDOM, 1000);
    IndexValue newValue = new IndexValue(logSegmentName, value.getBytes(), version);
    // delete in the same log segment
    newValue.setFlag(IndexValue.Flags.Delete_Index);
    newValue.setNewOffset(new Offset(logSegmentName, newOffset));
    newValue.setNewSize(newSize);
    if (version == 0) {
      verifyIndexValue(newValue, logSegmentName, newSize, newOffset, true, expiresAtMs, oldOffset, Utils.Infinite_Time,
          DEFAULT_SHORT_VALUE, DEFAULT_SHORT_VALUE);
    } else {
      verifyIndexValue(newValue, logSegmentName, newSize, newOffset, true, expiresAtMs, oldOffset, time.seconds(),
          serviceId, containerId);
    }

    // original message offset cleared
    newValue.clearOriginalMessageOffset();
    if (version == 0) {
      verifyIndexValue(newValue, logSegmentName, newSize, newOffset, true, expiresAtMs, -1, Utils.Infinite_Time,
          DEFAULT_SHORT_VALUE, DEFAULT_SHORT_VALUE);
    } else {
      verifyIndexValue(newValue, logSegmentName, newSize, newOffset, true, expiresAtMs, -1, time.seconds(), serviceId,
          containerId);
    }

    newValue = new IndexValue(logSegmentName, value.getBytes(), version);
    String newLogSegmentName = LogSegmentNameHelper.getNextPositionName(logSegmentName);
    // delete not in the same log segment
    newValue.setFlag(IndexValue.Flags.Delete_Index);
    newValue.setNewOffset(new Offset(newLogSegmentName, newOffset));
    newValue.setNewSize(newSize);
    if (version == 0) {
      verifyIndexValue(newValue, newLogSegmentName, newSize, newOffset, true, expiresAtMs, -1, Utils.Infinite_Time,
          DEFAULT_SHORT_VALUE, DEFAULT_SHORT_VALUE);
    } else {
      verifyIndexValue(newValue, newLogSegmentName, newSize, newOffset, true, expiresAtMs, -1, time.seconds(),
          serviceId, containerId);
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
<<<<<<< HEAD
=======
   */
  private void verifyIndexValue(IndexValue value, String logSegmentName, long size, long offset, boolean isDeleted,
      long expiresAtMs, long originalMessageOffset) {
    verifyGetters(value, logSegmentName, size, offset, isDeleted, expiresAtMs, originalMessageOffset,
        Utils.Infinite_Time, DEFAULT_SHORT_VALUE, DEFAULT_SHORT_VALUE);
    verifyGetters(new IndexValue(logSegmentName, value.getBytes(), version), logSegmentName, size, offset, isDeleted,
        expiresAtMs, originalMessageOffset, Utils.Infinite_Time, DEFAULT_SHORT_VALUE, DEFAULT_SHORT_VALUE);
    verifyInvalidValueSize(value, logSegmentName);
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
>>>>>>> Adding IndexValueBuilder
   * @param operationTimeInSecs the operation time in secs
   * @param serviceId the serviceId of the Index value
   * @param containerId the containerId of the Index value
   */
  private void verifyIndexValue(IndexValue value, String logSegmentName, long size, long offset, boolean isDeleted,
      long expiresAtMs, long originalMessageOffset, long operationTimeInSecs, short serviceId, short containerId) {
    verifyGetters(value, logSegmentName, size, offset, isDeleted, expiresAtMs, originalMessageOffset,
        operationTimeInSecs, serviceId, containerId);
    verifyGetters(new IndexValue(logSegmentName, value.getBytes(), version), logSegmentName, size, offset, isDeleted,
        expiresAtMs, originalMessageOffset, operationTimeInSecs, serviceId, containerId);
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
   * @param operationTimeInSecs the operation time in secs
   * @param serviceId the serviceId of the Index value
   * @param containerId the containerId of the Index value
   */
  private void verifyGetters(IndexValue value, String logSegmentName, long size, long offset, boolean isDeleted,
      long expiresAtMs, long originalMessageOffset, long operationTimeInSecs, short serviceId, short containerId) {
    assertEquals("Size is not as expected", size, value.getSize());
    assertEquals("Offset is not as expected", new Offset(logSegmentName, offset), value.getOffset());
    assertEquals("Delete status not as expected", isDeleted, value.isFlagSet(IndexValue.Flags.Delete_Index));
    if (version == 0) {
      assertEquals("ExpiresAtMs not as expected", expiresAtMs, value.getExpiresAtMs());
    } else {
      assertEquals("ExpiresAtMs not as expected",
          expiresAtMs != Utils.Infinite_Time ? (expiresAtMs / Time.MsPerSec) * Time.MsPerSec : Utils.Infinite_Time,
          value.getExpiresAtMs());
    }
    assertEquals("Original message offset not as expected", originalMessageOffset, value.getOriginalMessageOffset());
    if (version == 1) {
      assertEquals("Operation time mismatch", Math.toIntExact(operationTimeInSecs), value.getOperationTimeInSecs());
      assertEquals("ServiceId mismatch ", serviceId, value.getServiceId());
      assertEquals("ContainerId mismatch ", containerId, value.getContainerId());
    } else {
      assertEquals("Operation time mismatch", Utils.Infinite_Time, value.getOperationTimeInSecs());
      assertEquals("ServiceId mismatch ", DEFAULT_SHORT_VALUE, value.getServiceId());
      assertEquals("ContainerId mismatch ", DEFAULT_SHORT_VALUE, value.getContainerId());
    }
  }

  /**
   * Verifies that construction of {@link IndexValue} fails with an invalid {@link ByteBuffer} value
   * @param value the source {@link IndexValue} to contruct the bad one
   * @param logSegmentName the log segment name to be used to construct the {@link IndexValue}
   */
  private void verifyInvalidValueSize(IndexValue value, String logSegmentName) {
    try {
      int capacity = TestUtils.RANDOM.nextInt(value.getBytes().capacity());
      ByteBuffer invalidValue = ByteBuffer.allocate(capacity);
      invalidValue.put(value.getBytes().array(), 0, capacity);
      new IndexValue(logSegmentName, invalidValue, version);
      fail(
          "Contruction of IndexValue expected to fail with invalid byte buffer capacity of " + invalidValue.capacity());
    } catch (IllegalArgumentException e) {
    }
  }
}

