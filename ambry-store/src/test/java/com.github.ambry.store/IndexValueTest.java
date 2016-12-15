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

import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests for {@link IndexValue}.
 */
public class IndexValueTest {

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
    long expiresAtMs = Utils.getRandomLong(TestUtils.RANDOM, 1000);
    IndexValue value = new IndexValue(size, new Offset(logSegmentName, offset), expiresAtMs);
    verifyIndexValue(value, logSegmentName, size, offset, false, expiresAtMs, offset);
    value = new IndexValue(size, new Offset(logSegmentName, offset));
    verifyIndexValue(value, logSegmentName, size, offset, false, Utils.Infinite_Time, offset);
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
    long expiresAtMs = Utils.getRandomLong(TestUtils.RANDOM, 1000);
    IndexValue value = new IndexValue(oldSize, new Offset(logSegmentName, oldOffset), expiresAtMs);

    long newOffset = Utils.getRandomLong(TestUtils.RANDOM, 1000);
    long newSize = Utils.getRandomLong(TestUtils.RANDOM, 1000);

    IndexValue newValue = new IndexValue(logSegmentName, value.getBytes());
    // delete in the same log segment
    newValue.setFlag(IndexValue.Flags.Delete_Index);
    newValue.setNewOffset(new Offset(logSegmentName, newOffset));
    newValue.setNewSize(newSize);
    verifyIndexValue(newValue, logSegmentName, newSize, newOffset, true, expiresAtMs, oldOffset);

    newValue = new IndexValue(logSegmentName, value.getBytes());
    String newLogSegmentName = LogSegmentNameHelper.getNextPositionName(logSegmentName);
    // delete not in the same log segment
    newValue.setFlag(IndexValue.Flags.Delete_Index);
    newValue.setNewOffset(new Offset(newLogSegmentName, newOffset));
    newValue.setNewSize(newSize);
    verifyIndexValue(newValue, newLogSegmentName, newSize, newOffset, true, expiresAtMs, -1);
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
   */
  private void verifyIndexValue(IndexValue value, String logSegmentName, long size, long offset, boolean isDeleted,
      long expiresAtMs, long originalMessageOffset) {
    verifyGetters(value, logSegmentName, size, offset, isDeleted, expiresAtMs, originalMessageOffset);
    verifyGetters(new IndexValue(logSegmentName, value.getBytes()), logSegmentName, size, offset, isDeleted,
        expiresAtMs, originalMessageOffset);
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
   */
  private void verifyGetters(IndexValue value, String logSegmentName, long size, long offset, boolean isDeleted,
      long expiresAtMs, long originalMessageOffset) {
    assertEquals("Size is not as expected", size, value.getSize());
    assertEquals("Offset is not as expected", new Offset(logSegmentName, offset), value.getOffset());
    assertEquals("Delete status not as expected", isDeleted, value.isFlagSet(IndexValue.Flags.Delete_Index));
    assertEquals("ExpiresAtMs not as expected", expiresAtMs, value.getExpiresAtMs());
    assertEquals("Original message offset not as expected", originalMessageOffset, value.getOriginalMessageOffset());
  }
}

