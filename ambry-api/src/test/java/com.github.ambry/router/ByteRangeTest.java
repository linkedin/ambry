/*
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

package com.github.ambry.router;

import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Test the {@link ByteRange} class.
 */
public class ByteRangeTest {
  /**
   * Test that we can create valid ranges and read their offsets correctly.
   * @throws Exception
   */
  @Test
  public void testValidRange() throws Exception {
    testByteRangeCreationOffsetRange(0, 0, true);
    testByteRangeCreationFromStartOffset(0, true);
    testByteRangeCreationFromStartOffset(15, true);
    testByteRangeCreationLastNBytes(20, true);
    testByteRangeCreationLastNBytes(0, true);
    testByteRangeCreationOffsetRange(22, 44, true);
    testByteRangeCreationFromStartOffset(Long.MAX_VALUE, true);
  }

  /**
   * Ensure that we cannot create invalid ranges.
   * @throws Exception
   */
  @Test
  public void testInvalidRanges() throws Exception {
    // negative indices
    testByteRangeCreationOffsetRange(-2, 1, false);
    testByteRangeCreationOffsetRange(5, -1, false);
    testByteRangeCreationOffsetRange(0, -1, false);
    testByteRangeCreationOffsetRange(-3, -2, false);
    testByteRangeCreationFromStartOffset(-1, false);
    testByteRangeCreationLastNBytes(-2, false);
    // start greater than end offset
    testByteRangeCreationOffsetRange(32, 4, false);
    testByteRangeCreationOffsetRange(1, 0, false);
    testByteRangeCreationOffsetRange(Long.MAX_VALUE, Long.MAX_VALUE - 1, false);
  }

  /**
   * Test that resolving {@link ByteRange}s with a blob size to generate ranges with defined start/end offsets works as
   * expected.
   * @throws Exception
   */
  @Test
  public void testResolvedByteRange() throws Exception {
    // 0-0 (0th byte)
    ByteRange range = ByteRange.fromOffsetRange(0, 0);
    assertRangeResolutionFailure(range, 0);
    assertRangeResolutionFailure(range, -1);
    assertRangeResolutionSuccess(range, 2, 0, 0);

    // 0- (bytes after/including 0)
    range = ByteRange.fromStartOffset(0);
    assertRangeResolutionFailure(range, 0);
    assertRangeResolutionFailure(range, -1);
    assertRangeResolutionSuccess(range, 20, 0, 19);

    // 15- (bytes after/including 15)
    range = ByteRange.fromStartOffset(15);
    assertRangeResolutionFailure(range, 15);
    assertRangeResolutionFailure(range, -1);
    assertRangeResolutionSuccess(range, 20, 15, 19);
    assertRangeResolutionSuccess(range, 16, 15, 15);

    // -20 (last 20 bytes)
    range = ByteRange.fromLastNBytes(20);
    assertRangeResolutionFailure(range, 0);
    assertRangeResolutionFailure(range, -1);
    assertRangeResolutionSuccess(range, 20, 0, 19);
    assertRangeResolutionSuccess(range, 30, 10, 29);

    // 22-44 (bytes 22 through 44, inclusive)
    range = ByteRange.fromOffsetRange(22, 44);
    assertRangeResolutionFailure(range, 44);
    assertRangeResolutionSuccess(range, 45, 22, 44);

    // {MAX_LONG-50}- (bytes after/including MAX_LONG-50)
    range = ByteRange.fromStartOffset(Long.MAX_VALUE - 50);
    assertRangeResolutionFailure(range, 0);
    assertRangeResolutionFailure(range, -1);
    assertRangeResolutionFailure(range, 20);
    assertRangeResolutionSuccess(range, Long.MAX_VALUE, Long.MAX_VALUE - 50, Long.MAX_VALUE - 1);

    // Last 0 bytes
    range = ByteRange.fromLastNBytes(0);
    assertRangeResolutionSuccess(range, 0, 0, -1);
    assertRangeResolutionSuccess(range, 20, 20, 19);
  }

  /**
   * Test toString, equals, and hashCode methods.
   */
  @Test
  public void testToStringEqualsAndHashcode() {
    ByteRange a = ByteRange.fromLastNBytes(4);
    ByteRange b = ByteRange.fromLastNBytes(4);
    assertEquals("ByteRanges should be equal", a, b);
    assertEquals("ByteRange hashcodes should be equal", a.hashCode(), b.hashCode());
    assertEquals("toString output not as expected", "ByteRange{type=LAST_N_BYTES, lastNBytes=4}", a.toString());

    a = ByteRange.fromOffsetRange(2, 5);
    assertFalse("ByteRanges should not be equal", a.equals(b));
    b = ByteRange.fromOffsetRange(2, 5);
    assertEquals("ByteRanges should be equal", a, b);
    assertEquals("ByteRange hashcodes should be equal", a.hashCode(), b.hashCode());
    assertEquals("toString output not as expected", "ByteRange{type=OFFSET_RANGE, startOffset=2, endOffset=5}",
        a.toString());

    a = ByteRange.fromStartOffset(7);
    assertFalse("ByteRanges should not be equal", a.equals(b));
    b = ByteRange.fromStartOffset(7);
    assertEquals("ByteRanges should be equal", a, b);
    assertEquals("ByteRange hashcodes should be equal", a.hashCode(), b.hashCode());
    assertEquals("toString output not as expected", "ByteRange{type=FROM_START_OFFSET, startOffset=7}", a.toString());
  }

  /**
   * Test that {@link ByteRange} works as expected for byte ranges with a defined start and end offset.
   * @param startOffset the (inclusive) start byte offset to test.
   * @param endOffset the (inclusive) end byte offset to test.
   * @param expectSuccess {@code true} if the {@link ByteRange} creation should succeed.
   * @throws Exception
   */
  private void testByteRangeCreationOffsetRange(long startOffset, long endOffset, boolean expectSuccess)
      throws Exception {
    if (expectSuccess) {
      ByteRange byteRange = ByteRange.fromOffsetRange(startOffset, endOffset);
      assertEquals("Wrong range type", ByteRange.ByteRangeType.OFFSET_RANGE, byteRange.getType());
      assertEquals("Wrong startOffset", startOffset, byteRange.getStartOffset());
      assertEquals("Wrong endOffset", endOffset, byteRange.getEndOffset());
      try {
        byteRange.getLastNBytes();
        fail("Should not be able to call getLastNBytes for the range: " + byteRange);
      } catch (UnsupportedOperationException expected) {
      }
    } else {
      try {
        ByteRange.fromOffsetRange(startOffset, endOffset);
        fail(String.format("Range creation should not have succeeded with range [%d, %d]", startOffset, endOffset));
      } catch (IllegalArgumentException expected) {
      }
    }
  }

  /**
   * Test that {@link ByteRange} works as expected for byte ranges with only a defined start offset.
   * @param startOffset the (inclusive) start byte offset to test.
   * @param expectSuccess {@code true} if the {@link ByteRange} creation should succeed.
   * @throws Exception
   */
  private void testByteRangeCreationFromStartOffset(long startOffset, boolean expectSuccess) throws Exception {
    if (expectSuccess) {
      ByteRange byteRange = ByteRange.fromStartOffset(startOffset);
      assertEquals("Wrong range type", ByteRange.ByteRangeType.FROM_START_OFFSET, byteRange.getType());
      assertEquals("Wrong startOffset", startOffset, byteRange.getStartOffset());
      try {
        byteRange.getEndOffset();
        byteRange.getLastNBytes();
        fail("Should not be able to call getEndOffset or getLastNBytes for the range: " + byteRange);
      } catch (UnsupportedOperationException expected) {
      }
    } else {
      try {
        ByteRange.fromStartOffset(startOffset);
        fail("Range creation should not have succeeded with range from " + startOffset);
      } catch (IllegalArgumentException expected) {
      }
    }
  }

  /**
   * Test that {@link ByteRange} works as expected for byte ranges encoding the number of bytes to read from the end
   * of an object.
   * @param lastNBytes the number of bytes to read from the end of an object.
   * @param expectSuccess {@code true} if the {@link ByteRange} creation should succeed.
   * @throws Exception
   */
  private void testByteRangeCreationLastNBytes(long lastNBytes, boolean expectSuccess) throws Exception {
    if (expectSuccess) {
      ByteRange byteRange = ByteRange.fromLastNBytes(lastNBytes);
      assertEquals("Wrong range type", ByteRange.ByteRangeType.LAST_N_BYTES, byteRange.getType());
      assertEquals("Wrong lastNBytes", lastNBytes, byteRange.getLastNBytes());
      try {
        byteRange.getStartOffset();
        byteRange.getLastNBytes();
        fail("Should not be able to call getStartOffset or getEndOffset for the range: " + byteRange);
      } catch (UnsupportedOperationException expected) {
      }
    } else {
      try {
        ByteRange.fromLastNBytes(lastNBytes);
        fail("Range creation should not have succeeded with range of last " + lastNBytes + " bytes");
      } catch (IllegalArgumentException expected) {
      }
    }
  }

  /**
   * Test and assert that a {@link ByteRange} fails validation with a specified total blob size.
   * @param byteRange the {@link ByteRange} to resolve with a total blob size.
   * @param totalSize the total size of a blob.
   */
  private void assertRangeResolutionFailure(ByteRange byteRange, long totalSize) {
    try {
      byteRange.toResolvedByteRange(totalSize);
      fail("Should have failed to resolve range: " + byteRange + " with total size: " + totalSize);
    } catch (IllegalArgumentException expected) {
    }
  }

  /**
   * Test and assert that a {@link ByteRange} passes validation with a specified total blob size. Ensure that
   * the defined (wrt the total blob size) start and end offsets are set correctly in the resolved {@link ByteRange}.
   * @param byteRange the {@link ByteRange} to resolve with a total blob size.
   * @param totalSize the total size of a blob.
   * @param startOffset the expected start offset for the resolved {@link ByteRange}
   * @param endOffset the expected end offset for the resolved {@link ByteRange}
   * @throws Exception
   */
  private void assertRangeResolutionSuccess(ByteRange byteRange, long totalSize, long startOffset, long endOffset)
      throws Exception {
    ByteRange resolvedByteRange = byteRange.toResolvedByteRange(totalSize);
    assertEquals("Wrong startOffset with raw range: " + byteRange + " and total size: " + totalSize, startOffset,
        resolvedByteRange.getStartOffset());
    assertEquals("Wrong endOffset with raw range: " + byteRange + " and total size: " + totalSize, endOffset,
        resolvedByteRange.getEndOffset());
  }
}
