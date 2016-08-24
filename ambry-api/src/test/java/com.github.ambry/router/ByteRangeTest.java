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


public class ByteRangeTest {
  @Test
  public void testValidRange()
      throws Exception {
    testByteRangeCreationOffsetRange(0, 0, true);
    testByteRangeCreationFromStartOffset(0, true);
    testByteRangeCreationFromStartOffset(15, true);
    testByteRangeCreationLastNBytes(20, true);
    testByteRangeCreationLastNBytes(0, true);
    testByteRangeCreationOffsetRange(22, 44, true);
    testByteRangeCreationFromStartOffset(Long.MAX_VALUE - 50, true);
  }

  @Test
  public void testInvalidRanges()
      throws Exception {
    // negative indices
    testByteRangeCreationOffsetRange(-2, 1, false);
    testByteRangeCreationOffsetRange(5, -1, false);
    testByteRangeCreationOffsetRange(-3, -2, false);
    testByteRangeCreationFromStartOffset(-1, false);
    testByteRangeCreationLastNBytes(-2, false);
    // start greater than end offset
    testByteRangeCreationOffsetRange(32, 4, false);
  }

  private void testByteRangeCreationOffsetRange(long startOffset, long endOffset, boolean expectSuccess)
      throws Exception {
    if (expectSuccess) {
      ByteRange byteRange = ByteRange.fromOffsetRange(startOffset, endOffset);
      assertEquals("Wrong startOffset", startOffset, byteRange.getStartOffset());
      assertEquals("Wrong endOffset", endOffset, byteRange.getEndOffset());
      try {
        byteRange.getLastNBytes();
        fail("Should not be able to call getLastNBytes for the range: " + byteRange);
      } catch (IllegalStateException expected) {
      }
    } else {
      try {
        ByteRange.fromOffsetRange(startOffset, endOffset);
        fail(String.format("Range creation should not have succeeded with range [%d, %d]", startOffset, endOffset));
      } catch (InvalidByteRangeException expected) {
      }
    }
  }

  private void testByteRangeCreationFromStartOffset(long startOffset, boolean expectSuccess)
      throws Exception {
    if (expectSuccess) {
      ByteRange byteRange = ByteRange.fromStartOffset(startOffset);
      assertEquals("Wrong startOffset", startOffset, byteRange.getStartOffset());
      try {
        byteRange.getEndOffset();
        byteRange.getLastNBytes();
        fail("Should not be able to call getEndOffset or getLastNBytes for the range: " + byteRange);
      } catch (IllegalStateException expected) {
      }
    } else {
      try {
        ByteRange.fromStartOffset(startOffset);
        fail("Range creation should not have succeeded with range from " + startOffset);
      } catch (InvalidByteRangeException expected) {
      }
    }
  }

  private void testByteRangeCreationLastNBytes(long lastNBytes, boolean expectSuccess)
      throws Exception {
    if (expectSuccess) {
      ByteRange byteRange = ByteRange.fromLastNBytes(lastNBytes);
      assertEquals("Wrong lastNBytes", lastNBytes, byteRange.getLastNBytes());
      try {
        byteRange.getStartOffset();
        byteRange.getLastNBytes();
        fail("Should not be able to call getStartOffset or getEndOffset for the range: " + byteRange);
      } catch (IllegalStateException expected) {
      }
    } else {
      try {
        ByteRange.fromLastNBytes(lastNBytes);
        fail("Range creation should not have succeeded with range of last " + lastNBytes + " bytes");
      } catch (InvalidByteRangeException expected) {
      }
    }
  }
}
