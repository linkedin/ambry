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

import static com.github.ambry.router.ByteRange.UNDEFINED_OFFSET;
import static org.junit.Assert.*;


public class ByteRangeTest {
  @Test
  public void testValidRange()
      throws Exception {
    testByteRangeCreationClosedRange(0, 0, true);
    testByteRangeCreationOpenRange(0, true);
    testByteRangeCreationOpenRange(15, true);
    testByteRangeCreationLastNBytes(20, true);
    testByteRangeCreationLastNBytes(0, true);
    testByteRangeCreationClosedRange(22, 44, true);
    testByteRangeCreationOpenRange(Long.MAX_VALUE - 50, true);
  }

  @Test
  public void testInvalidRanges()
      throws Exception {
    // negative indices
    testByteRangeCreationClosedRange(-2, 1, false);
    testByteRangeCreationClosedRange(5, -1, false);
    testByteRangeCreationClosedRange(-3, -2, false);
    testByteRangeCreationOpenRange(-1, false);
    testByteRangeCreationLastNBytes(-2, false);
    // start greater than end offset
    testByteRangeCreationClosedRange(32, 4, false);
  }

  private void testByteRangeCreationClosedRange(long startOffset, long endOffset, boolean expectSuccess)
      throws Exception {
    if (expectSuccess) {
      ByteRange byteRange = ByteRange.fromClosedRange(startOffset, endOffset);
      assertEquals("Wrong startOffset", startOffset, byteRange.getStartOffset());
      assertEquals("Wrong endOffset", endOffset, byteRange.getEndOffset());
    } else {
      try {
        ByteRange.fromClosedRange(startOffset, endOffset);
        fail(String.format("Range creation should not have succeeded with range [%d, %d]", startOffset, endOffset));
      } catch (InvalidByteRangeException expected) {
      }
    }
  }

  private void testByteRangeCreationOpenRange(long startOffset, boolean expectSuccess)
      throws Exception {
    if (expectSuccess) {
      ByteRange byteRange = ByteRange.fromOpenRange(startOffset);
      assertEquals("Wrong startOffset", startOffset, byteRange.getStartOffset());
      assertEquals("Wrong endOffset", UNDEFINED_OFFSET, byteRange.getEndOffset());
    } else {
      try {
        ByteRange.fromOpenRange(startOffset);
        fail("Range creation should not have succeeded with range from " + startOffset);
      } catch (InvalidByteRangeException expected) {
      }
    }
  }

  private void testByteRangeCreationLastNBytes(long lastNBytes, boolean expectSuccess)
      throws Exception {
    if (expectSuccess) {
      ByteRange byteRange = ByteRange.fromLastNBytes(lastNBytes);
      assertEquals("Wrong startOffset", UNDEFINED_OFFSET, byteRange.getStartOffset());
      assertEquals("Wrong endOffset", lastNBytes, byteRange.getEndOffset());
    } else {
      try {
        ByteRange.fromLastNBytes(lastNBytes);
        fail("Range creation should not have succeeded with range of last " + lastNBytes + " bytes");
      } catch (InvalidByteRangeException expected) {
      }
    }
  }
}
