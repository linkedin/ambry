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
      throws InvalidByteRangeException {
    assertByteRangeCreationSuccess(0, 0);
    assertByteRangeCreationSuccess(0, UNDEFINED_OFFSET);
    assertByteRangeCreationSuccess(15, UNDEFINED_OFFSET);
    assertByteRangeCreationSuccess(UNDEFINED_OFFSET, 20);
    assertByteRangeCreationSuccess(UNDEFINED_OFFSET, 0);
    assertByteRangeCreationSuccess(22, 44);
    assertByteRangeCreationSuccess(Long.MAX_VALUE - 50, UNDEFINED_OFFSET);
  }

  @Test
  public void testInvalidRanges() {
    // negative indices (that aren't UNDEFINED_OFFSET)
    assertByteRangeCreationFailure(-2, 1);
    assertByteRangeCreationFailure(5, -2);
    assertByteRangeCreationFailure(-3, -2);
    // both offsets undefined
    assertByteRangeCreationFailure(UNDEFINED_OFFSET, UNDEFINED_OFFSET);
    // start greater than end offset
    assertByteRangeCreationFailure(32, 4);
  }

  private void assertByteRangeCreationFailure(long startOffset, long endOffset) {
    try {
      new ByteRange(startOffset, endOffset);
      fail(String.format("Range creation should not have succeeded with range [%d, %d]", startOffset, endOffset));
    } catch (InvalidByteRangeException expected) {
    }
  }

  private void assertByteRangeCreationSuccess(long startOffset, long endOffset)
      throws InvalidByteRangeException {
    ByteRange byteRange = new ByteRange(startOffset, endOffset);
    assertEquals("Wrong startOffset", startOffset, byteRange.getStartOffset());
    assertEquals("Wrong endOffset", endOffset, byteRange.getEndOffset());
  }
}
