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


public class ValidatedByteRangeTest {
  @Test
  public void testValidatedByteRange()
      throws InvalidByteRangeException {
    // 0-0 (0th byte)
    ByteRange range = ByteRange.fromOffsetRange(0, 0);
    assertRangeValidationFailure(range, 0);
    assertRangeValidationFailure(range, -1);
    assertRangeValidationSuccess(range, 2, 0, 0);

    // 0- (bytes after/including 0)
    range = ByteRange.fromStartOffset(0);
    assertRangeValidationFailure(range, 0);
    assertRangeValidationFailure(range, -1);
    assertRangeValidationSuccess(range, 20, 0, 19);

    // 15- (bytes after/including 15)
    range = ByteRange.fromStartOffset(15);
    assertRangeValidationFailure(range, 15);
    assertRangeValidationFailure(range, -1);
    assertRangeValidationSuccess(range, 20, 15, 19);
    assertRangeValidationSuccess(range, 16, 15, 15);

    // -20 (last 20 bytes)
    range = ByteRange.fromLastNBytes(20);
    assertRangeValidationFailure(range, 0);
    assertRangeValidationFailure(range, -1);
    assertRangeValidationSuccess(range, 20, 0, 19);
    assertRangeValidationSuccess(range, 30, 10, 29);

    // 22-44 (bytes 22 through 44, inclusive)
    range = ByteRange.fromOffsetRange(22, 44);
    assertRangeValidationFailure(range, 44);
    assertRangeValidationSuccess(range, 45, 22, 44);

    // {MAX_LONG-50}- (bytes after/including MAX_LONG-50)
    range = ByteRange.fromStartOffset(Long.MAX_VALUE - 50);
    assertRangeValidationFailure(range, 0);
    assertRangeValidationFailure(range, -1);
    assertRangeValidationFailure(range, 20);
    assertRangeValidationSuccess(range, Long.MAX_VALUE, Long.MAX_VALUE - 50, Long.MAX_VALUE - 1);

    // Last 0 bytes
    range = ByteRange.fromLastNBytes(0);
    assertRangeValidationSuccess(range, 0, 0, -1);
    assertRangeValidationSuccess(range, 20, 20, 19);
  }

  private void assertRangeValidationFailure(ByteRange byteRange, long totalSize) {
    try {
      new ValidatedByteRange(byteRange, totalSize);
      fail("Should have failed to validate range: " + byteRange + " with total size: " + totalSize);
    } catch (InvalidByteRangeException expected) {
    }
  }

  private void assertRangeValidationSuccess(ByteRange byteRange, long totalSize, long startOffset, long endOffset)
      throws InvalidByteRangeException {
    ValidatedByteRange validatedByteRange = new ValidatedByteRange(byteRange, totalSize);
    assertEquals("Wrong startOffset with raw range: " + byteRange + " and total size: " + totalSize, startOffset,
        validatedByteRange.getStartOffset());
    assertEquals("Wrong endOffset with raw range: " + byteRange + " and total size: " + totalSize, endOffset,
        validatedByteRange.getEndOffset());
  }
}