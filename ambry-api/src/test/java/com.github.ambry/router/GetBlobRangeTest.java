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

import junit.framework.Assert;
import org.junit.Test;

import static com.github.ambry.router.GetBlobRange.UNDEFINED_OFFSET;
import static org.junit.Assert.*;


public class GetBlobRangeTest {
  @Test
  public void testValidRange() {
    // 0-0 (0th byte)
    GetBlobRange range = new GetBlobRange(0, 0);
    assertFalse(range.withinTotalSize(0));
    assertFalse(range.withinTotalSize(-1));
    assertTrue(range.withinTotalSize(2));
    assertEquals(0, range.getStartOffset(2));
    assertEquals(0, range.getEndOffset(2));

    // 0- (bytes after/including 0)
    range = new GetBlobRange(0, UNDEFINED_OFFSET);
    assertFalse(range.withinTotalSize(0));
    assertFalse(range.withinTotalSize(-1));
    assertTrue(range.withinTotalSize(20));
    assertEquals(0, range.getStartOffset(20));
    assertEquals(19, range.getEndOffset(20));

    // 15- (bytes after/including 15)
    range = new GetBlobRange(15, UNDEFINED_OFFSET);
    assertFalse(range.withinTotalSize(15));
    assertFalse(range.withinTotalSize(-1));
    assertTrue(range.withinTotalSize(20));
    assertEquals(15, range.getStartOffset(20));
    assertEquals(19, range.getEndOffset(20));
    assertTrue(range.withinTotalSize(16));
    assertEquals(15, range.getStartOffset(16));
    assertEquals(15, range.getEndOffset(16));

    // -20 (last 20 bytes)
    range = new GetBlobRange(UNDEFINED_OFFSET, 20);
    assertFalse(range.withinTotalSize(0));
    assertFalse(range.withinTotalSize(-1));
    assertTrue(range.withinTotalSize(20));
    assertEquals(10, range.getStartOffset(30));
    assertEquals(29, range.getEndOffset(30));

    // 22-44 (bytes 22 through 44, inclusive)
    range = new GetBlobRange(22, 44);
    assertFalse(range.withinTotalSize(44));
    assertTrue(range.withinTotalSize(45));
    assertEquals(22, range.getStartOffset(45));
    assertEquals(44, range.getEndOffset(45));

    // {MAX_LONG-50}- (bytes after/including MAX_LONG-50)
    range = new GetBlobRange(Long.MAX_VALUE - 50, UNDEFINED_OFFSET);
    assertFalse(range.withinTotalSize(0));
    assertFalse(range.withinTotalSize(-1));
    assertFalse(range.withinTotalSize(20));
    assertTrue(range.withinTotalSize(Long.MAX_VALUE));
    assertEquals(Long.MAX_VALUE - 50, range.getStartOffset(Long.MAX_VALUE));
    assertEquals(Long.MAX_VALUE - 1, range.getEndOffset(Long.MAX_VALUE));
  }

  @Test
  public void testInvalidRanges() {
    long[][] invalidRanges = {
        // 0 sized range
        {UNDEFINED_OFFSET, 0},
        // negative indices (that aren't UNDEFINED_OFFSET)
        {-2, 1}, {5, -2}, {-3, -2},
        // both offsets undefined
        {UNDEFINED_OFFSET, UNDEFINED_OFFSET},
        // start greater than end offset
        {32, 4}};

    for (long[] range : invalidRanges) {
      try {
        new GetBlobRange(range[0], range[1]);
        Assert.fail(String.format("Range creation should not have succeeded with range [%d, %d]", range[0], range[1]));
      } catch (IllegalArgumentException expected) {
      }
    }
  }
}