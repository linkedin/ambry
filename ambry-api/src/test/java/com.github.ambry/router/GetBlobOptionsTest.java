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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


/**
 * Test the {@link GetBlobOptions} class for correctness.
 */
public class GetBlobOptionsTest {
  /**
   * Test that the range option can be assigned and retrieved correctly.
   * @throws Exception
   */
  @Test
  public void testRangeOption()
      throws Exception {
    long startOffset = 1;
    long endOffset = 2;
    ByteRange range = ByteRange.fromOffsetRange(startOffset, endOffset);
    GetBlobOptions options =
        new GetBlobOptions(GetBlobOptions.OperationType.All, ByteRange.fromOffsetRange(startOffset, endOffset));
    assertEquals("Range from options not as expected.", range, options.getRange());
  }

  /**
   * Test that the OperationType option can be assigned and retrieved correctly.
   * @throws Exception
   */
  @Test
  public void testGetOperationTypeOption() {
    GetBlobOptions options = new GetBlobOptions(GetBlobOptions.OperationType.BlobInfo, null);
    assertEquals("OperationType from options not as expected.", GetBlobOptions.OperationType.BlobInfo,
        options.getOperationType());
  }

  /**
   * Test toString, equals, and hashCode methods.
   */
  @Test
  public void testToStringEqualsAndHashcode() {
    ByteRange byteRange = ByteRange.fromLastNBytes(4);
    GetBlobOptions.OperationType type = GetBlobOptions.OperationType.Data;
    GetBlobOptions a = new GetBlobOptions(type, byteRange);
    GetBlobOptions b = new GetBlobOptions(type, byteRange);
    assertEquals("GetBlobOptions should be equal", a, b);
    assertEquals("GetBlobOptions hashcodes should be equal", a.hashCode(), b.hashCode());
    assertEquals("toString output not as expected",
        "GetBlobOptions{operationType=" + type + ", range=" + byteRange.toString() + "}", a.toString());

    b = new GetBlobOptions(GetBlobOptions.OperationType.Data, ByteRange.fromOffsetRange(2, 7));
    assertFalse("GetBlobOptions should not be equal.", a.equals(b));
    b = new GetBlobOptions(GetBlobOptions.OperationType.All, byteRange);
    assertFalse("GetBlobOptions should not be equal.", a.equals(b));
  }
}
