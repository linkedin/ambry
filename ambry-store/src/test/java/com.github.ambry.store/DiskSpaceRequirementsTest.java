/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.utils.Utils;
import java.util.Random;
import org.junit.Test;

import static org.junit.Assert.*;


public class DiskSpaceRequirementsTest {
  public static final Random RANDOM = new Random();

  /**
   * Test invalid constructor arguments
   */
  @Test
  public void invalidArgumentsTest() {
    doInvalidArgumentsTest(-1, 10, true);
    doInvalidArgumentsTest(10, -1, true);
  }

  /**
   * Test constructor and getter methods with valid constructor arguments.
   */
  @Test
  public void validArgumentsTest() {
    long segmentSizeInBytes = Utils.getRandomLong(RANDOM, Long.MAX_VALUE);
    long segmentsNeeded = Utils.getRandomLong(RANDOM, Long.MAX_VALUE);
    boolean swapRequired = RANDOM.nextBoolean();
    DiskSpaceRequirements requirements = new DiskSpaceRequirements(segmentSizeInBytes, segmentsNeeded, swapRequired);
    assertEquals("segment size doesn't match", segmentSizeInBytes, requirements.getSegmentSizeInBytes());
    assertEquals("segments needed doesn't match", segmentsNeeded, requirements.getSegmentsNeeded());
    assertEquals("swap required doesn't match", swapRequired, requirements.isSwapRequired());
  }

  /**
   * Expect an {@link IllegalArgumentException} when constructing the object.
   * @param segmentSizeInBytes the segment size in bytes.
   * @param segmentsNeeded the number of segments needed.
   * @param swapRequired whether swap segments are required.
   */
  private void doInvalidArgumentsTest(long segmentSizeInBytes, long segmentsNeeded, boolean swapRequired) {
    try {
      new DiskSpaceRequirements(segmentSizeInBytes, segmentsNeeded, swapRequired);
      fail("Should have encountered IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }
}