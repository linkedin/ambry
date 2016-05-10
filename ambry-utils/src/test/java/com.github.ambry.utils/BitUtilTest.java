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
package com.github.ambry.utils;

import org.junit.Assert;
import org.junit.Test;


/**
 * Test for all the bit manipulation utils
 */
public class BitUtilTest {
  @Test
  public void testBitUtil() {
    Assert.assertTrue(BitUtil.isPowerOfTwo(32));
    Assert.assertFalse(BitUtil.isPowerOfTwo(37));
    Assert.assertTrue(BitUtil.isPowerOfTwo(4096L));
    Assert.assertFalse(BitUtil.isPowerOfTwo(4097L));
    Assert.assertEquals(BitUtil.nextHighestPowerOfTwo(62), 64);
    Assert.assertEquals(BitUtil.nextHighestPowerOfTwo(4092L), 4096);

    Assert.assertEquals(BitUtil.ntz(0x24), 2);
    Assert.assertEquals(BitUtil.ntz(0x1000), 12);
    Assert.assertEquals(BitUtil.ntz2(0x1000), 12);
    Assert.assertEquals(BitUtil.ntz3(0x1000), 12);
    Assert.assertEquals(BitUtil.pop(0x1000), 1);
    long[] words1 = new long[2];
    words1[0] = 0x4f2;
    words1[1] = 0x754;
    long[] words2 = new long[2];
    words2[0] = 0x1de6;
    words2[1] = 0xa07;
    Assert.assertEquals(BitUtil.pop_andnot(words1, words2, 0, 2), 5);
    Assert.assertEquals(BitUtil.pop_array(words1, 0, 2), 12);
    Assert.assertEquals(BitUtil.pop_intersect(words1, words2, 0, 2), 7);
    Assert.assertEquals(BitUtil.pop_union(words1, words2, 0, 2), 19);
    Assert.assertEquals(BitUtil.pop_xor(words1, words2, 0, 2), 12);
  }
}
