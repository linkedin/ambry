package com.github.ambry.utils;

import junit.framework.Assert;
import org.junit.Test;


/**
 * Test for all the bit manipulation utils
 */
public class BitUtilTest {
  @Test
  public void testBitUtil() {
    Assert.assertTrue(BitUtil.isPowerOfTwo(64));
    Assert.assertFalse(BitUtil.isPowerOfTwo(63));
    Assert.assertTrue(BitUtil.isPowerOfTwo(4096L));
    Assert.assertFalse(BitUtil.isPowerOfTwo(4097L));
    Assert.assertEquals(BitUtil.nextHighestPowerOfTwo(62), 64);
    Assert.assertEquals(BitUtil.nextHighestPowerOfTwo(4092L), 4096);
    Assert.assertEquals(BitUtil.ntz(36), 2);
    Assert.assertEquals(BitUtil.ntz(4096L), 12);
    Assert.assertEquals(BitUtil.ntz2(4096L), 12);
    Assert.assertEquals(BitUtil.ntz3(4096L), 12);
    Assert.assertEquals(BitUtil.pop(4096), 1);
    long[] words1 = new long[2];
    words1[0] = 1266;
    words1[1] = 1876;
    long[] words2 = new long[2];
    words2[0] = 7654;
    words2[1] = 2567;
    Assert.assertEquals(BitUtil.pop_andnot(words1, words2, 0, 2), 5);
    Assert.assertEquals(BitUtil.pop_array(words1, 0, 2), 12);
    Assert.assertEquals(BitUtil.pop_intersect(words1, words2, 0, 2), 7);
    Assert.assertEquals(BitUtil.pop_union(words1, words2, 0, 2), 19);
    Assert.assertEquals(BitUtil.pop_xor(words1, words2, 0, 2), 12);
  }
}
