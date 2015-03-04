package com.github.ambry.utils;

import org.junit.Assert;
import org.junit.Test;


/**
 * OpenBitSet Test
 */
public class OpenBitSetTest {
  @Test
  public void testOpenBitSetTest() {
    OpenBitSet bitSet = new OpenBitSet(1000);
    bitSet.set(0);
    bitSet.set(100);
    Assert.assertTrue(bitSet.get(0));
    Assert.assertTrue(bitSet.get(100));
    Assert.assertFalse(bitSet.get(1));
    bitSet.clear(0);
    Assert.assertFalse(bitSet.get(0));
    Assert.assertEquals(bitSet.capacity(), 1024);
    Assert.assertEquals(bitSet.size(), 1024);
    Assert.assertEquals(bitSet.length(), 1024);
    Assert.assertEquals(bitSet.isEmpty(), false);
    Assert.assertEquals(bitSet.cardinality(), 1);
    OpenBitSet bitSet2 = new OpenBitSet(1000);
    bitSet2.set(100);
    bitSet2.set(1);
    bitSet2.and(bitSet);
    Assert.assertTrue(bitSet2.get(100));
    Assert.assertFalse(bitSet2.get(1));
    bitSet2.intersect(bitSet);
    Assert.assertTrue(bitSet2.get(100));
    OpenBitSet bitSet3 = new OpenBitSet(1000);
    bitSet3.set(100);
    Assert.assertTrue(bitSet2.equals(bitSet3));
    bitSet3.set(101);
    bitSet3.set(102);
    bitSet3.set(103);
    bitSet3.clear(100, 104);
    Assert.assertFalse(bitSet3.get(100));
    Assert.assertFalse(bitSet3.get(101));
    Assert.assertFalse(bitSet3.get(102));
    Assert.assertFalse(bitSet3.get(103));
  }
}
