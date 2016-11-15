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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class BloomFilterTest {
  public IFilter bf;

  public BloomFilterTest() {
    bf = FilterFactory.getFilter(10000L, FilterTestHelper.MAX_FAILURE_RATE);
  }

  public static IFilter testSerialize(IFilter f) throws IOException {
    f.add(ByteBuffer.wrap("a".getBytes()));
    ByteBuffer output = ByteBuffer.allocate(100000);
    DataOutputStream out = new DataOutputStream(new ByteBufferOutputStream(output));
    FilterFactory.serialize(f, out);

    output.flip();
    DataInputStream input = new DataInputStream(new ByteBufferInputStream(output));
    IFilter f2 = FilterFactory.deserialize(input);

    assert f2.isPresent(ByteBuffer.wrap("a".getBytes()));
    assert !f2.isPresent(ByteBuffer.wrap("b".getBytes()));
    return f2;
  }

  @Before
  public void clear() {
    bf.clear();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testBloomLimits1() {
    int maxBuckets = BloomCalculations.probs.length - 1;
    int maxK = BloomCalculations.probs[maxBuckets].length - 1;

    // possible
    BloomCalculations.computeBloomSpec(maxBuckets, BloomCalculations.probs[maxBuckets][maxK]);

    // impossible, throws
    BloomCalculations.computeBloomSpec(maxBuckets, BloomCalculations.probs[maxBuckets][maxK] / 2);
  }

  @Test
  public void testOne() {
    bf.add(ByteBuffer.wrap("a".getBytes()));
    assert bf.isPresent(ByteBuffer.wrap("a".getBytes()));
    assert !bf.isPresent(ByteBuffer.wrap("b".getBytes()));
  }

  @Test
  public void testFalsePositivesInt() {
    FilterTestHelper.testFalsePositives(bf, FilterTestHelper.intKeys(), FilterTestHelper.randomKeys2());
  }

  @Test
  public void testFalsePositivesRandom() {
    FilterTestHelper.testFalsePositives(bf, FilterTestHelper.randomKeys(), FilterTestHelper.randomKeys2());
  }

  @Test
  public void testWords() {
    if (KeyGenerator.WordGenerator.WORDS == 0) {
      return;
    }
    IFilter bf2 = FilterFactory.getFilter(KeyGenerator.WordGenerator.WORDS / 2, FilterTestHelper.MAX_FAILURE_RATE);
    int skipEven = KeyGenerator.WordGenerator.WORDS % 2 == 0 ? 0 : 2;
    FilterTestHelper.testFalsePositives(bf2, new KeyGenerator.WordGenerator(skipEven, 2),
        new KeyGenerator.WordGenerator(1, 2));
  }

  @Test
  public void testSerialize() throws IOException {
    BloomFilterTest.testSerialize(bf);
  }

  public void testManyHashes(Iterator<ByteBuffer> keys) {
    int MAX_HASH_COUNT = 128;
    Set<Long> hashes = new HashSet<Long>();
    long collisions = 0;
    while (keys.hasNext()) {
      hashes.clear();
      ByteBuffer buf = keys.next();
      BloomFilter bf = (BloomFilter) FilterFactory.getFilter(10, 1);
      for (long hashIndex : bf.getHashBuckets(buf, MAX_HASH_COUNT, 1024 * 1024)) {
        hashes.add(hashIndex);
      }
      collisions += (MAX_HASH_COUNT - hashes.size());
    }
    assert collisions <= 100;
  }

  @Test
  public void testManyRandom() {
    testManyHashes(FilterTestHelper.randomKeys());
  }

  @Test
  public void testHugeBFSerialization() throws IOException {
    ByteBuffer test = ByteBuffer.wrap(new byte[]{0, 1});

    File f = File.createTempFile("bloomFilterTest-", ".dat");
    f.deleteOnExit();

    BloomFilter filter = (BloomFilter) FilterFactory.getFilter(((long) 100000 / 8) + 1, 0.01d);
    filter.add(test);
    DataOutputStream out = new DataOutputStream(new FileOutputStream(f));
    FilterFactory.serialize(filter, out);
    filter.bitset.serialize(out);
    out.close();

    DataInputStream in = new DataInputStream(new FileInputStream(f));
    BloomFilter filter2 = (BloomFilter) FilterFactory.deserialize(in);
    Assert.assertTrue(filter2.isPresent(test));
    in.close();
  }
}
