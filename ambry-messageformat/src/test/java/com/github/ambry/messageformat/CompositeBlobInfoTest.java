/*
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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

package com.github.ambry.messageformat;

import com.github.ambry.store.MockId;
import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.TestUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import org.junit.Test;

import static org.junit.Assert.*;


public class CompositeBlobInfoTest {

  /**
   * Tests various valid inputs for {@link CompositeBlobInfo#getStoreKeysInByteRange(long, long)}
   */
  @Test
  public void testValidGetStoreKeysInByteRange() {
    List<Pair<StoreKey, Long>> keysAndContentSizes = createKeysAndContentSizes(60, 1, 1000000, 100);
    CompositeBlobInfo compositeBlobInfo = new CompositeBlobInfo(keysAndContentSizes);
    long totalSize = compositeBlobInfo.getTotalSize();
    //total byte range
    assertGetStoreKeysInByteRange(0, totalSize - 1, compositeBlobInfo, keysAndContentSizes);
    //last byte
    assertGetStoreKeysInByteRange(totalSize - 1, totalSize - 1, compositeBlobInfo, keysAndContentSizes);
    //first byte
    assertGetStoreKeysInByteRange(0, 0, compositeBlobInfo, keysAndContentSizes);
    //first half
    assertGetStoreKeysInByteRange(0, (totalSize - 1) / 2, compositeBlobInfo, keysAndContentSizes);
    //second half
    assertGetStoreKeysInByteRange((totalSize - 1) / 2, (totalSize - 1), compositeBlobInfo, keysAndContentSizes);
    //middle third of bytes
    assertGetStoreKeysInByteRange((totalSize - 1) / 3, 2 * (totalSize - 1) / 3, compositeBlobInfo, keysAndContentSizes);
    //byte in the middle
    assertGetStoreKeysInByteRange((totalSize - 1) / 2, (totalSize - 1) / 2, compositeBlobInfo, keysAndContentSizes);
    //two bytes in the middle
    assertGetStoreKeysInByteRange((totalSize - 1) / 2, (totalSize - 1) / 2 + 1, compositeBlobInfo, keysAndContentSizes);
    //total range minus first and last byte
    assertGetStoreKeysInByteRange(1, totalSize - 2, compositeBlobInfo, keysAndContentSizes);
  }

  /**
   * Tests various invalid inputs for {@link CompositeBlobInfo#getStoreKeysInByteRange(long, long)}
   */
  @Test
  public void testInvalidGetStoreKeysInByteRange() {
    List<Pair<StoreKey, Long>> keysAndContentSizes = createKeysAndContentSizes(60, 1, 1000000, 100);
    CompositeBlobInfo compositeBlobInfo = new CompositeBlobInfo(keysAndContentSizes);
    long totalSize = compositeBlobInfo.getTotalSize();
    //total size given as input
    assertInvalidGetStoreKeysInByteRange(totalSize, totalSize, compositeBlobInfo);
    //total size and total size + 1 given as input
    assertInvalidGetStoreKeysInByteRange(totalSize, totalSize + 1, compositeBlobInfo);
    //negative number given as input
    assertInvalidGetStoreKeysInByteRange(-1, totalSize - 1, compositeBlobInfo);
    //start bigger than end
    assertInvalidGetStoreKeysInByteRange(totalSize - 1, 0, compositeBlobInfo);
    //start bigger than end, one byte
    assertInvalidGetStoreKeysInByteRange(1, 0, compositeBlobInfo);
    //total range but end equal to total size
    assertInvalidGetStoreKeysInByteRange(0, totalSize, compositeBlobInfo);
  }

  /**
   * Tests that invalid inputs to the V3 {@link CompositeBlobInfo} ctor don't work
   */
  @Test
  public void testCreateInvalidCompositeBlob() {
    //tests null input
    invalidV3CompositeBlobInfoCtor(null);
    //tests empty input
    List<Pair<StoreKey, Long>> keysAndContentSizes = new ArrayList<>();
    invalidV3CompositeBlobInfoCtor(keysAndContentSizes);
    //tests attempt creation of compositeBlobInfo with 0 length blob
    keysAndContentSizes = createKeysAndContentSizes(60, 1, 1000000, 100);
    keysAndContentSizes.add(new Pair<>(new MockId(TestUtils.getRandomString(60)), 0L));
    invalidV3CompositeBlobInfoCtor(keysAndContentSizes);
  }

  /**
   * Tests various kinds of inputs for the V2 {@link CompositeBlobInfo}
   */
  @Test
  public void testV2CompositeBlobInfoCtor() {
    List<StoreKey> keys = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      keys.add(new MockId(TestUtils.getRandomString(60)));
    }
    //creates 1 10 byte blob, so not enough keys
    invalidV2CompositeBlobInfoCtor(100, 10, keys);
    //creates 9 100 byte blobs, so too many keys
    invalidV2CompositeBlobInfoCtor(100, 900, keys);
    //creates 9 100 byte blobs and 1 1 byte blob
    validV2CompositeBlobInfoCtor(100, 901, keys);
    //creates 10 100 byte blobs
    validV2CompositeBlobInfoCtor(100, 1000, keys);
    //creates 10 100 byte blobs and 1 1 byte blob, so not enough keys
    invalidV2CompositeBlobInfoCtor(100, 1001, keys);
    //rest are a mix of negative and 0 values for chunkSize and totalSize, all invalid
    invalidV2CompositeBlobInfoCtor(0, 0, keys);
    invalidV2CompositeBlobInfoCtor(1, 0, keys);
    invalidV2CompositeBlobInfoCtor(0, 1, keys);
    invalidV2CompositeBlobInfoCtor(-1, 1, keys);
    invalidV2CompositeBlobInfoCtor(-1, -1, keys);
    invalidV2CompositeBlobInfoCtor(1, -1, keys);
  }

  private void validV2CompositeBlobInfoCtor(int chunkSize, long totalSize, List<StoreKey> keys) {
    new CompositeBlobInfo(chunkSize, totalSize, keys);
  }

  private void invalidV2CompositeBlobInfoCtor(int chunkSize, long totalSize, List<StoreKey> keys) {
    try {
      new CompositeBlobInfo(chunkSize, totalSize, keys);
    } catch (IllegalArgumentException e) {
      //expected
      return;
    }
    fail();
  }

  private void invalidV3CompositeBlobInfoCtor(List<Pair<StoreKey, Long>> keysAndContentSizes) {
    try {
      new CompositeBlobInfo(keysAndContentSizes);
    } catch (IllegalArgumentException e) {
      //expected
      return;
    }
    fail();
  }

  /**
   * Creates a list of pairs of StoreKeys and content sizes, where each pair is of a random size
   * @param keySize size of the actual store key
   * @param lowerBound the smallest random content size value allowed
   * @param higherBound the largest random content size value allowed
   * @param numKeys number of keys to populate the list
   * @return a list of pairs of StoreKeys and content sizes, where each pair is of a random size
   */
  private List<Pair<StoreKey, Long>> createKeysAndContentSizes(int keySize, int lowerBound, int higherBound,
      int numKeys) {
    List<Pair<StoreKey, Long>> keysAndContentSizes = new ArrayList<>();
    Random rand = new Random();
    for (int i = 0; i < numKeys; i++) {
      keysAndContentSizes.add(new Pair<>(new MockId(TestUtils.getRandomString(keySize)),
          (long) rand.nextInt(higherBound - lowerBound) + lowerBound));
    }
    return keysAndContentSizes;
  }

  /*
  Naive O(n) algorithm to test the actual method (which should be O(ln(n))), linearly scans list for
  chunks with bytes that lie between start and end (both inclusive)
   */
  private List<CompositeBlobInfo.ChunkMetadata> getChunkMetadataForRange(long start, long end,
      List<Pair<StoreKey, Long>> keysAndContentSizes) {
    Objects.nonNull(keysAndContentSizes);
    long totalSize = 0;
    for (Pair<StoreKey, Long> keyAndContentSize : keysAndContentSizes) {
      totalSize += keyAndContentSize.getSecond();
    }
    if (end < start || start < 0L || end >= totalSize) {
      throw new IllegalArgumentException(
          "Bad input parameters, start=" + start + " end=" + end + " totalSize=" + totalSize);
    }
    List<CompositeBlobInfo.ChunkMetadata> ans = new ArrayList<>();
    long seenSoFar = -1;
    int idx = 0;
    while (seenSoFar < end) {
      Pair<StoreKey, Long> keyAndContentSize = keysAndContentSizes.get(idx++);
      seenSoFar += keyAndContentSize.getSecond();
      if (seenSoFar >= start) {
        ans.add(new CompositeBlobInfo.ChunkMetadata(keyAndContentSize.getFirst(),
            seenSoFar - keyAndContentSize.getSecond() + 1, keyAndContentSize.getSecond()));
      }
    }
    return ans;
  }

  private void assertInvalidGetStoreKeysInByteRange(long start, long end, CompositeBlobInfo compositeBlobInfo) {
    try {
      compositeBlobInfo.getStoreKeysInByteRange(start, end);
    } catch (IllegalArgumentException e) {
      //expected
      return;
    }
    fail();
  }

  private void assertGetStoreKeysInByteRange(long start, long end, CompositeBlobInfo compositeBlobInfo,
      List<Pair<StoreKey, Long>> keysAndContentSizes) {
    assertEqualChunkMetadataLists(getChunkMetadataForRange(start, end, keysAndContentSizes),
        compositeBlobInfo.getStoreKeysInByteRange(start, end));
  }

  private void assertEqualChunkMetadataLists(List<CompositeBlobInfo.ChunkMetadata> expected,
      List<CompositeBlobInfo.ChunkMetadata> actual) {
    assertNotNull("Actual shouldn't be null", actual);
    assertNotNull("Expected shouldn't be null", expected);
    assertEquals("Different list sizes", expected.size(), actual.size());
    for (int i = 0; i < actual.size(); i++) {
      assertEquals("Actual chunkMetadata is different than expected chunkMetadata", actual.get(i), expected.get(i));
    }
  }
}
