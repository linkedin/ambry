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

package com.github.ambry.messageformat;

import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.Pair;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/**
 * This class holds information about a composite blob parsed from the metadata blob. It contains the chunk size,
 * total composite blob size, and a list of keys for the blob's data chunks.
 */
public class CompositeBlobInfo {

  /**
   * POJO class for holding a store key, the size of the data content it refers to,
   * and the offset of the data in the larger composite blob
   */
  public static class StoreKeyAndSizeAndOffset {
    private final StoreKey storeKey;
    private final long offset;
    private final long size;

    public StoreKeyAndSizeAndOffset(StoreKey storeKey, long offset, long size) {
      this.storeKey = storeKey;
      this.offset = offset;
      this.size = size;
    }

    public StoreKey getStoreKey() {
      return storeKey;
    }

    public long getOffset() {
      return offset;
    }

    public long getSize() {
      return size;
    }
  }

  private final int chunkSize;
  private final long totalSize;
  private final List<StoreKey> keys;
  private final List<Long> offsets = new ArrayList<>();
  private final List<StoreKeyAndSizeAndOffset> keysAndSizesAndOffsets = new ArrayList<>();

  /**
   * Construct a {@link CompositeBlobInfo} object.
   * @param chunkSize The size of each data chunk except the last, which could possibly be smaller.
   * @param totalSize The total size of the composite blob.
   * @param keys The list of keys for this object's data chunks.
   */
  public CompositeBlobInfo(int chunkSize, long totalSize, List<StoreKey> keys) {
    this.chunkSize = chunkSize;
    this.totalSize = totalSize;
    this.keys = keys;
    long last = 0;
    for (StoreKey key : keys) {
      offsets.add(last);
      keysAndSizesAndOffsets.add(new StoreKeyAndSizeAndOffset(key, last, Math.min(chunkSize, totalSize - last)));
      last += chunkSize;
    }
  }

  /**
   * Construct a {@link CompositeBlobInfo} object.
   * @param keysAndContentSizes
   */
  public CompositeBlobInfo(List<Pair<StoreKey, Long>> keysAndContentSizes) {
    this.chunkSize = -1;
    this.keys = new ArrayList<>();
    long last = 0;
    for (Pair<StoreKey, Long> keyAndContentSize : keysAndContentSizes) {
      StoreKey key = keyAndContentSize.getFirst();
      keys.add(key);
      offsets.add(last);
      keysAndSizesAndOffsets.add(new StoreKeyAndSizeAndOffset(key, last, keyAndContentSize.getSecond()));
      last += keyAndContentSize.getSecond();
    }
    this.totalSize = last;
  }

  /**
   * Returns the keys (along with their data content size and offset relative to the total
   * composite blob) of the chunks related to the given byte range of the entire composite
   * blob
   * @param start inclusive starting byte index
   * @param end inclusive ending byte index
   * @return
   */
  public List<StoreKeyAndSizeAndOffset> getStoreKeysInByteRange(long start, long end) {
    if (end < start || start < 0L || end >= totalSize) {
      throw new IllegalArgumentException(
          "Bad input parameters, start=" + start + " end=" + end + " totalSize=" + totalSize);
    }
    int idx = Collections.binarySearch(offsets, start);
    //binarySearch returns -(insertion point) - 1 if not an exact match, which points to the index of
    //the first element greater than the key, or list.size() if all elements in the list are
    // less than the specified key.
    if (idx < 0) {
      idx = -idx - 2; //points to element with offset closest to but less than 'start'
    }
    List<StoreKeyAndSizeAndOffset> ans = new ArrayList<>();
    while (idx < keys.size() && offsets.get(idx) <= end) {
      ans.add(keysAndSizesAndOffsets.get(idx));
      idx++;
    }
    return ans;
  }

  /**
   * Get the total size of the composite blob.
   * @return The total size in bytes.
   */
  public long getTotalSize() {
    return totalSize;
  }

  /**
   * Get the size of each data chunk in the composite blob except the last, which could possibly be smaller.
   * @return The chunk size in bytes.
   */
  public int getChunkSize() {
    return chunkSize;
  }

  /**
   * Get the list of keys for the composite blob's data chunks.
   * @return A list of {@link StoreKey}s.
   */
  public List<StoreKey> getKeys() {
    return keys;
  }

  /**
   * Get the list of keys for the composite blob's data chunks, along with the
   * key's data content size and offset relative to the total composite blob
   * @return A list of {@link StoreKeyAndSizeAndOffset}
   */
  public List<StoreKeyAndSizeAndOffset> getKeysAndSizesAndOffsets() {
    return keysAndSizesAndOffsets;
  }
}
