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
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;


/**
 * This class holds information about a composite blob parsed from the metadata blob. It contains the chunk size,
 * total composite blob size, and a list of keys for the blob's data chunks.
 */
public class CompositeBlobInfo {

  private final int chunkSize;
  private final long totalSize;
  private final List<Long> offsets = new ArrayList<>();
  private final List<ChunkMetadata> chunkMetadataList = new ArrayList<>();
  private final short metadataContentVersion;

  /**
   * Construct a {@link CompositeBlobInfo} object.
   * @param chunkSize The size of each data chunk except the last, which could possibly be smaller.
   * @param totalSize The total size of the composite blob.
   * @param keys The list of keys for this object's data chunks.
   */
  public CompositeBlobInfo(int chunkSize, long totalSize, List<StoreKey> keys) {
    this.chunkSize = chunkSize;
    if (chunkSize < 1L) {
      throw new IllegalArgumentException("chunkSize cannot be 0 or less");
    }
    if (keys == null || keys.isEmpty()) {
      throw new IllegalArgumentException("keys should not be null or empty");
    }
    long rightAmountOfKeys = totalSize / chunkSize + (totalSize % chunkSize == 0 ? 0 : 1);
    if (rightAmountOfKeys != keys.size()) {
      throw new IllegalArgumentException("keys should contain " + rightAmountOfKeys + " keys, not " + keys.size());
    }
    this.totalSize = totalSize;
    metadataContentVersion = MessageFormatRecord.Metadata_Content_Version_V2;
    long last = 0;
    for (StoreKey key : keys) {
      offsets.add(last);
      if (totalSize - last < 1L) {
        throw new IllegalArgumentException("CompositeBlobInfo can't be composed of blobs with size 0 or less");
      }
      chunkMetadataList.add(new ChunkMetadata(key, last, Math.min(chunkSize, totalSize - last)));
      last += chunkSize;
    }
  }

  /**
   * Construct a {@link CompositeBlobInfo} object.
   * @param keysAndContentSizes list of store keys and the size of the data content they reference
   */
  public CompositeBlobInfo(List<Pair<StoreKey, Long>> keysAndContentSizes) {
    if (keysAndContentSizes == null || keysAndContentSizes.isEmpty()) {
      throw new IllegalArgumentException("keysAndContentSizes should not be null or empty");
    }
    this.chunkSize = -1;
    long last = 0;
    metadataContentVersion = MessageFormatRecord.Metadata_Content_Version_V3;
    for (Pair<StoreKey, Long> keyAndContentSize : keysAndContentSizes) {
      if (keyAndContentSize.getSecond() < 1L) {
        throw new IllegalArgumentException("CompositeBlobInfo can't be composed of blobs with size 0 or less");
      }
      StoreKey key = keyAndContentSize.getFirst();
      offsets.add(last);
      chunkMetadataList.add(new ChunkMetadata(key, last, keyAndContentSize.getSecond()));
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
   * @return the list of {@link ChunkMetadata} that lie upon the inclusive range
   * between 'start' and 'end'
   */
  public List<ChunkMetadata> getStoreKeysInByteRange(long start, long end) {
    if (end < start || start < 0L || end >= totalSize) {
      throw new IllegalArgumentException(
          "Bad input parameters, start=" + start + " end=" + end + " totalSize=" + totalSize);
    }
    int startIdx = Collections.binarySearch(offsets, start);
    //binarySearch returns -(insertion point) - 1 if not an exact match, which points to the index of
    //the first element greater than 'start', or list.size() if all elements in the list are
    // less than 'start'.
    if (startIdx < 0) {
      startIdx = -startIdx - 2; //points to element with offset closest to but less than 'start'
    }

    int endIdx = Collections.binarySearch(offsets, end);
    if (endIdx < 0) {
      endIdx = -endIdx - 2; //points to element with offset closest to but less than 'end'
    }
    endIdx++; //List.subList expects an exclusive index

    return chunkMetadataList.subList(startIdx, endIdx);
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
    return new AbstractList<StoreKey>() {
      @Override
      public StoreKey get(int index) {
        return chunkMetadataList.get(index).getStoreKey();
      }

      @Override
      public int size() {
        return chunkMetadataList.size();
      }
    };
  }

  /**
   * Get the list of keys for the composite blob's data chunks, along with the
   * key's data content size and offset relative to the total composite blob
   * @return A list of {@link ChunkMetadata}
   */
  public List<ChunkMetadata> getChunkMetadataList() {
    return chunkMetadataList;
  }

  /**
   * Return the version of the metadata content record that this object was deserialized from
   * @return the version of the metadata content record that this object was deserialized from
   */
  public short getMetadataContentVersion() {
    return metadataContentVersion;
  }

  /**
   * POJO class for holding a store key, the size of the data content it refers to,
   * and the offset of the data in the larger composite blob
   */
  public static class ChunkMetadata {
    private final StoreKey storeKey;
    private final long offset;
    private final long size;

    public ChunkMetadata(StoreKey storeKey, long offset, long size) {
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

    @Override
    public boolean equals(Object that) {
      if (that instanceof ChunkMetadata) {
        ChunkMetadata thatCM = (ChunkMetadata) that;
        return storeKey.equals(thatCM.storeKey) && offset == thatCM.offset && size == thatCM.size;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(storeKey, offset, size);
    }
  }
}
