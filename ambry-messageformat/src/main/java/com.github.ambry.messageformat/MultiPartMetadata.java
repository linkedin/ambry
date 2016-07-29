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
import java.util.List;


public class MultiPartMetadata {
  public static final int UNDEFINED_CHUNK_SIZE = -1;
  public static final int UNDEFINED_TOTAL_SIZE = -1;
  private final int chunkSize;
  private final long totalSize;
  private final List<StoreKey> keys;

  /**
   * Construct a {@link MultiPartMetadata} object with undefined max chunk size and total size.
   * @param keys The list of keys for this object's data chunks.
   */
  public MultiPartMetadata(List<StoreKey> keys) {
    this(UNDEFINED_CHUNK_SIZE, UNDEFINED_TOTAL_SIZE, keys);
  }

  /**
   * Construct a {@link MultiPartMetadata} object.
   * @param chunkSize The intermediate chunk size for the object in bytes.
   * @param totalSize The total size of the object.
   * @param keys The list of keys for this object's data chunks.
   */
  public MultiPartMetadata(int chunkSize, long totalSize, List<StoreKey> keys) {
    this.chunkSize = chunkSize;
    this.totalSize = totalSize;
    this.keys = keys;
  }

  /**
   * Get the total size of the multi-part object this metadata describes.
   * @return The total size in bytes.
   */
  public long getTotalSize() {
    return totalSize;
  }

  /**
   * Get the intermediate chunk size for this object's data chunks.
   * @return The chunk size in bytes.
   */
  public int getChunkSize() {
    return chunkSize;
  }

  public List<StoreKey> getKeys() {
    return keys;
  }
}
