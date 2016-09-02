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


/**
 * This class holds information about a composite blob parsed from the metadata blob. It contains the chunk size,
 * total composite blob size, and a list of keys for the blob's data chunks.
 */
public class CompositeBlobInfo {
  private final int chunkSize;
  private final long totalSize;
  private final List<StoreKey> keys;

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
}
