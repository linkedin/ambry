/**
 * Copyright 2025 LinkedIn Corp. All rights reserved.
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

import java.io.DataInputStream;


/**
 * Represents a file chunk that exists in the store
 */
public class StoreFileChunk {
  /**
   * The chunk stream of the file requested.
   */
  private final DataInputStream stream;

  /**
   * The size of the chunk in bytes.
   */
  private final long chunkLength;

  /**
   * Constructor to create a StoreFileChunk
   * @param stream the chunk stream of the file requested
   * @param chunkLength the size of the chunk in bytes
   */
  public StoreFileChunk(DataInputStream stream, long chunkLength) {
    this.stream = stream;
    this.chunkLength = chunkLength;
  }

  /**
   * Get the chunk stream of the file requested
   * @return DataInputStream representing the chunk stream of the file requested
   */
  public DataInputStream getStream() {
    return stream;
  }

  /**
   * Get the size of the chunk in bytes
   * @return the size of the chunk in bytes
   */
  public long getChunkLength() {
    return chunkLength;
  }
}
