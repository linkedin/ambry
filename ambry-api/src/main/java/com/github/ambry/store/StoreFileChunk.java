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

import com.github.ambry.utils.ByteBufferInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;


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
    Objects.requireNonNull(stream, "DataInputStream cannot be null");

    this.stream = stream;
    this.chunkLength = chunkLength;
  }

  /**
   * Create a StoreFileChunk from a ByteBuffer
   * @param buf the ByteBuffer to create the StoreFileChunk from
   * @return StoreFileChunk representing the chunk stream of the file requested
   */
  public static StoreFileChunk from(ByteBuffer buf) {
    Objects.requireNonNull(buf, "ByteBuffer cannot be null");
    return new StoreFileChunk(
        new DataInputStream(new ByteBufferInputStream(buf)), buf.remaining());
  }

  /**
   * Convert the chunk stream to a ByteBuffer
   * @return ByteBuffer representing the chunk stream of the file requested
   * @throws IOException
   */
  public ByteBuffer toBuffer() throws IOException {
    byte[] buf = new byte[(int) chunkLength];
    stream.readFully(buf);
    return ByteBuffer.wrap(buf);
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
