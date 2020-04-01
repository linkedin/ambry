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
package com.github.ambry.store;

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;


/**
 * A write interface provided by the underlying store to write to it
 */
public interface Write {

  /**
   * Appends the buffer into the underlying write interface (eg: file). Returns the number of bytes
   * successfully written
   * @param buffer The buffer from which data needs to be written from
   * @return The number of bytes written to the write interface
   * @throws StoreException if store error occurs when writing into underlying interface
   */
  int appendFrom(ByteBuffer buffer) throws StoreException;

  /**
   * Appends the channel to the underlying write interface. Writes "size" number of bytes
   * to the interface.
   * @param channel The channel from which data needs to be written from
   * @param size The amount of data in bytes to be written from the channel
   * @throws StoreException if store error occurs when writing into underlying interface
   */
  void appendFrom(ReadableByteChannel channel, long size) throws StoreException;
}
