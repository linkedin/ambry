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

import com.github.ambry.router.AsyncWritableChannel;
import com.github.ambry.router.Callback;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;


/**
 * This represents a list of messages from a given store.
 */
public interface MessageReadSet {

  /**
   * Write the message referred by the index in this set to the given channel from the given relative offset
   * up to maxSize. Less than the complete amount may be written, but no more than maxSize can be. The number
   * of bytes written is returned
   * @param index the index of the message in the set that needs to be written
   * @param channel the channel into which the data needs to be written to
   * @param relativeOffset The relative offset into the message from which the write needs to start
   * @param maxSize The max size that needs to be written from the given message
   * @return The total bytes that was written into the channel
   * @throws IOException
   */
  long writeTo(int index, WritableByteChannel channel, long relativeOffset, long maxSize) throws IOException;

  /**
   * This method is intend to write prefetched data from {@link MessageReadSet} to {@link AsyncWritableChannel}. Data
   * should be ready in memory(no blocking call) before write to {@link AsyncWritableChannel} asynchronously. Callback is
   * called when the entire batch of writes succeeds or fails.
   * @param channel the channel into which the data needs to be written to
   * @param callback The callback when data is fully wrote to the channel.
   */
  void writeTo(AsyncWritableChannel channel, Callback<Long> callback);

  /**
   * @return The total number of messages in this set
   */
  int count();

  /**
   * Returns the size of the ith message
   * @param index The index into the message set
   * @return The size in bytes of the message represented by the index
   */
  long sizeInBytes(int index);

  /**
   * Returns the key of the ith message
   * @param index The index into the message set
   * @return The key of the message represented by the index
   */
  StoreKey getKeyAt(int index);

  /**
   * Do data prefetch from store.
   * @param index The index into the message set
   * @param relativeOffset The relativeOffset to start with
   * @param size The size requested to doPrefetch.
   * @throws IOException
   */
  void doPrefetch(int index, long relativeOffset, long size) throws IOException;
}
