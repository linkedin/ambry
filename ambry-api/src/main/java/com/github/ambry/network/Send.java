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
package com.github.ambry.network;

import com.github.ambry.router.AsyncWritableChannel;
import com.github.ambry.router.Callback;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;


/**
 * Any data that needs to be sent over the network can implement
 * this interface
 */
public interface Send {
  /**
   * Writes content into the provided channel
   * @param channel The channel into which data needs to be written to
   * @return Number of bytes written
   * @throws IOException
   */
  long writeTo(WritableByteChannel channel) throws IOException;

  /**
   * Placeholder to support {@link AsyncWritableChannel}
   */
  default void writeTo(AsyncWritableChannel channel, Callback<Long> callback) {
    return;
  }

  /**
   * Returns true if the all data has been written
   * @return True if all the data has been written else false
   */
  boolean isSendComplete();

  /**
   * The total size in bytes that needs to be written to the channel
   * @return The size of the data in bytes to be written
   */
  long sizeInBytes();

  /**
   * Release all the resource this object holds. Make this a default method so subclasses don't have to override it.
   */
  default void release() {}
}
