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
package com.github.ambry.utils;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeoutException;


/**
 * A buffer pool interface that defines a utility for allocating and deallocating
 * (reclaiming back to the pool) {@link ByteBuffer}.
 */
public interface ByteBufferPool {
  /**
   * Allocate a byte buffer at the requested size
   * @param size the buffer size to allocate in bytes
   * @param timeToBlockInMs the maximum time in milliseconds to block a request
   *                      until the requested size of memory becomes available
   * @return A {@link ByteBuffer} at the requested size
   * @throws TimeoutException if request cannot be served within {@code timeToBlockMs}
   * @throws InterruptedException if the current thread is interrupted while waiting
   */
  ByteBuffer allocate(int size, long timeToBlockInMs) throws TimeoutException, InterruptedException;

  /**
   * Reclaim a {@link ByteBuffer} to the pool.
   * @param buffer the {@link ByteBuffer} to be deallocated back to the pool
   */
  void deallocate(ByteBuffer buffer);
}
