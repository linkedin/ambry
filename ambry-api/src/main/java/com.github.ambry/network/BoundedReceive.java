/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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

/**
 * This is an interface for {@link Receive} to use any kinds of data structure to buffer the
 * incoming request or response from the network.
 * @param <T> The type of the buffer. It's either a {@link java.nio.ByteBuffer} or a {@link io.netty.buffer.ByteBuf}.
 */
public interface BoundedReceive<T> extends Receive {

  /**
   * Return the buffer and transfer the ownership of this buffer to the caller. It will release the underlying buffer
   * and it's not safe to call this function twice.
   * @return The byte buffer that contains the bytes from the network.
   */
  T getAndRelease();

  /**
   * The size of read bytes from the network layer.
   */
  long sizeRead();
}
