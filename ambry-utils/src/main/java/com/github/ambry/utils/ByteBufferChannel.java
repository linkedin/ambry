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
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;


/**
 * A {@link WritableByteChannel} that stores the bytes written into it in a {@link ByteBuffer}.
 */
public class ByteBufferChannel implements WritableByteChannel {
  private final AtomicBoolean channelOpen = new AtomicBoolean(true);
  private final ReentrantLock bufferLock = new ReentrantLock();
  private final ByteBuffer buffer;

  /**
   * Gets the {@link ByteBuffer} that is being used to receive writes.
   * @return the {@link ByteBuffer} that is receives writes to this channel.
   */
  public ByteBuffer getBuffer() {
    return buffer;
  }

  /**
   * This object needs to be instantiated with a {@link ByteBuffer} that is provided by the caller. The maximum
   * number of bytes that can be written into the {@code buffer} is determined by {@code buffer.remaining()}.
   * @param buffer the buffer that can be used to recieve writes.
   */
  public ByteBufferChannel(ByteBuffer buffer) {
    this.buffer = buffer;
  }

  /**
   * {@inheritDoc}
   * <p/>
   * Copies bytes from {@code src} into the {@link ByteBuffer} ({@code buffer}) backing this channel. The number of
   * bytes copied is the minimum of {@code src.remaining()} and {@code buffer.remaining()}.
   * @param src the source {@link ByteBuffer} to copy bytes from.
   * @return the number of bytes copied.
   * @throws ClosedChannelException if the channel is closed when this function was called.
   */
  @Override
  public int write(ByteBuffer src) throws ClosedChannelException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
    int bytesWritten = 0;
    bufferLock.lock();
    try {
      int bytesToWrite = Math.min(src.remaining(), buffer.remaining());
      for (; bytesWritten < bytesToWrite; bytesWritten++) {
        buffer.put(src.get());
      }
    } finally {
      bufferLock.unlock();
    }
    return bytesWritten;
  }

  @Override
  public boolean isOpen() {
    return channelOpen.get();
  }

  @Override
  public void close() {
    channelOpen.set(false);
  }
}
