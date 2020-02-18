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

import com.github.ambry.utils.AbstractByteBufHolder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A netty {@link ByteBuf} version of Receive to buffer the incoming request or response.
 */
public class BoundedNettyByteBufReceive extends AbstractByteBufHolder<BoundedNettyByteBufReceive> {

  private ByteBuf buffer = null;
  private ByteBuf sizeBuffer = null;
  private long sizeToRead = 0;
  private long sizeRead = 0;
  private final static Logger logger = LoggerFactory.getLogger(BoundedNettyByteBufReceive.class);

  public BoundedNettyByteBufReceive() {
  }

  BoundedNettyByteBufReceive(ByteBuf buffer, long sizeToRead) {
    this.buffer = Objects.requireNonNull(buffer);
    this.sizeToRead = sizeToRead;
  }

  public boolean isReadComplete() {
    return buffer != null && sizeRead >= sizeToRead;
  }

  /**
   * Reading bytes from the {@link ReadableByteChannel} and writing those bytes to the given {@link ByteBuf}.
   * This method would change the writerIndex of the given buffer.
   * @param channel The {@link ReadableByteChannel} to read the bytes out.
   * @param buffer The {@link ByteBuf} to write bytes to.
   * @return The number of bytes read from channel.
   * @throws IOException Any I/O error.
   */
  private int readBytesFromReadableByteChannel(ReadableByteChannel channel, ByteBuf buffer) throws IOException {
    int n = channel.read(buffer.nioBuffer(buffer.writerIndex(), buffer.capacity() - buffer.writerIndex()));
    if (n > 0) {
      buffer.writerIndex(buffer.writerIndex() + n);
    }
    return n;
  }

  public long readFrom(ReadableByteChannel channel) throws IOException {
    long bytesRead = 0;
    if (buffer == null) {
      if (sizeBuffer == null) {
        sizeBuffer = ByteBufAllocator.DEFAULT.heapBuffer(Long.BYTES);
      }
      try {
        bytesRead = readBytesFromReadableByteChannel(channel, sizeBuffer);
        if (bytesRead < 0) {
          throw new EOFException();
        }
      } catch (IOException e) {
        sizeBuffer.release();
        sizeBuffer = null;
        throw e;
      }
      if (sizeBuffer.writerIndex() == sizeBuffer.capacity()) {
        sizeToRead = sizeBuffer.readLong();
        sizeRead += Long.BYTES;
        sizeBuffer.release();
        buffer = ByteBufAllocator.DEFAULT.heapBuffer((int) sizeToRead - Long.BYTES);
      }
    }
    if (buffer != null && sizeRead < sizeToRead) {
      long bytesReadFromChannel;
      try {
        bytesReadFromChannel = readBytesFromReadableByteChannel(channel, buffer);
        if (bytesReadFromChannel < 0) {
          throw new EOFException();
        }
      } catch (IOException e) {
        buffer.release();
        buffer = null;
        throw e;
      }
      sizeRead += bytesReadFromChannel;
      bytesRead += bytesReadFromChannel;
    }
    logger.trace("size read from channel {}", sizeRead);
    return bytesRead;
  }

  /**
   * The total size in bytes that needs to receive from the channel
   * It will be initialized only after header is read.
   * @return the size of the data in bytes to receive after reading header, otherwise return 0
   */
  public long sizeRead() {
    return sizeRead;
  }

  @Override
  public ByteBuf content() {
    return buffer;
  }

  @Override
  public BoundedNettyByteBufReceive replace(ByteBuf content) {
    return new BoundedNettyByteBufReceive(content, sizeToRead);
  }
}
