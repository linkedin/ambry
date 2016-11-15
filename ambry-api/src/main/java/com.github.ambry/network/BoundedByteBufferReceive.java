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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A byte buffer version of Receive to buffer the incoming request or response.
 */
public class BoundedByteBufferReceive implements Receive {

  private ByteBuffer buffer = null;
  private ByteBuffer sizeBuffer;
  private long sizeToRead;
  private long sizeRead;
  private Logger logger = LoggerFactory.getLogger(getClass());

  public BoundedByteBufferReceive() {
    sizeToRead = 0;
    sizeRead = 0;
    sizeBuffer = ByteBuffer.allocate(8);
  }

  @Override
  public boolean isReadComplete() {
    return !(buffer == null || sizeRead < sizeToRead);
  }

  @Override
  public long readFrom(ReadableByteChannel channel) throws IOException {
    long bytesRead = 0;
    if (buffer == null) {
      bytesRead = channel.read(sizeBuffer);
      if (bytesRead < 0) {
        throw new EOFException();
      }
      if (sizeBuffer.position() == sizeBuffer.capacity()) {
        sizeBuffer.flip();
        sizeToRead = sizeBuffer.getLong();
        sizeRead += 8;
        bytesRead += 8;
        buffer = ByteBuffer.allocate((int) sizeToRead - 8);
      }
    }
    if (buffer != null && sizeRead < sizeToRead) {
      long bytesReadFromChannel = channel.read(buffer);
      if (bytesReadFromChannel < 0) {
        throw new EOFException();
      }
      sizeRead += bytesReadFromChannel;
      bytesRead += bytesReadFromChannel;
      if (sizeRead == sizeToRead) {
        buffer.flip();
      }
    }
    logger.trace("size read from channel {}", sizeRead);
    return bytesRead;
  }

  public ByteBuffer getPayload() {
    return buffer;
  }

  /**
   * The total size in bytes that needs to receive from the channel
   * It will be initialized only after header is read.
   * @return the size of the data in bytes to receive after reading header, otherwise return 0
   */
  long sizeRead() {
    return sizeRead;
  }
}
