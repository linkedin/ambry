/*
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;


/**
 * Mock impl of {@link Write} backed by a {@link ByteBuffer}.
 */
public class MockWrite implements Write {

  private final ByteBuffer buf;

  /**
   * @param size the size of the buffer that will receive writes
   */
  public MockWrite(int size) {
    buf = ByteBuffer.allocate(size);
  }

  @Override
  public int appendFrom(ByteBuffer buffer) {
    int toWrite = buffer.remaining();
    buf.put(buffer);
    return toWrite;
  }

  @Override
  public void appendFrom(ReadableByteChannel channel, long size) throws StoreException {
    if (size > buf.remaining()) {
      throw new IllegalStateException("Cannot fit " + size + " bytes in buf that has " + buf.remaining() + " space");
    }
    int savedLimit = buf.limit();
    buf.limit(buf.position() + (int) size);
    try {
      while (buf.hasRemaining()) {
        channel.read(buf);
      }
    } catch (IOException e) {
      // The IOException message may vary in different java versions. As code evolves, we may need to update IO_ERROR_STR
      // in StoreException (based on java version that is being employed) to correctly capture disk I/O related errors.
      StoreErrorCodes errorCode = StoreException.resolveErrorCode(e);
      throw new StoreException(errorCode.toString() + " while writing into store", e, errorCode);
    }
    buf.limit(savedLimit);
  }

  /**
   * @return the buffer that is used for the writes. The buffer returned is a duplicate of the buffer being used for
   * writes so all data changes will reflect in both buffers.
   */
  public ByteBuffer getBuffer() {
    ByteBuffer ret = buf.duplicate();
    ret.flip();
    return ret;
  }
}
