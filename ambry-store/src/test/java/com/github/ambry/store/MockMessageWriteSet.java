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
package com.github.ambry.store;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.messageformat.MessageFormatWriteSet;
import com.github.ambry.messageformat.MessageSievingInputStream;
import com.github.ambry.utils.ByteBufferInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;


/**
 * A mock implementation of {@link MessageWriteSet} to help write to a {@link Store} and simulate cases where
 * IOException occurred.
 */
public class MockMessageWriteSet extends MessageFormatWriteSet {
  final List<ByteBuffer> buffers;
  final List<MessageInfo> infos;
  final StoreException exception;

  public class ByteBufferListInputStream extends InputStream {
    private final List<ByteBuffer> buffers;
    private int currentBufferIndex = 0;

    public ByteBufferListInputStream(List<ByteBuffer> buffers) {
      this.buffers = buffers;
    }

    @Override
    public int read() {
      while (currentBufferIndex < buffers.size()) {
        ByteBuffer currentBuffer = buffers.get(currentBufferIndex);
        if (currentBuffer.hasRemaining()) {
          return currentBuffer.get() & 0xFF; // Read a byte and convert to an unsigned int
        }
        currentBufferIndex++;
      }
      return -1; // End of stream
    }

    @Override
    public int read(byte[] b, int off, int len) {
      if (b == null) {
        throw new NullPointerException("Byte array is null");
      }
      if (off < 0 || len < 0 || len > b.length - off) {
        throw new IndexOutOfBoundsException("Invalid offset or length");
      }
      if (len == 0) {
        return 0;
      }

      int bytesRead = 0;
      while (currentBufferIndex < buffers.size() && bytesRead < len) {
        ByteBuffer currentBuffer = buffers.get(currentBufferIndex);
        int remaining = currentBuffer.remaining();
        if (remaining > 0) {
          int bytesToRead = Math.min(len - bytesRead, remaining);
          currentBuffer.get(b, off + bytesRead, bytesToRead);
          bytesRead += bytesToRead;
        } else {
          currentBufferIndex++;
        }
      }
      return bytesRead == 0 ? -1 : bytesRead; // Return -1 if no bytes were read
    }
  }


  /**
   * Constructor taking fixed lists of {@link MessageInfo} and {@link ByteBuffer}.
   * @param infos
   * @param buffers
   */
  public MockMessageWriteSet(List<MessageInfo> infos, List<ByteBuffer> buffers) {
    this(infos, buffers, null);
  }

  /**
   * Constructor taking fixed lists of {@link MessageInfo} and {@link ByteBuffer} and specified {@link StoreException}
   * @param infos
   * @param buffers
   * @param exception
   */
  public MockMessageWriteSet(List<MessageInfo> infos, List<ByteBuffer> buffers, StoreException exception) {
    super(null);
    this.infos = infos;
    this.buffers = buffers;
    this.exception = exception;
  }

  /**
   * Constructor that starts with empty lists.
   */
  public MockMessageWriteSet() {
    super(null);
    this.infos = new ArrayList<>();
    this.buffers = new ArrayList<>();
    this.exception = null;
  }

  /**
   * Add a single {@link MessageInfo} and {@link ByteBuffer} corresponding to a blob.
   * @param info
   * @param buffer
   */
  public void add(MessageInfo info, ByteBuffer buffer) {
    infos.add(info);
    buffers.add(buffer);
  }

  /**
   * Reset the buffers so they can be written again.
   */
  public void resetBuffers() {
    for (ByteBuffer buffer : buffers) {
      buffer.flip();
    }
  }

  @Override
  public long writeTo(Write writeChannel) throws StoreException {
    if (exception != null) {
      throw exception;
    }
    long sizeWritten = 0;
    for (ByteBuffer buffer : buffers) {
      sizeWritten += buffer.remaining();
      writeChannel.appendFrom(Channels.newChannel(new ByteBufferInputStream(buffer)), buffer.remaining());
    }
    return sizeWritten;
  }

  @Override
  public List<MessageInfo> getMessageSetInfo() {
    return infos;
  }

  public List<ByteBuffer> getBuffers() {
    return buffers;
  }
  /**
   * Returns input stream
   * @return InputStream
   */
  @Override
  public InputStream getStreamToWrite() {
    try {
      MessageSievingInputStream stream = new MessageSievingInputStream(new ByteBufferListInputStream(buffers), infos,
          null, new MetricRegistry());
      stream.setReplicaLocation("localhost");
      return stream;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
