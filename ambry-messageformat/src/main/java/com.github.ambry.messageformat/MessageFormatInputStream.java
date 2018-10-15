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
package com.github.ambry.messageformat;

import com.github.ambry.utils.CrcInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Converts a set of message inputs into the right message format.
 * This provides the base implementation for all types of messages
 * that need to be persisted. The bytebuffer is mandatory for
 * all derived classes. It is used to store all data except and
 * stream based data. The stream is an optional payload used to
 * stream contents from an input stream.
 */
public abstract class MessageFormatInputStream extends InputStream {

  protected ByteBuffer buffer = null;
  protected CrcInputStream stream = null;
  protected long streamLength = 0;
  protected long streamRead = 0;
  ByteBuffer crc = ByteBuffer.allocate(MessageFormatRecord.Crc_Size);
  protected long messageLength;
  protected Logger logger = LoggerFactory.getLogger(getClass());

  @Override
  public int read() throws IOException {
    if (buffer != null && buffer.remaining() > 0) {
      return buffer.get() & 0xFF;
    }
    if (stream != null && streamRead < streamLength) {
      streamRead++;
      return stream.read();
    }
    if (stream != null) {
      if (crc.position() == 0) {
        crc.putLong(stream.getValue());
        crc.flip();
      }
      if (crc.remaining() > 0) {
        return crc.get() & 0xFF;
      }
    }
    return -1;
  }

  // keep reading. the caller will decide when to end
  @Override
  public int read(byte b[], int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }
    int totalRead = 0;

    if (buffer != null && buffer.remaining() > 0) {
      int bytesToRead = Math.min(buffer.remaining(), len);
      buffer.get(b, off, bytesToRead);
      totalRead += bytesToRead;
    }
    if (stream != null) {
      if (streamRead < streamLength && (len - totalRead) > 0) {
        long bytesToRead = Math.min(streamLength - streamRead, len - totalRead);
        int readFromStream = stream.read(b, off + totalRead, (int) bytesToRead);
        streamRead += readFromStream;
        totalRead += readFromStream;
      }

      if (streamRead == streamLength) {
        if (crc.position() == 0) {
          crc.putLong(stream.getValue());
          crc.flip();
        }
        int bytesToRead = Math.min(crc.remaining(), len - totalRead);
        crc.get(b, off + totalRead, bytesToRead);
        totalRead += bytesToRead;
      }
    }
    return totalRead > 0 ? totalRead : -1;
  }

  @Override
  public int available() {
    return (buffer == null ? 0 : buffer.remaining()) + (int) (streamLength - streamRead) + crc.remaining();
  }

  public long getSize() {
    return messageLength;
  }
}
