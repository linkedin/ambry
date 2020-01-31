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

import com.github.ambry.utils.ByteBufferInputStream;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;


/**
 * Contains the blob stream along with some required info
 */
public class BlobData {
  private final BlobType blobType;
  private final long size;
  private ByteBuf byteBuf;
  private ByteBufferInputStream stream = null;

  /**
   * The blob data contains the stream and other required info
   * @param blobType {@link BlobType} of the blob
   * @param size The size of the blob content.
   * @param byteBuf The content of this blob in a {@link ByteBuf}.
   */
  public BlobData(BlobType blobType, long size, ByteBuf byteBuf) {
    this.blobType = blobType;
    this.size = size;
    this.byteBuf = byteBuf;
  }

  /**
   * The blob data contains the stream and other required info
   * @param blobType {@link BlobType} of the blob
   * @param size The size of the blob content.
   * @param stream The {@link ByteBufferInputStream} containing the blob content.
   */
  @Deprecated
  public BlobData(BlobType blobType, long size, ByteBufferInputStream stream) {
    this.blobType = blobType;
    this.size = size;
    this.byteBuf = Unpooled.wrappedBuffer(stream.getByteBuffer());
    this.stream = stream;
  }

  /**
   * @return the type of the blob.
   */
  public BlobType getBlobType() {
    return this.blobType;
  }

  /**
   * @return the size of the blob content.
   */
  public long getSize() {
    return size;
  }

  /**
   * @return the {@link ByteBufferInputStream} containing the blob content.
   */
  @Deprecated
  public ByteBufferInputStream getStream() {
    if (stream != null) {
      return stream;
    }
    // The blob content is passed as a ByteBuf since the stream is nulle
    if (byteBuf == null) {
      return null;
    }
    ByteBuffer temp = ByteBuffer.allocate(byteBuf.readableBytes());
    byteBuf.readBytes(temp);
    byteBuf.release();
    byteBuf = null;
    temp.flip();
    stream = new ByteBufferInputStream(temp);
    return stream;
  }

  /**
   * Return the netty {@link ByteBuf} and then transfer the ownship to the caller. It's not safe
   * to call this method more than once.
   */
  public ByteBuf getAndRelease() {
    if (byteBuf == null) {
      return null;
    }
    try {
      return byteBuf.retainedDuplicate();
    } finally {
      byteBuf.release();
      byteBuf = null;
    }
  }
}
