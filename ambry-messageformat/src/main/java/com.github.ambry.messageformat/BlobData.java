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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;


/**
 * Contains the blob stream along with some required info
 */
public class BlobData implements ByteBufHolder {
  private final BlobType blobType;
  private final long size;
  private ByteBuf content;

  /**
   * The blob data contains the stream and other required info
   * @param blobType {@link BlobType} of the blob
   * @param size The size of the blob content.
   * @param content The content of this blob in a {@link ByteBuf}.
   */
  public BlobData(BlobType blobType, long size, ByteBuf content) {
    this.blobType = blobType;
    this.size = size;
    this.content = content;
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

  @Override
  public ByteBuf content() {
    return content;
  }

  @Override
  public BlobData copy() {
    return replace(content().copy());
  }

  @Override
  public BlobData duplicate() {
    return replace(content().duplicate());
  }

  @Override
  public BlobData retainedDuplicate() {
    return replace(content().retainedDuplicate());
  }

  @Override
  public BlobData replace(ByteBuf content) {
    BlobData data = new BlobData(blobType, size, content);
    return data;
  }

  @Override
  public int refCnt() {
    return content.refCnt();
  }

  @Override
  public BlobData retain() {
    content.retain();
    return this;
  }

  @Override
  public BlobData retain(int increment) {
    content.retain(increment);
    return this;
  }

  @Override
  public BlobData touch() {
    content.touch();
    return this;
  }

  @Override
  public BlobData touch(Object hint) {
    content.touch(hint);
    return this;
  }

  @Override
  public boolean release() {
    return content.release();
  }

  @Override
  public boolean release(int decrement) {
    return content.release(decrement);
  }
}
