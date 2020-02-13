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

import com.github.ambry.utils.AbstractByteBufHolder;
import io.netty.buffer.ByteBuf;


/**
 * Contains the blob stream along with some required info
 */
public class BlobData extends AbstractByteBufHolder<BlobData> {
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
  public BlobData replace(ByteBuf content) {
    return new BlobData(blobType, size, content);
  }
}
