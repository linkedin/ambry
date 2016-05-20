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


/**
 * Contains the blob stream along with some required info
 */
public class BlobData {
  private final BlobType blobType;
  private final long size;
  private final ByteBufferInputStream stream;

  /**
   * The blob data contains the stream and other required info
   * @param blobType {@link BlobType} of the blob
   * @param size The size of the blob content.
   * @param stream The {@link ByteBufferInputStream} containing the blob content.
   */
  public BlobData(BlobType blobType, long size, ByteBufferInputStream stream) {
    this.blobType = blobType;
    this.size = size;
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
  public ByteBufferInputStream getStream() {
    return stream;
  }
}
