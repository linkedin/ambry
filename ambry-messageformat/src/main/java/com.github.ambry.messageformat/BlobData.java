/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
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

import java.io.InputStream;


/**
 * Contains the blob stream along with some required info
 */
public class BlobData {
  private final BlobType blobType;
  private final long size;
  private final InputStream stream;

  /**
   * The blob data contains the stream and other required info
   * @param blobType {@link BlobType} of the blob
   * @param size The size of the blob
   * @param stream The stream that contains the blob
   */
  public BlobData(BlobType blobType, long size, InputStream stream) {
    this.blobType = blobType;
    this.size = size;
    this.stream = stream;
  }

  public BlobType getBlobType() {
    return this.blobType;
  }

  public long getSize() {
    return size;
  }

  public InputStream getStream() {
    return stream;
  }
}
