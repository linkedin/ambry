/*
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

package com.github.ambry.router;

import com.github.ambry.messageformat.BlobInfo;


/**
 * Represents the result of a getBlob operation.
 */
public class GetBlobResult {
  private final BlobInfo blobInfo;
  private final ReadableStreamChannel blobDataChannel;

  /**
   * Construct a {@link GetBlobResult}.
   * @param blobInfo the {@link BlobInfo} for the blob, or {@code null}.
   * @param blobDataChannel the {@link ReadableStreamChannel} containing the blob data, or {@code null}.
   */
  public GetBlobResult(BlobInfo blobInfo, ReadableStreamChannel blobDataChannel) {
    this.blobInfo = blobInfo;
    this.blobDataChannel = blobDataChannel;
  }

  /**
   * @return the {@link BlobInfo} for the getBlob operation, or {@code null} if there is none.
   */
  public BlobInfo getBlobInfo() {
    return blobInfo;
  }

  /**
   * @return the blob data {@link ReadableStreamChannel} for the getBlob operation, or {@code null} if there is none.
   */
  public ReadableStreamChannel getBlobDataChannel() {
    return blobDataChannel;
  }
}
