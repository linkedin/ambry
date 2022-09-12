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
import com.github.ambry.store.StoreKey;
import java.util.List;


/**
 * Represents the result of a getBlob operation.
 */
public class GetBlobResult {
  private final BlobInfo blobInfo;
  private final ReadableStreamChannel blobDataChannel;
  private final List<StoreKey> blobChunkIds;

  /**
   * Construct a {@link GetBlobResult}.
   * @param blobInfo the {@link BlobInfo} for the blob, or {@code null}.
   * @param blobDataChannel the {@link ReadableStreamChannel} containing the blob data, or {@code null}.
   * @param blobChunkIds List of chunkIds for a composite blob.
   */
  public GetBlobResult(BlobInfo blobInfo, ReadableStreamChannel blobDataChannel, List<StoreKey> blobChunkIds) {
    this.blobInfo = blobInfo;
    this.blobDataChannel = blobDataChannel;
    this.blobChunkIds = blobChunkIds;
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

  /**
   * @return all data chunk {@link StoreKey} of composite blob or {@code null} if there is none.
   */
  public List<StoreKey> getBlobChunkIds() {
    return blobChunkIds;
  }
}
