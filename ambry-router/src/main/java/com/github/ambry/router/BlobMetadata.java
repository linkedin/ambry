/**
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.commons.AmbryCacheEntry;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.CompositeBlobInfo;


/**
 * A container for metadata of a composite blob
 */
public class BlobMetadata implements AmbryCacheEntry {

  private final String blobId;
  private final BlobInfo blobInfo;
  private final CompositeBlobInfo compositeBlobInfo;

  /**
   * Constructs an instance of blob metadata
   * @param blobId Unique BlobID for Ambry blob
   * @param blobInfo Contains blob properties, user metadata, lifeVersion of the blob.
   * @param compositeBlobInfo Contains list of dataChunk Ids
   */
  public BlobMetadata(String blobId, BlobInfo blobInfo, CompositeBlobInfo compositeBlobInfo) {
    this.blobId = blobId;
    this.blobInfo = blobInfo;
    this.compositeBlobInfo = compositeBlobInfo;
  }

  /**
   * @return Ambry blobID associated with this blob metadata
   */
  public String getBlobId() {
    return blobId;
  }

  /**
   * @return Instance of compositeBlobInfo
   */
  public CompositeBlobInfo getCompositeBlobInfo() {
    return compositeBlobInfo;
  }

  /**
   * @return Instance of BlobInfo
   */
  public BlobInfo getBlobInfo() {
    return blobInfo;
  }

  /**
   * @return Size of this metadata object in bytes
   */
  @Override
  public int sizeBytes() {
    // TODO: Implement sizeBytes() for class members
    return 0;
  }
}
