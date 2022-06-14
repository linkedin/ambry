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

package com.github.ambry.commons;

import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.CompositeBlobInfo;

public class MetadataChunk {

  private final BlobInfo blobInfo;
  private final CompositeBlobInfo compositeBlobInfo;

  public MetadataChunk(BlobInfo blobInfo, CompositeBlobInfo compositeBlobInfo) {
    this.blobInfo = blobInfo;
    this.compositeBlobInfo = compositeBlobInfo;
  }

  public CompositeBlobInfo getCompositeBlobInfo() {
    return compositeBlobInfo;
  }
  public BlobInfo getBlobInfo() { return blobInfo; }

}
