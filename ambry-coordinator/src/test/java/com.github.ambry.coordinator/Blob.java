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
package com.github.ambry.coordinator;

import com.github.ambry.messageformat.BlobData;
import com.github.ambry.messageformat.BlobProperties;
import java.nio.ByteBuffer;


/**
 * Blob stored in Ambry. Blob consists of properties, user metadata, and data.
 */
class Blob {
  private final BlobProperties blobProperties;
  private final ByteBuffer userMetadata;
  private final BlobData blobData;

  public Blob(BlobProperties blobProperties, ByteBuffer userMetadata, BlobData blobData) {
    this.blobProperties = blobProperties;
    this.userMetadata = userMetadata;
    this.blobData = blobData;
  }

  public BlobProperties getBlobProperties() {
    return blobProperties;
  }

  public ByteBuffer getUserMetadata() {
    return userMetadata;
  }

  public BlobData getBlobData() {
    return blobData;
  }
}
