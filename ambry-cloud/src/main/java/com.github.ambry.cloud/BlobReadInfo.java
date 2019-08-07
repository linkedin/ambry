/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud;

import com.github.ambry.commons.BlobId;
import com.github.ambry.utils.ByteBufferOutputStream;
import java.nio.ByteBuffer;


public class BlobReadInfo {
  private final CloudBlobMetadata blobMetadata;
  private final BlobId blobId;
  private ByteBuffer prefetchedBuffer;
  private boolean isPrefetched;

  public BlobReadInfo(CloudBlobMetadata blobMetadata, BlobId blobId) {
    this.blobMetadata = blobMetadata;
    this.blobId = blobId;
    this.isPrefetched = false;
  }

  public void prefetchBlob(CloudDestination cloudDestination) throws CloudStorageException {
    prefetchedBuffer = ByteBuffer.allocate((int) blobMetadata.getSize());
    ByteBufferOutputStream outputStream = new ByteBufferOutputStream(prefetchedBuffer);
    cloudDestination.downloadBlob(blobId, outputStream);
    isPrefetched = true;
  }

  public ByteBuffer downloadBlob(CloudDestination cloudDestination) throws CloudStorageException {
    ByteBuffer blobByteBuffer = ByteBuffer.allocate((int) blobMetadata.getSize());
    ByteBufferOutputStream outputStream = new ByteBufferOutputStream(blobByteBuffer);
    cloudDestination.downloadBlob(blobId, outputStream);
    return blobByteBuffer;
  }

  public ByteBuffer getPrefetchedBuffer() {
    return prefetchedBuffer;
  }

  public boolean isPrefetched() {
    return isPrefetched;
  }

  public BlobId getBlobId() {
    return blobId;
  }

  public long getBlobSize() {
    return blobMetadata.getSize();
  }
}
