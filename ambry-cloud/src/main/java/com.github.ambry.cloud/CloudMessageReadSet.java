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

import com.github.ambry.store.MessageReadSet;
import com.github.ambry.store.StoreKey;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CloudMessageReadSet implements MessageReadSet {

  private final List<BlobReadInfo> blobReadInfoList;
  private final List<? extends StoreKey> storeKeys;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  CloudMessageReadSet(List<BlobReadInfo> blobReadInfoList, List<? extends StoreKey> storeKeys) {
    this.blobReadInfoList = blobReadInfoList;
    this.storeKeys = storeKeys;
  }

  @Override
  public long writeTo(int index, WritableByteChannel channel, long relativeOffset, long maxSize) throws IOException {
    validateIndex(index);
    long written = 0;
    CloudBlob cloudBlob = blobReadInfoList.get(index).getBlobRef();

    ByteBuffer outputBuffer;
    try {
      if (cloudBlob.isPrefetched()) {
        outputBuffer = cloudBlob.getPrefetchedData();
      } else {
        outputBuffer = cloudBlob.download();
      }
    } catch(CloudStorageException ex) {
      throw new IOException("Download of cloud blob " + blobReadInfoList.get(index).getBlobMetadata().getId() + " failed");
    }
    logger.trace("Downloaded {} bytes to the write channel from the cloud blob : {}", written, blobReadInfoList.get(index).getBlobMetadata().getId());
    outputBuffer.flip();
    return channel.write(outputBuffer);
  }

  @Override
  public int count() {
    return blobReadInfoList.size();
  }

  @Override
  public long sizeInBytes(int index) {
    validateIndex(index);
    return blobReadInfoList.get(index).getBlobMetadata().getSize();
  }

  @Override
  public StoreKey getKeyAt(int index) {
    validateIndex(index);
    return storeKeys.get(index);
  }

  @Override
  public void doPrefetch(int index, long relativeOffset, long size) throws IOException {
    try {
      blobReadInfoList.get(index).getBlobRef().doPrefetch();
    } catch(CloudStorageException ex) {
      throw new IOException("Prefetch of cloud blob " + blobReadInfoList.get(index).getBlobMetadata().getId() + " failed");
    }
  }

  private void validateIndex(int index) {
    if(index >= blobReadInfoList.size()) {
      throw new IndexOutOfBoundsException("index [" + index + "] out of the messageset");
    }
  }
}
