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
  private final CloudDestination cloudDestination;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  CloudMessageReadSet(List<BlobReadInfo> blobReadInfoList, CloudDestination cloudDestination) {
    this.blobReadInfoList = blobReadInfoList;
    this.cloudDestination = cloudDestination;
  }

  @Override
  public long writeTo(int index, WritableByteChannel channel, long relativeOffset, long maxSize) throws IOException {
    validateIndex(index);
    long written = 0;
    BlobReadInfo blobReadInfo = blobReadInfoList.get(index);

    ByteBuffer outputBuffer;
    try {
      if (blobReadInfo.isPrefetched()) {
        outputBuffer = blobReadInfo.getPrefetchedBuffer();
      } else {
        outputBuffer = blobReadInfo.downloadBlob(cloudDestination);
      }
    } catch (CloudStorageException ex) {
      throw new IOException("Download of cloud blob " + blobReadInfoList.get(index).getBlobId().getID() + " failed");
    }
    logger.trace("Downloaded {} bytes to the write channel from the cloud blob : {}", written,
        blobReadInfoList.get(index).getBlobId().getID());
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
    return blobReadInfoList.get(index).getBlobSize();
  }

  @Override
  public StoreKey getKeyAt(int index) {
    validateIndex(index);
    return blobReadInfoList.get(index).getBlobId();
  }

  @Override
  public void doPrefetch(int index, long relativeOffset, long size) throws IOException {
    try {
      blobReadInfoList.get(index).prefetchBlob(cloudDestination);
    } catch (CloudStorageException ex) {
      throw new IOException("Prefetch of cloud blob " + blobReadInfoList.get(index).getBlobId().getID() + " failed");
    }
  }

  private void validateIndex(int index) {
    if (index >= blobReadInfoList.size()) {
      throw new IndexOutOfBoundsException("index [" + index + "] out of the messageset");
    }
  }
}
