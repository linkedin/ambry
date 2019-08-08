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
import com.github.ambry.store.MessageReadSet;
import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.ByteBufferOutputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An implementation of {@code MessageReadSet} for cloud based storage
 */
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

  /**
   * Validate that the index is withing the bounds of {@code blobReadInfoList}
   * @param index The index to be verified
   * @throws {@code IndexOutOfBoundsException} if the index is out of bounds
   */
  private void validateIndex(int index) {
    if (index >= blobReadInfoList.size()) {
      throw new IndexOutOfBoundsException("index [" + index + "] out of the messageset");
    }
  }

  /**
   * A class to maintain download information about each blob
   */
  static class BlobReadInfo {
    private final CloudBlobMetadata blobMetadata;
    private final BlobId blobId;
    private ByteBuffer prefetchedBuffer;
    private boolean isPrefetched;

    public BlobReadInfo(CloudBlobMetadata blobMetadata, BlobId blobId) {
      this.blobMetadata = blobMetadata;
      this.blobId = blobId;
      this.isPrefetched = false;
    }

    /**
     * Prefetch the {@code blob} from {@code CloudDestination} and put it in {@code prefetchedBuffer}
     * @param cloudDestination {@code CloudDestination} implementation representing the cloud from which download will happen.
     * @throws CloudStorageException if blob cloud not be downloaded
     */
    public void prefetchBlob(CloudDestination cloudDestination) throws CloudStorageException {
      prefetchedBuffer = ByteBuffer.allocate((int) blobMetadata.getSize());
      ByteBufferOutputStream outputStream = new ByteBufferOutputStream(prefetchedBuffer);
      cloudDestination.downloadBlob(blobId, outputStream);
      isPrefetched = true;
    }

    /**
     * Donwload the blob from the {@code CloudDestination}
     * @param cloudDestination cloudDestination {@code CloudDestination} implementation representing the cloud from which download will happen.
     * @return {@code ByteBuffer} containing the blob data
     * @throws CloudStorageException if blob download fails.
     */
    public ByteBuffer downloadBlob(CloudDestination cloudDestination) throws CloudStorageException {
      ByteBuffer blobByteBuffer = ByteBuffer.allocate((int) blobMetadata.getSize());
      ByteBufferOutputStream outputStream = new ByteBufferOutputStream(blobByteBuffer);
      cloudDestination.downloadBlob(blobId, outputStream);
      return blobByteBuffer;
    }

    /**
     * Getter for {@code prefetchedBuffer}
     * @return {@code ByteBuffer}
     */
    public ByteBuffer getPrefetchedBuffer() {
      return prefetchedBuffer;
    }

    /**
     * Getter for {@code isPrefetched}
     * @return prefetch status
     */
    public boolean isPrefetched() {
      return isPrefetched;
    }

    /**
     * Getter for {@code blobId}
     * @return {@code BlobId}
     */
    public BlobId getBlobId() {
      return blobId;
    }

    /**
     * Get the size of blob from metadata
     * @return long size
     */
    public long getBlobSize() {
      return blobMetadata.getSize();
    }
  }
}
