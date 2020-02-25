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
import com.github.ambry.router.AsyncWritableChannel;
import com.github.ambry.router.Callback;
import com.github.ambry.store.MessageReadSet;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.ByteBufferOutputStream;
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
  private final CloudBlobStore blobStore;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  CloudMessageReadSet(List<BlobReadInfo> blobReadInfoList, CloudBlobStore blobStore) {
    this.blobReadInfoList = blobReadInfoList;
    this.blobStore = blobStore;
  }

  @Override
  public long writeTo(int index, WritableByteChannel channel, long relativeOffset, long maxSize) throws IOException {
    validateIndex(index);
    long written = 0;
    BlobReadInfo blobReadInfo = blobReadInfoList.get(index);
    String blobIdStr = blobReadInfo.getBlobId().getID();

    try {
      // TODO: Need to refactor the code to avoid prefetching blobs for BlobInfo request,
      // or at least to prefetch only the header (requires CloudDestination enhancement)
      if (!blobReadInfo.isPrefetched()) {
        blobReadInfo.prefetchBlob(blobStore);
      }
      ByteBuffer outputBuffer = blobReadInfo.getPrefetchedBuffer();
      long sizeToRead = Math.min(maxSize, blobReadInfo.getBlobSize() - relativeOffset);
      outputBuffer.limit((int) (relativeOffset + sizeToRead));
      outputBuffer.position((int) (relativeOffset));
      written = channel.write(outputBuffer);
    } catch (StoreException ex) {
      throw new IOException("Write of cloud blob " + blobIdStr + " failed", ex);
    }
    logger.trace("Downloaded {} bytes to the write channel from the cloud blob : {}", written, blobIdStr);
    return written;
  }

  @Override
  public void writeTo(AsyncWritableChannel channel, Callback<Long> callback) {
    // TODO: read from cloud based store and write to AsyncWritableChannel is needed in the future.
    throw new UnsupportedOperationException();
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
    BlobReadInfo blobReadInfo = blobReadInfoList.get(index);
    try {
      blobReadInfo.prefetchBlob(blobStore);
    } catch (StoreException ex) {
      throw new IOException("Prefetch of cloud blob " + blobReadInfo.getBlobId().getID() + " failed", ex);
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
     * @param blobStore {@code CloudBlobStore} implementation representing the cloud from which download will happen.
     * @throws StoreException if blob cloud not be downloaded
     */
    public void prefetchBlob(CloudBlobStore blobStore) throws StoreException {
      // Casting blobsize to int, as blobs are chunked in Ambry, and chunk size is 4/8MB.
      // However, if in future, if very large size of blobs are allowed, then prefetching logic should be changed.
      prefetchedBuffer = ByteBuffer.allocate((int) blobMetadata.getSize());
      ByteBufferOutputStream outputStream = new ByteBufferOutputStream(prefetchedBuffer);
      blobStore.downloadBlob(blobMetadata, blobId, outputStream);
      isPrefetched = true;
    }

    /**
     * Donwload the blob from the {@code CloudDestination} to the {@code OutputStream}
     * @param blobStore {@code CloudBlobStore} implementation representing the cloud from which download will happen.
     * @param outputStream OutputStream to download the blob to
     * @throws StoreException if blob download fails.
     */
    public void downloadBlob(CloudBlobStore blobStore, OutputStream outputStream) throws StoreException {
      blobStore.downloadBlob(blobMetadata, blobId, outputStream);
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
