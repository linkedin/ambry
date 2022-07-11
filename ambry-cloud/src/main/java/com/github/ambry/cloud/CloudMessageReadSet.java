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
import com.github.ambry.commons.Callback;
import com.github.ambry.router.AsyncWritableChannel;
import com.github.ambry.store.MessageReadSet;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreKey;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An implementation of {@code MessageReadSet} for cloud based storage
 */
class CloudMessageReadSet implements MessageReadSet {

  private final List<BlobReadInfo> blobReadInfoList;
  private final CloudBlobStore blobStore;
  private static final Logger logger = LoggerFactory.getLogger(CloudMessageReadSet.class);

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
      if (!blobReadInfo.isBlobDownloaded()) {
        blobReadInfo.downloadBlob(blobStore);
      }
      ByteBuf outputBuf = blobReadInfo.getBlobContent().duplicate();
      long sizeToRead = Math.min(maxSize, blobReadInfo.getBlobSize() - relativeOffset);
      outputBuf.setIndex((int) (relativeOffset), (int) (relativeOffset + sizeToRead));
      written = channel.write(outputBuf.nioBuffer());
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
      if (!blobReadInfo.isBlobDownloaded()) {
        blobReadInfo.downloadBlob(blobStore);
      }
      ByteBuf byteBuf = blobReadInfoList.get(index).getBlobContent();
      byteBuf.setIndex((int) (relativeOffset), (int) (relativeOffset + size));
    } catch (StoreException ex) {
      throw new IOException("Prefetch of cloud blob " + blobReadInfo.getBlobId().getID() + " failed", ex);
    }
  }

  @Override
  public ByteBuf getPrefetchedData(int index) {
    validateIndex(index);
    return blobReadInfoList.get(index).getBlobContent();
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
    private ByteBuf blobContent = null;

    public BlobReadInfo(CloudBlobMetadata blobMetadata, BlobId blobId) {
      this.blobMetadata = blobMetadata;
      this.blobId = blobId;
    }

    /**
     * Download the {@code blob} from {@code CloudDestination} and put it in {@code blobContent}
     * @param blobStore {@code CloudBlobStore} implementation representing the cloud from which download will happen.
     * @throws StoreException if blob cloud not be downloaded
     */
    public void downloadBlob(CloudBlobStore blobStore) throws StoreException {
      // Casting blobsize to int, as blobs are chunked in Ambry, and chunk size is 4/8MB.
      // However, if in future, if very large size of blobs are allowed, then prefetching logic should be changed.
      blobContent = PooledByteBufAllocator.DEFAULT.ioBuffer((int) blobMetadata.getSize());
      ByteBufOutputStream outputStream = new ByteBufOutputStream(blobContent);
      blobStore.downloadBlob(blobMetadata, blobId, outputStream);
    }

    /**
     * Download the {@code blob} from {@code CloudDestination} and put it in {@code blobContent} asynchronously.
     * @param blobStore {@code CloudBlobStore} implementation representing the cloud from which download will happen.
     * @return a {@link CompletableFuture} that will eventually complete when the blob is downloaded successfully. If
     *         the blob could not be downloaded, it will complete exceptionally containing the {@link StoreException}
     */
    public CompletableFuture<Void> downloadBlobAsync(CloudBlobStore blobStore) {
      // Casting blobsize to int, as blobs are chunked in Ambry, and chunk size is 4/8MB.
      // However, if in future, if very large size of blobs are allowed, then prefetching logic should be changed.
      blobContent = PooledByteBufAllocator.DEFAULT.ioBuffer((int) blobMetadata.getSize());
      ByteBufOutputStream outputStream = new ByteBufOutputStream(blobContent);
      return blobStore.downloadBlobAsync(blobMetadata, blobId, outputStream);
    }

    /**
     * @return the blob content.
     */
    public ByteBuf getBlobContent() {
      return blobContent;
    }

    /**
     * @return true if the blob has been downloaded.
     */
    public boolean isBlobDownloaded() {
      return blobContent != null;
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
