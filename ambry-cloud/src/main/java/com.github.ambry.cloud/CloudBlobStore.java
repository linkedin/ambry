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

import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.store.FindInfo;
import com.github.ambry.store.FindToken;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.MessageWriteSet;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreErrorCodes;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreGetOptions;
import com.github.ambry.store.StoreInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreStats;
import com.github.ambry.store.Write;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The blob store that reflects data in a cloud storage.
 */
class CloudBlobStore implements Store {

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final PartitionId partitionId;
  private CloudDestination cloudDestination;
  private boolean started;

  /**
   * Constructor for CloudBlobStore
   * @param partitionId partition associated with BlobStore.
   * @param verProps the cloud store properties.
   */
  CloudBlobStore(PartitionId partitionId, VerifiableProperties verProps, CloudDestination cloudDestination) {
    this.cloudDestination = cloudDestination;
    this.partitionId = partitionId;
  }

  @Override
  public void start() throws StoreException {
    started = true;
  }

  @Override
  public StoreInfo get(List<? extends StoreKey> ids, EnumSet<StoreGetOptions> storeGetOptions) throws StoreException {
    throw new UnsupportedOperationException("Method not supported");
  }

  @Override
  public void put(MessageWriteSet messageSetToWrite) throws StoreException {
    checkStarted();
    if (messageSetToWrite.getMessageSetInfo().isEmpty()) {
      throw new IllegalArgumentException("Message write set cannot be empty");
    }
    checkDuplicates(messageSetToWrite);

    // Write the blobs in the message set
    CloudWriter cloudWriter = new CloudWriter(this, messageSetToWrite.getMessageSetInfo());
    try {
      messageSetToWrite.writeTo(cloudWriter);
    } catch (IOException ex) {
      throw new StoreException(ex, StoreErrorCodes.IOError);
    }
  }

  /**
   * Upload the blob to the cloud destination.
   * @param messageInfo the {@link MessageInfo} containing blob metadata.
   * @param messageBuf the bytes to be uploaded.
   * @param size the number of bytes to upload.
   * @throws Exception
   */
  private void putBlob(MessageInfo messageInfo, ByteBuffer messageBuf, long size) throws Exception {
    boolean performUpload = messageInfo.getExpirationTimeInMs() == Utils.Infinite_Time && !messageInfo.isDeleted();
    // May need to encrypt buffer before upload
    if (performUpload) {
      BlobId blobId = (BlobId) messageInfo.getStoreKey();
      cloudDestination.uploadBlob(blobId, size, new ByteBufferInputStream(messageBuf));
    }
  }

  @Override
  public void delete(MessageWriteSet messageSetToDelete) throws StoreException {
    checkStarted();
    checkDuplicates(messageSetToDelete);

    try {
      for (MessageInfo msgInfo : messageSetToDelete.getMessageSetInfo()) {
        BlobId blobId = (BlobId) msgInfo.getStoreKey();
        cloudDestination.deleteBlob(blobId);
      }
    } catch (Exception ex) {
      throw new StoreException(ex, StoreErrorCodes.IOError);
    }
  }

  /**
   * {@inheritDoc}
   * Currently, the only supported operation is to set the TTL to infinite (i.e. no arbitrary increase or decrease)
   * @param messageSetToUpdate The list of messages that need to be updated
   * @throws StoreException if there is a problem persisting the operation in the store.
   */
  @Override
  public void updateTtl(MessageWriteSet messageSetToUpdate) throws StoreException {
    checkStarted();
  }

  @Override
  public FindInfo findEntriesSince(FindToken token, long maxTotalSizeOfEntries) throws StoreException {
    throw new UnsupportedOperationException("Method not supported");
  }

  @Override
  public Set<StoreKey> findMissingKeys(List<StoreKey> keys) throws StoreException {
    checkStarted();
    // Check existence of keys in cloud container
    Set<StoreKey> missingKeys = new HashSet<>();
    try {
      for (StoreKey key : keys) {
        BlobId blobId = (BlobId) key;
        if (cloudDestination.doesBlobExist(blobId) == false) {
          missingKeys.add(key);
        }
      }
    } catch (Exception ex) {
      throw new StoreException(ex, StoreErrorCodes.IOError);
    }
    return missingKeys;
  }

  @Override
  public StoreStats getStoreStats() {
    return null;
  }

  @Override
  public boolean isKeyDeleted(StoreKey key) throws StoreException {
    checkStarted();
    return false;
  }

  @Override
  public long getSizeInBytes() {
    throw new UnsupportedOperationException("Method not supported");
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public void shutdown() throws StoreException {
    started = false;
  }

  /**
   * @return {@code true} if this store has been started successfully.
   */
  boolean isStarted() {
    return started;
  }

  private void checkStarted() throws StoreException {
    if (!started) {
      throw new StoreException("Store not started", StoreErrorCodes.Store_Not_Started);
    }
  }

  /**
   * Detects duplicates in {@code writeSet}
   * @param writeSet the {@link MessageWriteSet} to detect duplicates in
   * @throws StoreException if a duplicate is detected
   */
  // TODO: move into MessageWriteSet?
  private void checkDuplicates(MessageWriteSet writeSet) throws StoreException {
    List<MessageInfo> infos = writeSet.getMessageSetInfo();
    if (infos.size() > 1) {
      Set<StoreKey> seenKeys = new HashSet<>();
      for (MessageInfo info : infos) {
        if (!seenKeys.add(info.getStoreKey())) {
          throw new IllegalArgumentException("WriteSet contains duplicates. Duplicate detected: " + info.getStoreKey());
        }
      }
    }
  }

  @Override
  public String toString() {
    return "PartitionId: " + partitionId.toPathString() + " in the cloud";
  }

  private class CloudWriter implements Write {
    private final CloudBlobStore cloudBlobStore;
    private final List<MessageInfo> messageInfoList;
    private int messageIndex = 0;

    CloudWriter(CloudBlobStore cloudBlobStore, List<MessageInfo> messageInfoList) {
      this.cloudBlobStore = cloudBlobStore;
      this.messageInfoList = messageInfoList;
    }

    @Override
    public int appendFrom(ByteBuffer buffer) throws IOException {
      return 0;
    }

    @Override
    public void appendFrom(ReadableByteChannel channel, long size) throws IOException {
      // Upload the blob corresponding to the current message index
      MessageInfo messageInfo = messageInfoList.get(messageIndex);
      if (messageInfo.getSize() != size) {
        throw new IllegalStateException("Mismatched buffer length for blob: " + messageInfo.getStoreKey().getID());
      }
      ByteBuffer messageBuf = ByteBuffer.allocate((int) size);
      channel.read(messageBuf);
      try {
        cloudBlobStore.putBlob(messageInfo, messageBuf, size);
        messageIndex++;
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }
}
