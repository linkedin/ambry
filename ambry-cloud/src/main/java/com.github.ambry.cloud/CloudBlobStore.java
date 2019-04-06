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
import com.github.ambry.config.CloudConfig;
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
import java.security.GeneralSecurityException;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The blob store that reflects data in a cloud storage.
 */
class CloudBlobStore implements Store {

  private static final Logger logger = LoggerFactory.getLogger(CloudBlobStore.class);
  private final PartitionId partitionId;
  private final CloudDestination cloudDestination;
  private final CloudBlobCryptoAgent cryptoService;
  private final CloudConfig cloudConfig;
  private final long minTtlMillis;
  private boolean started;

  /**
   * Constructor for CloudBlobStore
   * @param partitionId partition associated with BlobStore.
   * @param cloudDestination the {@link CloudDestination}.
   * @param cloudConfig the {@link CloudConfig} to use.
   * @param cloudDestination the {@link CloudDestination} to use.
   * @param cryptoService the {@link CloudBlobCryptoAgent} to use for encryption.
   */
  CloudBlobStore(PartitionId partitionId, CloudConfig cloudConfig, CloudDestination cloudDestination,
      CloudBlobCryptoAgent cryptoService) {
    this.cloudDestination = Objects.requireNonNull(cloudDestination, "cloudDestination is required");
    this.partitionId = Objects.requireNonNull(partitionId, "partitionId is required");
    this.cloudConfig = Objects.requireNonNull(cloudConfig, "cloudConfig is required");
    minTtlMillis = TimeUnit.DAYS.toMillis(cloudConfig.vcrMinTtlDays);
    this.cryptoService = cryptoService;
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
    CloudWriteChannel cloudWriter = new CloudWriteChannel(this, messageSetToWrite.getMessageSetInfo());
    messageSetToWrite.writeTo(cloudWriter);
  }

  /**
   * Upload the blob to the cloud destination.
   * @param messageInfo the {@link MessageInfo} containing blob metadata.
   * @param messageBuf the bytes to be uploaded.
   * @param size the number of bytes to upload.
   * @throws CloudStorageException if the upload failed.
   */
  private void putBlob(MessageInfo messageInfo, ByteBuffer messageBuf, long size)
      throws CloudStorageException, IOException, GeneralSecurityException {
    if (shouldUpload(messageInfo)) {
      BlobId blobId = (BlobId) messageInfo.getStoreKey();
      // TODO: would be more efficient to call blobId.isEncrypted()
      String kmsContext = cryptoService.getEncryptionContext();
      CloudBlobMetadata.EncryptionType encryptionType;
      if (cloudConfig.vcrRequireEncryption && cryptoService != null && (blobId.getVersion() < BlobId.BLOB_ID_V4 || !BlobId.isEncrypted(blobId.getID()))) {
        // Need to encrypt the buffer before upload
        messageBuf = cryptoService.encrypt(messageBuf);
        encryptionType = CloudBlobMetadata.EncryptionType.VCR;
      }
      else {
        kmsContext = blobId.getAccountId()+":"+blobId.getContainerId();
        encryptionType = CloudBlobMetadata.EncryptionType.ROUTER;
      }
      CloudBlobMetadata blobMetadata =
          new CloudBlobMetadata(blobId, messageInfo.getOperationTimeMs(), messageInfo.getExpirationTimeInMs(),
              messageInfo.getSize(), kmsContext, encryptionType, CloudBlobMetadata.VcrEncryptionFormat.DEFAULT);
      cloudDestination.uploadBlob(blobId, size, blobMetadata, new ByteBufferInputStream(messageBuf));
    }
  }

  /**
   * Utility to decide whether a blob should be uploaded.
   * @param messageInfo The {@link MessageInfo} containing the blob metadata.
   * @return {@code true} is the blob should be upload, {@code false} otherwise.
   */
  private boolean shouldUpload(MessageInfo messageInfo) {
    if (messageInfo.isDeleted()) {
      return false;
    }

    // expiration time above threshold
    return (messageInfo.getExpirationTimeInMs() == Utils.Infinite_Time
        || messageInfo.getExpirationTimeInMs() - messageInfo.getOperationTimeMs() >= minTtlMillis);
  }

  @Override
  public void delete(MessageWriteSet messageSetToDelete) throws StoreException {
    checkStarted();
    checkDuplicates(messageSetToDelete);

    try {
      for (MessageInfo msgInfo : messageSetToDelete.getMessageSetInfo()) {
        BlobId blobId = (BlobId) msgInfo.getStoreKey();
        cloudDestination.deleteBlob(blobId, msgInfo.getOperationTimeMs());
      }
    } catch (CloudStorageException ex) {
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
    // Note: we skipped uploading the blob on PUT record if the TTL was below threshold.
    try {
      for (MessageInfo msgInfo : messageSetToUpdate.getMessageSetInfo()) {
        BlobId blobId = (BlobId) msgInfo.getStoreKey();
        cloudDestination.updateBlobExpiration(blobId, msgInfo.getExpirationTimeInMs());
      }
    } catch (CloudStorageException ex) {
      throw new StoreException(ex, StoreErrorCodes.IOError);
    }
  }

  @Override
  public FindInfo findEntriesSince(FindToken token, long maxTotalSizeOfEntries) throws StoreException {
    throw new UnsupportedOperationException("Method not supported");
  }

  @Override
  public Set<StoreKey> findMissingKeys(List<StoreKey> keys) throws StoreException {
    checkStarted();
    // Check existence of keys in cloud metadata
    List<BlobId> blobIdList = keys.stream().map(key -> (BlobId) key).collect(Collectors.toList());
    try {
      Set<String> foundSet = cloudDestination.getBlobMetadata(blobIdList).keySet();
      return keys.stream().filter(key -> !foundSet.contains(key.getID())).collect(Collectors.toSet());
    } catch (CloudStorageException ex) {
      throw new StoreException(ex, StoreErrorCodes.IOError);
    }
  }

  @Override
  public StoreStats getStoreStats() {
    return null;
  }

  @Override
  public boolean isKeyDeleted(StoreKey key) throws StoreException {
    checkStarted();
    // Return false for now, because we don't track recently deleted keys
    // This way, the replica thread will replay the delete resulting in a no-op
    // TODO: Consider LRU cache of recently deleted keys
    return false;
  }

  @Override
  public long getSizeInBytes() {
    throw new UnsupportedOperationException("Method not supported");
  }

  @Override
  public boolean isEmpty() {
    // TODO: query destination stats in start method
    return false;
  }

  @Override
  public void shutdown() {
    started = false;
  }

  private void checkStarted() throws StoreException {
    if (!started) {
      throw new StoreException("Store not started", StoreErrorCodes.Store_Not_Started);
    }
  }

  /**
   * Detects duplicates in {@code writeSet}
   * @param writeSet the {@link MessageWriteSet} to detect duplicates in
   * @throws IllegalArgumentException if a duplicate is detected
   */
  private void checkDuplicates(MessageWriteSet writeSet) {
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

  private class CloudWriteChannel implements Write {
    private final CloudBlobStore cloudBlobStore;
    private final List<MessageInfo> messageInfoList;
    private int messageIndex = 0;

    CloudWriteChannel(CloudBlobStore cloudBlobStore, List<MessageInfo> messageInfoList) {
      this.cloudBlobStore = cloudBlobStore;
      this.messageInfoList = messageInfoList;
    }

    @Override
    public int appendFrom(ByteBuffer buffer) throws StoreException {
      throw new UnsupportedOperationException("Method not supported");
    }

    @Override
    public void appendFrom(ReadableByteChannel channel, long size) throws StoreException {
      // Upload the blob corresponding to the current message index
      MessageInfo messageInfo = messageInfoList.get(messageIndex);
      if (messageInfo.getSize() != size) {
        throw new IllegalStateException("Mismatched buffer length for blob: " + messageInfo.getStoreKey().getID());
      }
      ByteBuffer messageBuf = ByteBuffer.allocate((int) size);
      int bytesRead = 0;
      try {
        while (bytesRead < size) {
          int readResult = channel.read(messageBuf);
          if (readResult == -1) {
            throw new IOException(
                "Channel read returned -1 before reading expected number of bytes, blobId=" + messageInfo.getStoreKey()
                    .getID());
          }
          bytesRead += readResult;
        }
        cloudBlobStore.putBlob(messageInfo, messageBuf, size);
        messageIndex++;
      } catch (IOException | CloudStorageException | GeneralSecurityException e) {
        throw new StoreException(e.getMessage(), StoreErrorCodes.IOError);
      }
    }
  }
}
