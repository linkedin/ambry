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

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaState;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.MessageFormatWriteSet;
import com.github.ambry.replication.FindToken;
import com.github.ambry.store.FindInfo;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.MessageWriteSet;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreErrorCodes;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreGetOptions;
import com.github.ambry.store.StoreInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreStats;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.cloud.CloudBlobMetadata.*;


/**
 * The blob store that reflects data in a cloud storage.
 */
public class CloudBlobStore implements Store {

  private static final Logger logger = LoggerFactory.getLogger(CloudBlobStore.class);
  private final CloudDestination cloudDestination;
  private volatile ReplicaState currentState = ReplicaState.OFFLINE;

  /**
   * Constructor for CloudBlobStore
   * @param properties the {@link VerifiableProperties} to use.
   * @param partitionId partition associated with BlobStore.
   * @param cloudDestination the {@link CloudDestination} to use.
   * @param clusterMap the {@link ClusterMap} to use.
   * @param vcrMetrics the {@link VcrMetrics} to use.
   * @throws IllegalStateException if construction failed.
   */
  public CloudBlobStore(VerifiableProperties properties, PartitionId partitionId, CloudDestination cloudDestination,
      ClusterMap clusterMap, VcrMetrics vcrMetrics) throws IllegalStateException {
    this.cloudDestination = Objects.requireNonNull(cloudDestination, "cloudDestination is required");
  }

  @Override
  public void start() {
    currentState = ReplicaState.STANDBY;
    logger.debug("Started store: {}", this);
  }

  @Override
  public StoreInfo get(List<? extends StoreKey> ids, EnumSet<StoreGetOptions> storeGetOptions) throws StoreException {
    throw new UnsupportedOperationException();
  }

  public CompletableFuture<StoreInfo> getAsync(List<? extends StoreKey> ids, EnumSet<StoreGetOptions> storeGetOptions) {
    throw new UnsupportedOperationException();
  }

  public void downloadBlob(CloudBlobMetadata cloudBlobMetadata, BlobId blobId, OutputStream outputStream)
      throws StoreException {
    throw new UnsupportedOperationException();
  }

  CompletableFuture<Void> downloadBlobAsync(CloudBlobMetadata cloudBlobMetadata, BlobId blobId,
      OutputStream outputStream) {
    throw new UnsupportedOperationException();
  }

  /**
   * Puts a set of messages into the store
   * @param messageSetToWrite The message set to write to the store
   * @throws StoreException
   */
  @Override
  public void put(MessageWriteSet messageSetToWrite) throws StoreException {
    try {
      cloudDestination.uploadBlobs((MessageFormatWriteSet) messageSetToWrite);
    } catch (CloudStorageException e) {
      throw new StoreException(e, StoreErrorCodes.IOError);
    }
  }

  public CompletableFuture<Void> putAsync(MessageWriteSet messageSetToWrite) {
    throw new UnsupportedOperationException();
  }


  @Override
  public void delete(List<MessageInfo> infos) throws StoreException {
    try {
      for (MessageInfo msg : infos) {
        cloudDestination.deleteBlob((BlobId) msg.getStoreKey(), msg.getOperationTimeMs(), msg.getLifeVersion(), null);
      }
    } catch (CloudStorageException cse) {
      if (cse.getCause() instanceof StoreException) {
        throw (StoreException) cse.getCause();
      }
      throw new StoreException(cse.getCause(), StoreErrorCodes.IOError);
    }
  }

  @Override
  public void purge(List<MessageInfo> infosToPurge) throws StoreException {
    throw new UnsupportedOperationException("Method not supported");
  }

  @Override
  public void forceDelete(List<MessageInfo> infosToDelete) throws StoreException {
    throw new UnsupportedOperationException("Method not supported");
  }

  public CompletableFuture<Void> deleteAsync(List<MessageInfo> infos) {
    throw new UnsupportedOperationException();
  }


  @Override
  public short undelete(MessageInfo info) throws StoreException {
    // TODO: Remove the duplicate code by calling unDeleteAsync() method.
    checkStarted();
    try {
      return cloudDestination.undeleteBlob((BlobId) info.getStoreKey(), info.getLifeVersion(), null);
    } catch (CloudStorageException cse) {
      if (cse.getCause() instanceof StoreException) {
        throw (StoreException) cse.getCause();
      }
      throw new StoreException(cse.getCause(), StoreErrorCodes.IOError);
    }
  }

  /**
   * Undelete the blob identified by {@code id} in the store asynchronously. When the lifeVersion is
   * {@link MessageInfo#LIFE_VERSION_FROM_FRONTEND}, this method is invoked by the responding to the frontend request.
   * Otherwise, it's invoked in the replication thread.
   * @param info The {@link MessageInfo} that carries some basic information about this operation. Only the StoreKey,
   *             OperationTime, LifeVersion should be used in this method.
   * @return a {@link CompletableFuture} that will eventually contain the lifeVersion of the undeleted blob or the
   *         {@link StoreException} if an error occurred.
   */
  public CompletableFuture<Short> undeleteAsync(MessageInfo info) {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   * Currently, the only supported operation is to set the TTL to infinite (i.e. no arbitrary increase or decrease)
   * @param infos The list of messages that need to be updated.
   * @throws StoreException
   */
  @Override
  public void updateTtl(List<MessageInfo> infos) throws StoreException {
    try {
      for (MessageInfo msg : infos) {
        cloudDestination.updateBlobExpiration((BlobId) msg.getStoreKey(), Utils.Infinite_Time, null);
      }
    } catch (CloudStorageException cse) {
      if (cse.getCause() instanceof StoreException) {
        throw (StoreException) cse.getCause();
      }
      throw new StoreException(cse.getCause(),  StoreErrorCodes.IOError);
    }
  }

  @Override
  public FindInfo findEntriesSince(FindToken token, long maxTotalSizeOfEntries, String hostname,
      String remoteReplicaPath) throws StoreException {
    return null;
  }

  /**
   * Updates the TTL of all the messages in the list in the store asynchronously. When the lifeVersion is
   * {@link MessageInfo#LIFE_VERSION_FROM_FRONTEND}, this method is invoked by the responding to the frontend request.
   * Otherwise, it's invoked in the replication thread.
   * @param infosToUpdate The list of messages that need to be updated. Only the StoreKey, OperationTime,
   *                      ExpirationTime, LifeVersion should be used in this method.
   * @return a {@link CompletableFuture} that will eventually complete successfully when all the TTL of all messages are
   *         updated successfully or will contain the {@link StoreException} if an error occurred.
   */
  public CompletableFuture<Void> updateTtlAsync(List<MessageInfo> infosToUpdate) {
    throw new UnsupportedOperationException();
  }



  @Override
  public Set<StoreKey> findMissingKeys(List<StoreKey> keys) throws StoreException {
    Set<StoreKey> missingKeys = new HashSet<>();
    keys.stream().forEach(k -> {
      try {
        if (!cloudDestination.getBlobMetadata(Collections.singletonList((BlobId) k)).containsKey(k.getID())) {
          missingKeys.add(k);
        }
      } catch (Throwable e) {
        // pass; error is handled or printed in getBlobMetadata
      }
    });
    return missingKeys;
  }

  @Override
  public MessageInfo findKey(StoreKey key) throws StoreException {
    try {
      Map<String, CloudBlobMetadata> cloudBlobMetadataListMap = cloudDestination.getBlobMetadata(Collections.singletonList((BlobId) key));
      CloudBlobMetadata cloudBlobMetadata = cloudBlobMetadataListMap.get(key.getID());
      if (cloudBlobMetadata != null) {
        return new MessageInfo(key, cloudBlobMetadata.getSize(), cloudBlobMetadata.isDeleted(),
            cloudBlobMetadata.isTtlUpdated(), cloudBlobMetadata.isUndeleted(), cloudBlobMetadata.getExpirationTime(), null,
            (short) cloudBlobMetadata.getAccountId(), (short) cloudBlobMetadata.getContainerId(),
            cloudBlobMetadata.getLastUpdateTime(), cloudBlobMetadata.getLifeVersion());
      } else {
        throw new StoreException(String.format("FindKey couldn't find key: %s", key), StoreErrorCodes.ID_Not_Found);
      }
    } catch (CloudStorageException e) {
      throw new StoreException(e, StoreErrorCodes.IOError);
    }
  }

  @Override
  public Map<String, MessageInfo> findAllMessageInfosForKey(StoreKey key) throws StoreException {
    return Store.super.findAllMessageInfosForKey(key);
  }

  @Override
  public StoreStats getStoreStats() {
    return null;
  }

  @Override
  public boolean isKeyDeleted(StoreKey key) throws StoreException {
    throw new UnsupportedOperationException();
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
  public boolean isBootstrapInProgress() {
    return false;
  }

  @Override
  public boolean isDecommissionInProgress() {
    return false;
  }

  @Override
  public void completeBootstrap() {
    // no op
  }

  @Override
  public void setCurrentState(ReplicaState state) {
    throw new UnsupportedOperationException("Method not supported");
  }

  @Override
  public ReplicaState getCurrentState() {
    return currentState;
  }

  @Override
  public long getEndPositionOfLastPut() {
    throw new UnsupportedOperationException("Method not supported");
  }

  @Override
  public boolean recoverFromDecommission() {
    throw new UnsupportedOperationException("Method not supported");
  }

  @Override
  public boolean isDisabled() {
    throw new UnsupportedOperationException("Method not supported");
  }

  @Override
  public void shutdown() {
    currentState = ReplicaState.OFFLINE;
    logger.info("Stopped store: {}", this.toString());
  }

  @Override
  public Long getBlobContentCRC(MessageInfo msg) throws StoreException, IOException {
    return Store.super.getBlobContentCRC(msg);
  }

  @Override
  public boolean isStarted() {
    return true;
  }

  private void checkStarted() throws StoreException {
    // There is no concept of start/stop Azure cloud partition
  }

  @Override
  public String toString() {
    return "";
  }

}
