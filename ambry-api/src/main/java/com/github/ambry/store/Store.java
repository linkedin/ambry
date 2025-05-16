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
package com.github.ambry.store;

import com.github.ambry.clustermap.ReplicaState;
import com.github.ambry.replication.FindToken;
import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * The object store's underlying store
 */
public interface Store {

  /**
   * Initializes the store without starting it.
   * @throws StoreException
   */
  void initialize() throws StoreException;

  /**
   * Starts the store if it is initialized.
   * @throws StoreException
   */
  void load() throws StoreException;

  /**
   * Starts the store. It will initialize the store and then load the store.
   * @throws StoreException
   */
  void start() throws StoreException;

  /**
   * Returns the PartitionFileStore associated with the BlobStore
   * @throws StoreException exception
   */
  default PartitionFileStore getFileStore() throws StoreException {
    return null;
  }

  /**
   * Returns the store info for the given ids
   * @param ids The list of ids whose messages need to be retrieved
   * @param storeGetOptions A set of additional options that the store needs to use while getting the message
   * @return The store info for the given ids
   * @throws StoreException
   */
  StoreInfo get(List<? extends StoreKey> ids, EnumSet<StoreGetOptions> storeGetOptions) throws StoreException;

  /**
   * Puts a set of messages into the store. When the lifeVersion is {@link MessageInfo#LIFE_VERSION_FROM_FRONTEND}, this
   * method is invoked by the responding to the frontend request. Otherwise, it's invoked in the replication thread.
   * @param messageSetToWrite The message set to write to the store
   *                          Only the StoreKey, OperationTime, ExpirationTime, LifeVersion should be used in this method.
   * @throws StoreException
   */
  void put(MessageWriteSet messageSetToWrite) throws StoreException;

  /**
   * Deletes all the messages in the list. When the lifeVersion is {@link MessageInfo#LIFE_VERSION_FROM_FRONTEND}, this
   * method is invoked by the responding to the frontend request. Otherwise, it's invoked in the replication thread.
   * @param infosToDelete The list of messages that need to be deleted.
   *                      Only the StoreKey, OperationTime, LifeVersion should be used in this method.
   * @throws StoreException
   */
  void delete(List<MessageInfo> infosToDelete) throws StoreException;

  /**
   * Deletes all the messages in the list. When the lifeVersion is {@link MessageInfo#LIFE_VERSION_FROM_FRONTEND}, this
   * method is invoked by the responding to the frontend BATCH_DELETE request. Invocation in replication thread is not
   * yet supported.
   * @param infosToDelete The list of messages that need to be deleted.
   *                      Only the StoreKey, OperationTime, LifeVersion should be used in this method.
   * @return The {@link StoreBatchDeleteInfo} for the given ids
   * @throws StoreException
   */
  StoreBatchDeleteInfo batchDelete(List<MessageInfo> infosToDelete) throws StoreException;

  /**
   * Write a purge message for all the blob ids in the message info  list.
   * Blobs are purged when it has been determined that a chunk is no longer needed (i.e, is expired or deleted
   * and past retention window or maybe an orphan chunk). Once compaction sees a purge message, it will clean up the chunk
   * without any other checks.
   * @param infosToPurge The list of messages that need to be purged.
   * @throws StoreException
   */
  void purge(List<MessageInfo> infosToPurge) throws StoreException;

  /**
   * Force to write delete record for all the messages in the list when the Put record doesn't exist. Please note that
   * the message infos of the blobs should have valid life version (i.e not -1). For requests from frontend, set life
   * version to 0 before calling this method.
   * @param infosToDelete The list of messages that need to be deleted.
   * @throws StoreException
   */
  void forceDelete(List<MessageInfo> infosToDelete) throws StoreException;

  /**
   * Undelete the blob identified by {@code id}. When the lifeVersion is {@link MessageInfo#LIFE_VERSION_FROM_FRONTEND},
   * this method is invoked by the responding to the frontend request. Otherwise, it's invoked in the replication thread.
   * @param info The {@link MessageInfo} that carries some basic information about this operation.
   *             Only the StoreKey, OperationTime, LifeVersion should be used in this method.
   * @return the lifeVersion of the undeleted blob.
   */
  short undelete(MessageInfo info) throws StoreException;

  /**
   * Updates the TTL of all the messages in the list. When the lifeVersion is {@link MessageInfo#LIFE_VERSION_FROM_FRONTEND},
   * this method is invoked by the responding to the frontend request. Otherwise, it's invoked in the replication thread.
   * @param infosToUpdate The list of messages that need to be updated
   *                      Only the StoreKey, OperationTime, ExpirationTime, LifeVersion should be used in this method.
   * @throws StoreException
   */
  void updateTtl(List<MessageInfo> infosToUpdate) throws StoreException;

  /**
   * Finds all the entries from the store given a find token
   * @param token The token that acts as a bookmark to make subsequent searches
   * @param maxTotalSizeOfEntries The maximum total size of entries that needs to be returned. The api will try to
   *                              return a list of entries whose total size is close to this value.
   * @param hostname HostName of the datanode where the token belongs to.
   * @param remoteReplicaPath The path of remote replica.
   * @return The FindInfo instance that contains the entries found and the new token for future searches
   * @throws StoreException
   */
  FindInfo findEntriesSince(FindToken token, long maxTotalSizeOfEntries, String hostname, String remoteReplicaPath)
      throws StoreException;

  /**
   * Finds all the keys that are not present in the store from the input keys
   * @param keys The list of keys that need to be checked for existence
   * @return The list of keys that are not present in the store
   * @throws StoreException
   */
  Set<StoreKey> findMissingKeys(List<StoreKey> keys) throws StoreException;

  /**
   * Return {@link MessageInfo} of given key. This method will only be used in replication thread.
   * @param key The key of which blob to return {@link MessageInfo}.
   * @return The {@link MessageInfo}.
   * @throws StoreException
   */
  MessageInfo findKey(StoreKey key) throws StoreException;

  /**
   * Return all the MessageInfos associated with the given key in the store. The result is a map, whose key
   * should be string that would indicated the internal order of each MessageInfo value, if there is any internal
   * order.
   * @param key The key of which blob to return {@link MessageInfo}.
   * @return A map of {@link MessageInfo} as value and an ordered key.
   * @throws StoreException
   */
  default Map<String, MessageInfo> findAllMessageInfosForKey(StoreKey key) throws StoreException {
    MessageInfo info = findKey(key);
    Map<String, MessageInfo> result = new HashMap<>();
    result.put("0", info);
    return result;
  }

  /**
   * Get the corresponding {@link StoreStats} instance for this store.
   * @return a {@link StoreStats} instance which can be used to fetch store related stats
   */
  StoreStats getStoreStats();

  /**
   * Checks if the key is deleted. Returns true is the key is deleted. Returns false if the
   * key is present, not available, ttl expired.
   * @param key The key that needs to be checked for deletion state
   * @return True, if the key is deleted, false otherwise
   * @throws StoreException
   */
  boolean isKeyDeleted(StoreKey key) throws StoreException;

  /**
   * Returns the size of the store in bytes
   * @return The size of the store in bytes
   */
  long getSizeInBytes();

  /**
   * @return absolute end position of last PUT in bytes.
   * @throws StoreException
   */
  long getEndPositionOfLastPut() throws StoreException;

  /**
   * @return true if the store contains no data
   */
  boolean isEmpty();

  /**
   * @return true if the store is initialized
   */
  boolean isInitialized();

  /**
   * @return true if the store is started
   */
  boolean isStarted();

  /**
   * @return {@code true} if store has initiated bootstrap process and bootstrap is still in progress.
   */
  boolean isBootstrapInProgress();

  /**
   * @return {@code true} if store has initiated decommission process (STANDBY -> INACTIVE -> OFFLINE) and decommission
   * is still in progress.
   */
  boolean isDecommissionInProgress();

  /**
   * Take actions (if any) to complete the bootstrap (i.e, delete bootstrap file in store directory)
   */
  void completeBootstrap();

  /**
   * Set current state of the store.
   * @param state {@link ReplicaState} associated with local store
   */
  void setCurrentState(ReplicaState state);

  /**
   * @return current {@link ReplicaState} of the store
   */
  ReplicaState getCurrentState();

  /**
   * @return {@code true} if the store is recovering from previous decommission failure. {@code false} otherwise.
   */
  boolean recoverFromDecommission();

  /**
   * @return {@code true} if the store is disabled due to disk I/O error or by admin operation(i.e. stop replica).
   */
  boolean isDisabled();

  /**
   * @return {@code true} if the store recovers from partial log segment recovery failure.
   */
  default boolean hasPartialRecovery() {
    // By default it's false
    return false;
  }

  /**
   * Shuts down the store
   */
  void shutdown() throws StoreException;

  /**
   * Returns blob-content CRC
   * @param msg Metadata of blob
   * @return CRC of blob-content
   * @throws StoreException
   * @throws IOException
   */
  default Long getBlobContentCRC(MessageInfo msg) throws StoreException, IOException { return null; }

  /**
   * Returns the log segment metadata files
   * @param includeActiveLogSegment
   * @return
   */
  List<LogInfo> getLogSegmentMetadataFiles(boolean includeActiveLogSegment);

  /**
   * Returns True if compaction is in progress
   */
  boolean isCompactionInProgress();
}
