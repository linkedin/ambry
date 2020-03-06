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

import com.codahale.metrics.Timer;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaState;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
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
import com.github.ambry.store.Write;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.ByteBufferOutputStream;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.cloud.CloudBlobMetadata.*;


/**
 * The blob store that reflects data in a cloud storage.
 */
class CloudBlobStore implements Store {

  private static final Logger logger = LoggerFactory.getLogger(CloudBlobStore.class);
  private static final int cacheInitialCapacity = 1000;
  private static final float cacheLoadFactor = 0.75f;
  private final PartitionId partitionId;
  private final CloudDestination cloudDestination;
  private final ClusterMap clusterMap;
  private final CloudBlobCryptoAgentFactory cryptoAgentFactory;
  private final CloudBlobCryptoAgent cryptoAgent;
  private final CloudRequestAgent requestAgent;
  private final VcrMetrics vcrMetrics;

  // Map blobId to state (created, ttlUpdated, deleted)
  private final Map<String, BlobState> recentBlobCache;
  private final long minTtlMillis;
  private final boolean requireEncryption;
  // For live serving mode, implement retries and disable caching
  private final boolean isVcr;
  private boolean started;

  /**
   * Constructor for CloudBlobStore
   * @param properties the {@link VerifiableProperties} to use.
   * @param partitionId partition associated with BlobStore.
   * @param cloudDestination the {@link CloudDestination} to use.
   * @param clusterMap the {@link ClusterMap} to use.
   * @param vcrMetrics the {@link VcrMetrics} to use.
   * @throws IllegalStateException if construction failed.
   */
  CloudBlobStore(VerifiableProperties properties, PartitionId partitionId, CloudDestination cloudDestination,
      ClusterMap clusterMap, VcrMetrics vcrMetrics) throws IllegalStateException {
    CloudConfig cloudConfig = new CloudConfig(properties);
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(properties);
    this.clusterMap = clusterMap;
    this.cloudDestination = Objects.requireNonNull(cloudDestination, "cloudDestination is required");
    this.partitionId = Objects.requireNonNull(partitionId, "partitionId is required");
    this.vcrMetrics = Objects.requireNonNull(vcrMetrics, "vcrMetrics is required");
    minTtlMillis = TimeUnit.DAYS.toMillis(cloudConfig.vcrMinTtlDays);
    requireEncryption = cloudConfig.vcrRequireEncryption;
    isVcr = cloudConfig.cloudIsVcr;
    if (isVcr) {
      recentBlobCache = Collections.synchronizedMap(new RecentBlobCache(cloudConfig.recentBlobCacheLimit));
    } else {
      recentBlobCache = Collections.emptyMap();
    }
    requestAgent = new CloudRequestAgent(cloudConfig, vcrMetrics);

    String cryptoAgentFactoryClass = cloudConfig.cloudBlobCryptoAgentFactoryClass;
    try {
      cryptoAgentFactory = Utils.getObj(cryptoAgentFactoryClass, properties, clusterMapConfig.clusterMapClusterName,
          vcrMetrics.getMetricRegistry());
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Unable to construct factory " + cryptoAgentFactoryClass, e);
    }
    this.cryptoAgent = cryptoAgentFactory.getCloudBlobCryptoAgent();
  }

  @Override
  public void start() {
    started = true;
    logger.info("Started store: {}", this.toString());
  }

  @Override
  public StoreInfo get(List<? extends StoreKey> ids, EnumSet<StoreGetOptions> storeGetOptions) throws StoreException {
    checkStarted();
    checkStoreKeyDuplicates(ids);
    List<CloudMessageReadSet.BlobReadInfo> blobReadInfos = new ArrayList<>(ids.size());
    List<MessageInfo> messageInfos = new ArrayList<>(ids.size());
    try {
      List<BlobId> blobIdList = ids.stream().map(key -> (BlobId) key).collect(Collectors.toList());
      Map<String, CloudBlobMetadata> cloudBlobMetadataListMap =
          requestAgent.doWithRetries(() -> cloudDestination.getBlobMetadata(blobIdList), "GetBlobMetadata",
              partitionId.toPathString());
      if (cloudBlobMetadataListMap.size() < blobIdList.size()) {
        Set<BlobId> missingBlobs = blobIdList.stream()
            .filter(blobId -> !cloudBlobMetadataListMap.containsKey(blobId))
            .collect(Collectors.toSet());
        throw new StoreException("Some of the keys were missing in the cloud metadata store: " + missingBlobs,
            StoreErrorCodes.ID_Not_Found);
      }
      long currentTimeStamp = System.currentTimeMillis();
      validateCloudMetadata(cloudBlobMetadataListMap, storeGetOptions, currentTimeStamp);
      for (BlobId blobId : blobIdList) {
        CloudBlobMetadata blobMetadata = cloudBlobMetadataListMap.get(blobId.getID());
        // TODO: need to add ttlUpdated to CloudBlobMetadata so we can use it here
        // For now, set ttlUpdated = true for all permanent blobs, so the correct ttl
        // is applied by GetOperation.
        boolean ttlUpdated = blobMetadata.getExpirationTime() == Utils.Infinite_Time;
        boolean deleted = blobMetadata.getDeletionTime() != Utils.Infinite_Time;
        MessageInfo messageInfo =
            new MessageInfo(blobId, blobMetadata.getSize(), deleted, ttlUpdated, blobMetadata.getExpirationTime(),
                (short) blobMetadata.getAccountId(), (short) blobMetadata.getContainerId(),
                getOperationTime(blobMetadata));
        messageInfos.add(messageInfo);
        blobReadInfos.add(new CloudMessageReadSet.BlobReadInfo(blobMetadata, blobId));
      }
    } catch (CloudStorageException e) {
      throw new StoreException(e, StoreErrorCodes.IOError);
    }
    CloudMessageReadSet messageReadSet = new CloudMessageReadSet(blobReadInfos, this);
    return new StoreInfo(messageReadSet, messageInfos);
  }

  /**
   * Download the blob corresponding to the {@code blobId} from the {@code CloudDestination} to the given {@code outputStream}
   * If the blob was encrypted by vcr during upload, then this method also decrypts it.
   * @param cloudBlobMetadata blob metadata to determine if the blob was encrypted by vcr during upload.
   * @param blobId Id of the blob to the downloaded.
   * @param outputStream {@code OutputStream} of the donwloaded blob.
   * @throws StoreException if there is an error in downloading the blob.
   */
  void downloadBlob(CloudBlobMetadata cloudBlobMetadata, BlobId blobId, OutputStream outputStream)
      throws StoreException {
    try {
      // TODO: for GET ops, avoid extra trip to fetch metadata unless config flag is set
      // TODO: if needed, fetch metadata here and check encryption
      if (cloudBlobMetadata.getEncryptionOrigin() == EncryptionOrigin.VCR) {
        ByteBuffer encryptedBlob = ByteBuffer.allocate((int) cloudBlobMetadata.getEncryptedSize());
        requestAgent.doWithRetries(() -> {
          cloudDestination.downloadBlob(blobId, new ByteBufferOutputStream(encryptedBlob));
          return null;
        }, "Download", cloudBlobMetadata.getPartitionId());
        ByteBuffer decryptedBlob = cryptoAgent.decrypt(encryptedBlob);
        outputStream.write(decryptedBlob.array());
      } else {
        requestAgent.doWithRetries(() -> {
          cloudDestination.downloadBlob(blobId, outputStream);
          return null;
        }, "Download", cloudBlobMetadata.getPartitionId());
      }
    } catch (CloudStorageException | GeneralSecurityException | IOException e) {
      throw new StoreException("Error occured in downloading blob for blobid :" + blobId, StoreErrorCodes.IOError);
    }
  }

  /**
   * validate the {@code CloudBlobMetadata} map to make sure it has metadata for all keys, and they meet the {@code storeGetOptions} requirements.
   * @param cloudBlobMetadataListMap
   * @throws StoreException if the {@code CloudBlobMetadata} isnt valid
   */
  private void validateCloudMetadata(Map<String, CloudBlobMetadata> cloudBlobMetadataListMap,
      EnumSet<StoreGetOptions> storeGetOptions, long currentTimestamp) throws StoreException {
    for (String key : cloudBlobMetadataListMap.keySet()) {
      if (isBlobDeleted(cloudBlobMetadataListMap.get(key)) && !storeGetOptions.contains(
          StoreGetOptions.Store_Include_Deleted)) {
        throw new StoreException("Id " + key + " has been deleted on the cloud", StoreErrorCodes.ID_Deleted);
      }
      if (isBlobExpired(cloudBlobMetadataListMap.get(key), currentTimestamp) && !storeGetOptions.contains(
          StoreGetOptions.Store_Include_Expired)) {
        throw new StoreException("Id " + key + " has expired on the cloud", StoreErrorCodes.TTL_Expired);
      }
    }
  }

  /**
   * Gets the operation time for a blob from blob metadata based on the blob's current state and timestamp recorded for that state.
   * @param metadata blob metadata from which to derive operation time.
   * @return operation time.
   */
  private long getOperationTime(CloudBlobMetadata metadata) {
    if (isBlobDeleted(metadata)) {
      return metadata.getDeletionTime();
    }
    return (metadata.getCreationTime() == Utils.Infinite_Time) ? metadata.getUploadTime() : metadata.getCreationTime();
  }

  /**
   * Check if the blob is marked for deletion in its metadata
   * @param metadata to check for deletion
   * @return true if deleted. false otherwise
   */
  static boolean isBlobDeleted(CloudBlobMetadata metadata) {
    return metadata.getDeletionTime() != Utils.Infinite_Time;
  }

  /**
   * Check if the blob is expired
   * @param metadata to check for expiration
   * @return true if expired. false otherwise
   */
  static boolean isBlobExpired(CloudBlobMetadata metadata, long currentTimeStamp) {
    return metadata.getExpirationTime() != Utils.Infinite_Time && metadata.getExpirationTime() < currentTimeStamp;
  }

  /**
   * Puts a set of messages into the store
   * @param messageSetToWrite The message set to write to the store
   * @throws StoreException
   */
  @Override
  public void put(MessageWriteSet messageSetToWrite) throws StoreException {
    checkStarted();
    if (messageSetToWrite.getMessageSetInfo().isEmpty()) {
      throw new IllegalArgumentException("Message write set cannot be empty");
    }
    checkDuplicates(messageSetToWrite.getMessageSetInfo());

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
      throws CloudStorageException, IOException {
    if (shouldUpload(messageInfo)) {
      BlobId blobId = (BlobId) messageInfo.getStoreKey();
      boolean isRouterEncrypted = isRouterEncrypted(blobId);
      EncryptionOrigin encryptionOrigin = isRouterEncrypted ? EncryptionOrigin.ROUTER : EncryptionOrigin.NONE;
      boolean encryptThisBlob = requireEncryption && !isRouterEncrypted;
      if (encryptThisBlob) {
        // Need to encrypt the buffer before upload
        long encryptedSize = -1;
        Timer.Context encryptionTimer = vcrMetrics.blobEncryptionTime.time();
        try {
          messageBuf = cryptoAgent.encrypt(messageBuf);
          encryptedSize = messageBuf.remaining();
        } catch (GeneralSecurityException ex) {
          vcrMetrics.blobEncryptionErrorCount.inc();
        } finally {
          encryptionTimer.stop();
        }
        vcrMetrics.blobEncryptionCount.inc();
        CloudBlobMetadata blobMetadata =
            new CloudBlobMetadata(blobId, messageInfo.getOperationTimeMs(), messageInfo.getExpirationTimeInMs(),
                messageInfo.getSize(), EncryptionOrigin.VCR, cryptoAgent.getEncryptionContext(),
                cryptoAgentFactory.getClass().getName(), encryptedSize);
        // If buffer was encrypted, we no longer know its size
        long bufferlen = (encryptedSize == -1) ? size : encryptedSize;
        uploadBlobInternal(blobId, bufferlen, blobMetadata, new ByteBufferInputStream(messageBuf));
      } else {
        // Upload blob as is
        CloudBlobMetadata blobMetadata =
            new CloudBlobMetadata(blobId, messageInfo.getOperationTimeMs(), messageInfo.getExpirationTimeInMs(),
                messageInfo.getSize(), encryptionOrigin);
        uploadBlobInternal(blobId, size, blobMetadata, new ByteBufferInputStream(messageBuf));
      }
      addToCache(blobId.getID(), BlobState.CREATED);
    } else {
      logger.trace("Blob is skipped: {}", messageInfo);
      vcrMetrics.blobUploadSkippedCount.inc();
    }
  }

  private void uploadBlobInternal(BlobId blobId, long bufferlen, CloudBlobMetadata blobMetadata,
      InputStream inputStream) throws CloudStorageException {
    requestAgent.doWithRetries(() -> cloudDestination.uploadBlob(blobId, bufferlen, blobMetadata, inputStream),
        "Upload", partitionId.toPathString());
  }

  /**
   * Utility to check whether a blob was already encrypted by the router.
   * @param blobId the blob to check.
   * @return True if the blob is encrypted, otherwise false.
   */
  private static boolean isRouterEncrypted(BlobId blobId) throws IOException {
    // TODO: would be more efficient to call blobId.isEncrypted()
    return blobId.getVersion() >= BlobId.BLOB_ID_V4 && BlobId.isEncrypted(blobId.getID());
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
    if (recentBlobCache.containsKey(messageInfo.getStoreKey().getID())) {
      return false;
    }
    // expiration time above threshold. Expired blobs are blocked by ReplicaThread.
    // FIXME: for !isVcr, ignore expiration time
    return (messageInfo.getExpirationTimeInMs() == Utils.Infinite_Time
        || messageInfo.getExpirationTimeInMs() - messageInfo.getOperationTimeMs() >= minTtlMillis);
  }

  @Override
  public void delete(List<MessageInfo> infos) throws StoreException {
    checkStarted();
    checkDuplicates(infos);

    try {
      for (MessageInfo msgInfo : infos) {
        BlobId blobId = (BlobId) msgInfo.getStoreKey();
        String blobKey = msgInfo.getStoreKey().getID();
        BlobState blobState = recentBlobCache.get(blobKey);
        if (blobState != BlobState.DELETED) {
          requestAgent.doWithRetries(() -> cloudDestination.deleteBlob(blobId, msgInfo.getOperationTimeMs()), "Delete",
              partitionId.toPathString());
          addToCache(blobKey, BlobState.DELETED);
        }
      }
    } catch (CloudStorageException ex) {
      throw new StoreException(ex, StoreErrorCodes.IOError);
    }
  }

  @Override
  public short undelete(MessageInfo info) throws StoreException {
    throw new UnsupportedOperationException("Undelete not supported in cloud store");
  }

  /**
   * {@inheritDoc}
   * Currently, the only supported operation is to set the TTL to infinite (i.e. no arbitrary increase or decrease)
   * @param infos The list of messages that need to be updated
   * @throws StoreException
   */
  @Override
  public void updateTtl(List<MessageInfo> infos) throws StoreException {
    checkStarted();
    // Note: we skipped uploading the blob on PUT record if the TTL was below threshold.
    try {
      for (MessageInfo msgInfo : infos) {
        // MessageInfo.expirationTimeInMs is not reliable if ttlUpdate is set. See {@code PersistentIndex#findKey()}
        // and {@code PersistentIndex#markAsPermanent()}. If we change updateTtl to be more flexible, code here will
        // need to be modified.
        if (msgInfo.isTtlUpdated()) {
          BlobId blobId = (BlobId) msgInfo.getStoreKey();
          BlobState blobState = recentBlobCache.get(blobId.getID());
          if (blobState == null || blobState == BlobState.CREATED) {
            requestAgent.doWithRetries(() -> cloudDestination.updateBlobExpiration(blobId, Utils.Infinite_Time),
                "UpdateTtl", partitionId.toPathString());
            addToCache(blobId.getID(), BlobState.TTL_UPDATED);
          }
        } else {
          logger.error("updateTtl() is called but msgInfo.isTtlUpdated is not set. msgInfo: {}", msgInfo);
          vcrMetrics.updateTtlNotSetError.inc();
        }
      }
    } catch (CloudStorageException ex) {
      throw new StoreException(ex, StoreErrorCodes.IOError);
    }
  }

  /**
   * Add a blob state mapping to the recent blob cache.
   * @param blobKey the blob key to cache.
   * @param blobState the state of the blob.
   */
  // Visible for test
  void addToCache(String blobKey, BlobState blobState) {
    if (isVcr) {
      recentBlobCache.put(blobKey, blobState);
    }
  }

  @Override
  public FindInfo findEntriesSince(FindToken token, long maxTotalSizeOfEntries) throws StoreException {
    try {
      FindResult findResult = requestAgent.doWithRetries(
          () -> cloudDestination.findEntriesSince(partitionId.toPathString(), token, maxTotalSizeOfEntries),
          "FindEntriesSince", partitionId.toPathString());
      if (findResult.getMetadataList().isEmpty()) {
        return new FindInfo(Collections.emptyList(), findResult.getUpdatedFindToken());
      }
      List<MessageInfo> messageEntries = new ArrayList<>();
      for (CloudBlobMetadata metadata : findResult.getMetadataList()) {
        messageEntries.add(getMessageInfoFromMetadata(metadata));
      }
      return new FindInfo(messageEntries, findResult.getUpdatedFindToken());
    } catch (CloudStorageException | IOException ex) {
      throw new StoreException(ex, StoreErrorCodes.IOError);
    }
  }

  /**
   * Create {@link MessageInfo} object from {@link CloudBlobMetadata} object.
   * @param metadata {@link CloudBlobMetadata} object.
   * @return {@link MessageInfo} object.
   * @throws IOException
   */
  private MessageInfo getMessageInfoFromMetadata(CloudBlobMetadata metadata) throws IOException {
    BlobId blobId = new BlobId(metadata.getId(), clusterMap);
    long operationTime = (metadata.getDeletionTime() > 0) ? metadata.getDeletionTime()
        : (metadata.getCreationTime() > 0) ? metadata.getCreationTime() : metadata.getUploadTime();
    boolean isDeleted = metadata.getDeletionTime() > 0;
    boolean isTtlUpdated = false;  // No way to know
    return new MessageInfo(blobId, metadata.getSize(), isDeleted, isTtlUpdated, metadata.getExpirationTime(),
        (short) metadata.getAccountId(), (short) metadata.getContainerId(), operationTime);
  }

  @Override
  public Set<StoreKey> findMissingKeys(List<StoreKey> keys) throws StoreException {
    checkStarted();
    // Check existence of keys in cloud metadata
    List<BlobId> blobIdQueryList = keys.stream()
        .filter(key -> !recentBlobCache.containsKey(key.getID()))
        .map(key -> (BlobId) key)
        .collect(Collectors.toList());
    if (blobIdQueryList.isEmpty()) {
      // Cool, the cache did its job and eliminated a Cosmos query!
      return Collections.emptySet();
    }
    try {

      Set<String> foundSet =
          requestAgent.doWithRetries(() -> cloudDestination.getBlobMetadata(blobIdQueryList), "FindMissingKeys",
              partitionId.toPathString()).keySet();
      // return input keys - cached keys - keys returned by query
      return keys.stream()
          .filter(key -> !foundSet.contains(key.getID()))
          .filter(key -> !recentBlobCache.containsKey(key.getID()))
          .collect(Collectors.toSet());
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
    // Not definitive, but okay for some deletes to be replayed.
    return (BlobState.DELETED == recentBlobCache.get(key.getID()));
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
    throw new UnsupportedOperationException("Method not supported");
  }

  @Override
  public long getEndPositionOfLastPut() throws StoreException {
    throw new UnsupportedOperationException("Method not supported");
  }

  @Override
  public boolean recoverFromDecommission() {
    throw new UnsupportedOperationException("Method not supported");
  }

  @Override
  public void shutdown() {
    recentBlobCache.clear();
    started = false;
    logger.info("Stopped store: {}", this.toString());
  }

  @Override
  public boolean isStarted() {
    return started;
  }

  private void checkStarted() throws StoreException {
    if (!started) {
      throw new StoreException("Store not started", StoreErrorCodes.Store_Not_Started);
    }
  }

  /**
   * Detects duplicates in {@code writeSet}
   * @param infos the list of {@link MessageInfo} to detect duplicates in
   * @throws IllegalArgumentException if a duplicate is detected
   */
  private void checkDuplicates(List<MessageInfo> infos) throws IllegalArgumentException {
    List<StoreKey> keys = infos.stream().map(info -> info.getStoreKey()).collect(Collectors.toList());
    checkStoreKeyDuplicates(keys);
  }

  /**
   * Detects duplicates in {@code keys}
   * @param keys list of {@link StoreKey} to detect duplicates in
   * @throws IllegalArgumentException if a duplicate is detected
   */
  private void checkStoreKeyDuplicates(List<? extends StoreKey> keys) throws IllegalArgumentException {
    if (keys.size() > 1) {
      new HashSet<>();
      Set<StoreKey> seenKeys = new HashSet<>();
      Set<StoreKey> duplicates = keys.stream().filter(key -> !seenKeys.add(key)).collect(Collectors.toSet());
      if (duplicates.size() > 0) {
        throw new IllegalArgumentException("list contains duplicates. Duplicates detected: " + duplicates);
      }
    }
  }

  @Override
  public String toString() {
    return "PartitionId: " + partitionId.toPathString() + " in the cloud";
  }

  /** The lifecycle state of a recently seen blob. */
  enum BlobState {CREATED, TTL_UPDATED, DELETED}

  /** A {@link Write} implementation used by this store to write data. */
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
        messageBuf.flip();
        cloudBlobStore.putBlob(messageInfo, messageBuf, size);
        messageIndex++;
      } catch (IOException | CloudStorageException e) {
        throw new StoreException(e, StoreErrorCodes.IOError);
      }
    }
  }

  /**
   * A local LRA cache of recent blobs processed by this store.
   */
  private class RecentBlobCache extends LinkedHashMap<String, BlobState> {
    private final int maxEntries;

    public RecentBlobCache(int maxEntries) {
      // Use access order for eviction
      super(cacheInitialCapacity, cacheLoadFactor, true);
      this.maxEntries = maxEntries;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<String, BlobState> eldest) {
      return (this.size() > maxEntries);
    }
  }
}
