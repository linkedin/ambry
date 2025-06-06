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
import com.github.ambry.commons.FutureUtils;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.MessageFormatWriteSet;
import com.github.ambry.replication.FindToken;
import com.github.ambry.store.FindInfo;
import com.github.ambry.store.LogInfo;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.MessageWriteSet;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreBatchDeleteInfo;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.cloud.CloudBlobMetadata.*;


/**
 * The blob store that reflects data in a cloud storage.
 */
public class CloudBlobStore implements Store {

  private static final Logger logger = LoggerFactory.getLogger(CloudBlobStore.class);
  private static final int cacheInitialCapacity = 1000;
  private static final float cacheLoadFactor = 0.75f;
  public static final int STATUS_NOT_FOUND = 404;
  private static final short IGNORE_LIFE_VERSION = -2;
  private final PartitionId partitionId;
  private final CloudDestination cloudDestination;
  private final ClusterMap clusterMap;
  private final CloudBlobCryptoAgentFactory cryptoAgentFactory;
  private final CloudBlobCryptoAgent cryptoAgent;
  private final CloudRequestAgent requestAgent;
  private final VcrMetrics vcrMetrics;
  private final StoreConfig storeConfig;
  private final long ttlUpdateBufferTimeMs;

  // Map blobId to state (created, ttlUpdated, deleted)
  private final Map<String, BlobLifeState> recentBlobCache;
  private final long minTtlMillis;
  private final boolean requireEncryption;
  // Distinguishes between VCR and live serving mode
  private final boolean isVcr;
  private volatile ReplicaState currentState = ReplicaState.OFFLINE;
  private CloudConfig cloudConfig;

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
    this.cloudConfig = new CloudConfig(properties);
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(properties);
    this.clusterMap = clusterMap;
    this.storeConfig = new StoreConfig(properties);
    this.ttlUpdateBufferTimeMs = TimeUnit.SECONDS.toMillis(storeConfig.storeTtlUpdateBufferTimeSeconds);
    this.cloudDestination = Objects.requireNonNull(cloudDestination, "cloudDestination is required");
    this.partitionId = Objects.requireNonNull(partitionId, "partitionId is required");
    this.vcrMetrics = Objects.requireNonNull(vcrMetrics, "vcrMetrics is required");
    minTtlMillis = TimeUnit.DAYS.toMillis(cloudConfig.vcrMinTtlDays);
    requireEncryption = cloudConfig.vcrRequireEncryption;
    isVcr = cloudConfig.cloudIsVcr;
    recentBlobCache = null;
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
  public void initialize() {
    logger.debug("Initialized store: {}", this);
  }

  @Override
  public void load() {
    currentState = ReplicaState.STANDBY;
    logger.debug("Loaded the store: {}", this);
  }

  @Override
  public void start() {
    initialize();
    load();
    logger.debug("Started store: {}", this);
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
      throw new StoreException(e.getMessage(), StoreErrorCodes.IOError);
    }
  }

  /**
   * Delete blob
   * @param infos The list of messages that need to be deleted.
   * @throws StoreException
   */
  @Override
  public void delete(List<MessageInfo> infos) throws StoreException {
    try {
      for (MessageInfo msg : infos) {
        cloudDestination.deleteBlob((BlobId) msg.getStoreKey(), msg.getOperationTimeMs(), msg.getLifeVersion(), null);
      }
    } catch (StoreException e) {
      throw e;
    } catch (Throwable e) {
      throw new StoreException(e.getMessage(), StoreErrorCodes.IOError);
    }
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
    } catch (StoreException e) {
      throw e;
    } catch (Throwable e) {
      throw new StoreException(e.getMessage(),  StoreErrorCodes.IOError);
    }
  }

  /**
   * Undelete a blob
   * @param info The {@link MessageInfo} that carries some basic information about this operation.
   * @return
   * @throws StoreException
   */
  @Override
  public short undelete(MessageInfo info) throws StoreException {
    try {
      return cloudDestination.undeleteBlob((BlobId) info.getStoreKey(), info.getLifeVersion(), null);
    } catch (StoreException e) {
      throw e;
    } catch (Throwable e) {
      throw new StoreException(e.getMessage(), StoreErrorCodes.IOError);
    }
  }

  /**
   * Find missing keys
   * @param keys The list of keys that need to be checked for existence
   * @return
   * @throws StoreException
   */
  @Override
  public Set<StoreKey> findMissingKeys(List<StoreKey> keys) throws StoreException {
    // Keys may be missing if we are here, we are here to find out
    try {
      Set<StoreKey> missingKeys = new HashSet<>();
      for (StoreKey key : keys) {
        if (!cloudDestination.doesBlobExist((BlobId) key)) {
          missingKeys.add(key);
        }
      }
      return missingKeys;
    } catch (Throwable e) {
      throw new StoreException(e.getMessage(), StoreErrorCodes.IOError);
    }
  }

  /**
   * Find key metadata
   * @param key The key of which blob to return {@link MessageInfo}.
   * @return
   * @throws StoreException
   */
  @Override
  public MessageInfo findKey(StoreKey key) throws StoreException {
    try {
      // If we are here, the key must exist
      CloudBlobMetadata cloudBlobMetadata = cloudDestination.getCloudBlobMetadata((BlobId) key);
      return new MessageInfo(key, cloudBlobMetadata.getSize(), cloudBlobMetadata.isDeleted(),
          cloudBlobMetadata.isTtlUpdated(), cloudBlobMetadata.isUndeleted(), cloudBlobMetadata.getExpirationTime(),
          null, (short) cloudBlobMetadata.getAccountId(), (short) cloudBlobMetadata.getContainerId(),
          cloudBlobMetadata.getLastUpdateTime(), cloudBlobMetadata.getLifeVersion());
    } catch (CloudStorageException e) {
      switch (e.getStatusCode()) {
        case HttpStatus.SC_NOT_FOUND:
          throw new StoreException(e.getMessage(), StoreErrorCodes.IDNotFound);
        default:
          throw new StoreException(e.getMessage(), StoreErrorCodes.IOError);
      }
    } catch (Throwable e) {
      throw new StoreException(e.getMessage(), StoreErrorCodes.IOError);
    }
  }

  // UNUSED CODE

  @Override
  public StoreInfo get(List<? extends StoreKey> ids, EnumSet<StoreGetOptions> storeGetOptions) throws StoreException {
    // TODO: Remove the duplicate code by calling getAsync() method and wait on it once we verify that download works correctly.
    checkStarted();
    checkStoreKeyDuplicates(ids);
    List<CloudMessageReadSet.BlobReadInfo> blobReadInfos = new ArrayList<>(ids.size());
    List<MessageInfo> messageInfos = new ArrayList<>(ids.size());
    try {
      List<BlobId> blobIdList = ids.stream().map(key -> (BlobId) key).collect(Collectors.toList());
      Map<String, CloudBlobMetadata> cloudBlobMetadataListMap =
          requestAgent.doWithRetries(() -> cloudDestination.getBlobMetadata(blobIdList), "GetBlobMetadata",
              partitionId.toPathString());
      // Throw StoreException with ID_Not_Found if cloudBlobMetadataListMap size is less than expected.
      if (cloudBlobMetadataListMap.size() < blobIdList.size()) {
        Set<BlobId> missingBlobs = blobIdList.stream()
            .filter(blobId -> !cloudBlobMetadataListMap.containsKey(blobId))
            .collect(Collectors.toSet());
        throw new StoreException("Some of the keys were missing in the cloud metadata store: " + missingBlobs,
            StoreErrorCodes.IDNotFound);
      }
      long currentTimeStamp = System.currentTimeMillis();
      // Validate cloud meta data, may throw StoreException with ID_Deleted, TTL_Expired and Authorization_Failure
      validateCloudMetadata(cloudBlobMetadataListMap, storeGetOptions, currentTimeStamp, ids);
      for (BlobId blobId : blobIdList) {
        CloudBlobMetadata blobMetadata = cloudBlobMetadataListMap.get(blobId.getID());
        // TODO: need to add ttlUpdated to CloudBlobMetadata so we can use it here
        // For now, set ttlUpdated = true for all permanent blobs, so the correct ttl
        // is applied by GetOperation.
        boolean ttlUpdated = blobMetadata.getExpirationTime() == Utils.Infinite_Time;
        boolean deleted = blobMetadata.getDeletionTime() != Utils.Infinite_Time;
        MessageInfo messageInfo =
            new MessageInfo(blobId, blobMetadata.getSize(), deleted, ttlUpdated, blobMetadata.isUndeleted(),
                blobMetadata.getExpirationTime(), null, (short) blobMetadata.getAccountId(),
                (short) blobMetadata.getContainerId(), getOperationTime(blobMetadata), blobMetadata.getLifeVersion());
        messageInfos.add(messageInfo);
        blobReadInfos.add(new CloudMessageReadSet.BlobReadInfo(blobMetadata, blobId));
      }
    } catch (CloudStorageException e) {
      if (e.getCause() instanceof StoreException) {
        throw (StoreException) e.getCause();
      } else {
        throw new StoreException(e, StoreErrorCodes.IOError);
      }
    }
    CloudMessageReadSet messageReadSet = new CloudMessageReadSet(blobReadInfos, this);
    return new StoreInfo(messageReadSet, messageInfos);
  }

  /**
   * Returns the store info for the given ids asynchronously.
   * @param ids The list of ids whose messages need to be retrieved
   * @param storeGetOptions A set of additional options that the store needs to use while getting the message
   * @return a {@link CompletableFuture} that will eventually contain the {@link StoreInfo} for the given ids or the
   *         {@link StoreException} if an error occurred
   */
  public CompletableFuture<StoreInfo> getAsync(List<? extends StoreKey> ids, EnumSet<StoreGetOptions> storeGetOptions) {
    try {
      checkStarted();
      checkStoreKeyDuplicates(ids);

      List<MessageInfo> messageInfos = new ArrayList<>(ids.size());
      List<BlobId> blobIdList = ids.stream().map(key -> (BlobId) key).collect(Collectors.toList());

      return cloudDestination.getBlobMetadataAsync(blobIdList).handleAsync((cloudBlobMetadataListMap, throwable) -> {
        if (throwable != null) {
          unwrapCompletionExceptionAndThrowStoreException(throwable, CloudStorageException.class,
              this::mapToStoreException);
          return null;
        } else {
          try {
            // Metadata is fetched successfully. Validate the metadata list and return them to next completion stage.
            if (cloudBlobMetadataListMap.size() < blobIdList.size()) {
              // Throw StoreException with ID_Not_Found if cloudBlobMetadataListMap size is less than expected.
              Set<BlobId> missingBlobs = blobIdList.stream()
                  .filter(blobId -> !cloudBlobMetadataListMap.containsKey(blobId))
                  .collect(Collectors.toSet());
              throw new StoreException("Some of the keys were missing in the cloud metadata store: " + missingBlobs,
                  StoreErrorCodes.IDNotFound);
            }

            // Validate cloud meta data, may throw StoreException with ID_Deleted, TTL_Expired and Authorization_Failure
            long currentTimeStamp = System.currentTimeMillis();
            validateCloudMetadata(cloudBlobMetadataListMap, storeGetOptions, currentTimeStamp, ids);
          } catch (StoreException e) {
            throw new CompletionException(e);
          }

          List<CloudMessageReadSet.BlobReadInfo> blobReadInfos = new ArrayList<>(ids.size());
          for (BlobId blobId : blobIdList) {
            CloudBlobMetadata blobMetadata = cloudBlobMetadataListMap.get(blobId.getID());
            // TODO: need to add ttlUpdated to CloudBlobMetadata so we can use it here
            // For now, set ttlUpdated = true for all permanent blobs, so the correct ttl
            // is applied by GetOperation.
            boolean ttlUpdated = blobMetadata.getExpirationTime() == Utils.Infinite_Time;
            boolean deleted = blobMetadata.getDeletionTime() != Utils.Infinite_Time;
            MessageInfo messageInfo =
                new MessageInfo.Builder(blobId, blobMetadata.getSize(), (short) blobMetadata.getAccountId(),
                    (short) blobMetadata.getContainerId(), getOperationTime(blobMetadata)).isDeleted(deleted)
                    .isTtlUpdated(ttlUpdated)
                    .isUndeleted(blobMetadata.isUndeleted())
                    .expirationTimeInMs(blobMetadata.getExpirationTime())
                    .lifeVersion(blobMetadata.getLifeVersion())
                    .build();
            messageInfos.add(messageInfo);
            blobReadInfos.add(new CloudMessageReadSet.BlobReadInfo(blobMetadata, blobId));
          }
          return blobReadInfos;
        }
      }).thenCompose(blobReadInfos -> {
        // Since prefetch is enabled by default, it should be okay to download the content here. If we want to stream
        // content directly from Azure to output channel, it requires more refactoring of the cloud stack such as
        // passing the output channel to the CloudDestination, StorageClient, etc. We shouldn't need streaming now since
        // each blob stored in Azure is of max 4 MB (as large blobs are chunked to size of 4 MB at router).
        List<CompletableFuture<Void>> operationFutures = new ArrayList<>();
        blobReadInfos.forEach(blobReadInfo -> operationFutures.add(blobReadInfo.downloadBlobAsync(this)));
        return CompletableFuture.allOf(operationFutures.toArray(new CompletableFuture<?>[0])).thenApply(unused -> {
          CloudMessageReadSet messageReadSet = new CloudMessageReadSet(blobReadInfos, this);
          return new StoreInfo(messageReadSet, messageInfos);
        });
      });
    } catch (StoreException e) {
      return FutureUtils.completedExceptionally(e);
    }
  }

  /**
   * Download the blob corresponding to the {@code blobId} from the {@code CloudDestination} to the given {@code outputStream}
   * If the blob was encrypted by vcr during upload, then this method also decrypts it.
   * @param cloudBlobMetadata blob metadata to determine if the blob was encrypted by vcr during upload.
   * @param blobId Id of the blob to the downloaded.
   * @param outputStream {@code OutputStream} of the downloaded blob.
   * @throws StoreException if there is an error in downloading the blob.
   */
  public void downloadBlob(CloudBlobMetadata cloudBlobMetadata, BlobId blobId, OutputStream outputStream)
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
      throw new StoreException("Error occurred in downloading blob for blobid :" + blobId, StoreErrorCodes.IOError);
    }
  }

  /**
   * Download the blob corresponding to the {@code blobId} from the {@code CloudDestination} to the given
   * {@code outputStream} asynchronously. If the blob was encrypted by vcr during upload, then this method also decrypts
   * it.
   * @param cloudBlobMetadata blob metadata to determine if the blob was encrypted by vcr during upload.
   * @param blobId Id of the blob to the downloaded.
   * @param outputStream {@code OutputStream} of the downloaded blob.
   * @return a {@link CompletableFuture} that will eventually complete when the blob is downloaded to the given
   *         {@code outputStream} or will contain the {@link StoreException} if an error occurred
   */
  CompletableFuture<Void> downloadBlobAsync(CloudBlobMetadata cloudBlobMetadata, BlobId blobId,
      OutputStream outputStream) {
    // TODO: for GET ops, avoid extra trip to fetch metadata unless config flag is set
    // TODO: if needed, fetch metadata here and check encryption
    if (cloudBlobMetadata.getEncryptionOrigin() == EncryptionOrigin.VCR) {
      ByteBuffer encryptedBlob = ByteBuffer.allocate((int) cloudBlobMetadata.getEncryptedSize());
      return cloudDestination.downloadBlobAsync(blobId, new ByteBufferOutputStream(encryptedBlob))
          .handleAsync((unused, throwable) -> {
            // Since we may have to decrypt the blob, use handleAsync method to avoid blocking the response thread.
            if (throwable != null) {
              unwrapCompletionExceptionAndThrowStoreException(throwable, CloudStorageException.class,
                  cse -> new StoreException("Error occurred in downloading blob for blobId :" + blobId, cse,
                      StoreErrorCodes.IOError));
            }
            // Download is successful. Decrypt the blob
            ByteBuffer decryptedBlob;
            try {
              decryptedBlob = cryptoAgent.decrypt(encryptedBlob);
              outputStream.write(decryptedBlob.array());
            } catch (GeneralSecurityException | IOException e) {
              throw new CompletionException(
                  new StoreException("Error occurred in downloading blob for blobId :" + blobId,
                      StoreErrorCodes.IOError));
            }
            return unused;
          });
    } else {
      return cloudDestination.downloadBlobAsync(blobId, outputStream).exceptionally(throwable -> {
        unwrapCompletionExceptionAndThrowStoreException(throwable, CloudStorageException.class,
            cse -> new StoreException("Error occurred in downloading blob for blobId :" + blobId, cse,
                StoreErrorCodes.IOError));
        return null;
      });
    }
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
   * Puts a set of messages into the store asynchronously. When the lifeVersion is {@link MessageInfo#LIFE_VERSION_FROM_FRONTEND},
   * this method is invoked by the responding to the frontend request. Otherwise, it's invoked in the replication thread.
   * @param messageSetToWrite The message set to write to the store. Only the StoreKey, OperationTime, ExpirationTime,
   *                          LifeVersion should be used in this method.
   * @return a {@link CompletableFuture} that will eventually complete successfully when the messages are written to
   *         store or will contain the {@link StoreException} if an error occurred
   */
  public CompletableFuture<Void> putAsync(MessageWriteSet messageSetToWrite) {
    try {
      checkStarted();
      if (messageSetToWrite.getMessageSetInfo().isEmpty()) {
        throw new IllegalArgumentException("Message write set cannot be empty");
      }
      checkDuplicates(messageSetToWrite.getMessageSetInfo());

      CloudWriteChannel cloudWriter = new CloudWriteChannel(this, messageSetToWrite.getMessageSetInfo());
      // Write the blobs in the message set to cloud write channel asynchronously
      return ((CloudMessageFormatWriteSet) messageSetToWrite).writeAsyncTo(cloudWriter).thenApply(unused -> null);
    } catch (StoreException e) {
      return FutureUtils.completedExceptionally(e);
    }
  }

  /**
   * Upload the blob to the cloud destination.
   * @param messageInfo the {@link MessageInfo} containing blob metadata.
   * @param messageBuf the bytes to be uploaded.
   * @param size the number of bytes to upload.
   * @throws CloudStorageException if the upload failed.
   */
  private void putBlob(MessageInfo messageInfo, ByteBuffer messageBuf, long size)
      throws CloudStorageException, IOException, StoreException {
    // TODO: Remove the duplicate code by calling putBlobAsync() method and wait on it once we verify that upload works
    //  correctly.
    if (shouldUpload(messageInfo)) {
      BlobId blobId = (BlobId) messageInfo.getStoreKey();
      boolean isRouterEncrypted = isRouterEncrypted(blobId);
      EncryptionOrigin encryptionOrigin = isRouterEncrypted ? EncryptionOrigin.ROUTER : EncryptionOrigin.NONE;
      boolean encryptThisBlob = requireEncryption && !isRouterEncrypted;
      boolean uploaded;
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
                cryptoAgentFactory.getClass().getName(), encryptedSize, messageInfo.getLifeVersion());
        // If buffer was encrypted, we no longer know its size
        long bufferLen = (encryptedSize == -1) ? size : encryptedSize;
        uploaded = uploadWithRetries(blobId, messageBuf, bufferLen, blobMetadata);
      } else {
        // PutRequest lifeVersion from frontend is -1. Should set to 0. (0 is the starting life version number for any data).
        // Put from replication or recovery should use liferVersion as it's.
        short lifeVersion =
            messageInfo.hasLifeVersion(messageInfo.getLifeVersion()) ? messageInfo.getLifeVersion() : (short) 0;
        CloudBlobMetadata blobMetadata =
            new CloudBlobMetadata(blobId, messageInfo.getOperationTimeMs(), messageInfo.getExpirationTimeInMs(),
                messageInfo.getSize(), encryptionOrigin, lifeVersion);
        uploaded = uploadWithRetries(blobId, messageBuf, size, blobMetadata);
      }
      addToCache(blobId.getID(), (short) 0, BlobState.CREATED);
      if (!uploaded && !isVcr) {
        // If put is coming from frontend, then uploadBlob must be true. Its not acceptable that a blob already exists.
        // If put is coming from vcr, then findMissingKeys might have reported a key to be missing even though the blob
        // was uploaded.
        throw new StoreException(String.format("Another blob with same key %s exists in store", blobId.getID()),
            StoreErrorCodes.AlreadyExist);
      }
    } else {
      vcrMetrics.blobUploadSkippedCount.inc();
      // The only case where its ok to see a put request for a already seen blob is, during replication if the blob is
      // expiring within {@link CloudConfig#vcrMinTtlDays} for vcr to upload.
      if (isVcr && !isExpiringSoon(messageInfo) && !messageInfo.isDeleted()) {
        throw new StoreException(
            String.format("Another blob with same key %s exists in store", messageInfo.getStoreKey().getID()),
            StoreErrorCodes.AlreadyExist);
      }
    }
  }

  /**
   * Upload the blob to the cloud destination asynchronously.
   * @param messageInfo the {@link MessageInfo} containing blob metadata.
   * @param messageBuf the bytes to be uploaded.
   * @param size the number of bytes to upload.
   * @return a {@link CompletableFuture} that will eventually complete successfully when the messages are written to
   *         store or will contain the {@link CloudStorageException} if an error occurred
   */
  private CompletableFuture<Void> putBlobAsync(MessageInfo messageInfo, ByteBuffer messageBuf, long size) {
    try {
      if (shouldUpload(messageInfo)) {
        CloudBlobMetadata blobMetadata;
        BlobId blobId = (BlobId) messageInfo.getStoreKey();
        boolean isRouterEncrypted;
        isRouterEncrypted = isRouterEncrypted(blobId);
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
          blobMetadata =
              new CloudBlobMetadata(blobId, messageInfo.getOperationTimeMs(), messageInfo.getExpirationTimeInMs(),
                  messageInfo.getSize(), EncryptionOrigin.VCR, cryptoAgent.getEncryptionContext(),
                  cryptoAgentFactory.getClass().getName(), encryptedSize, messageInfo.getLifeVersion());
          // If buffer was encrypted, we no longer know its size
          size = (encryptedSize == -1) ? size : encryptedSize;
        } else {
          // PutRequest lifeVersion from frontend is -1. Should set to 0. (0 is the starting life version number for any data).
          // Put from replication or recovery should use liferVersion as it's.
          short lifeVersion =
              MessageInfo.hasLifeVersion(messageInfo.getLifeVersion()) ? messageInfo.getLifeVersion() : (short) 0;
          blobMetadata =
              new CloudBlobMetadata(blobId, messageInfo.getOperationTimeMs(), messageInfo.getExpirationTimeInMs(),
                  messageInfo.getSize(), encryptionOrigin, lifeVersion);
        }

        // Upload blob asynchronously
        return uploadAsyncWithRetries(blobId, messageBuf, size, blobMetadata).handle((uploaded, throwable) -> {
          if (throwable != null) {
            throw throwable instanceof CompletionException ? (CompletionException) throwable
                : new CompletionException(throwable);
          }
          addToCache(blobId.getID(), (short) 0, BlobState.CREATED);
          if (!uploaded && !isVcr) {
            // If put is coming from frontend, then uploadBlob must be true. Its not acceptable that a blob already exists.
            // If put is coming from vcr, then findMissingKeys might have reported a key to be missing even though the blob
            // was uploaded.
            throw new CompletionException(
                new StoreException(String.format("Another blob with same key %s exists in store", blobId.getID()),
                    StoreErrorCodes.AlreadyExist));
          } else {
            return null;
          }
        });
      }
      vcrMetrics.blobUploadSkippedCount.inc();
      // The only case where its ok to see a put request for a already seen blob is, during replication if the blob is
      // expiring within {@link CloudConfig#vcrMinTtlDays} for vcr to upload.
      if (isVcr && !isExpiringSoon(messageInfo) && !messageInfo.isDeleted()) {
        return FutureUtils.completedExceptionally(new StoreException(
            String.format("Another blob with same key %s exists in store", messageInfo.getStoreKey().getID()),
            StoreErrorCodes.AlreadyExist));
      }
      return CompletableFuture.completedFuture(null);
    } catch (IOException e) {
      return FutureUtils.completedExceptionally(e);
    }
  }

  /**
   * Upload the supplied message buffer to a blob in the cloud destination.
   * @param blobId the {@link BlobId}.
   * @param messageBuf the byte buffer to upload.
   * @param bufferSize the size of the buffer.
   * @param blobMetadata the {@link CloudBlobMetadata} for the blob.
   * @return boolean indicating if the upload was completed.
   * @throws CloudStorageException if the upload failed.
   */
  private boolean uploadWithRetries(BlobId blobId, ByteBuffer messageBuf, long bufferSize,
      CloudBlobMetadata blobMetadata) throws CloudStorageException {
    return requestAgent.doWithRetries(() -> {
      // Note: reset buffer and input stream each time through the loop
      messageBuf.rewind();
      InputStream uploadInputStream = new ByteBufferInputStream(messageBuf);
      return cloudDestination.uploadBlob(blobId, bufferSize, blobMetadata, uploadInputStream);
    }, "Upload", partitionId.toPathString());
  }

  /**
   * Upload the supplied message buffer to a blob in the cloud destination asynchronously.
   * @param blobId the {@link BlobId}.
   * @param messageBuf the byte buffer to upload.
   * @param bufferSize the size of the buffer.
   * @param blobMetadata the {@link CloudBlobMetadata} for the blob.
   * @return a {@link CompletableFuture} that will eventually complete with a {@link Boolean} value indicating if the
   *         upload was completed or with a {@link CloudStorageException} if an error occurred.
   */
  private CompletableFuture<Boolean> uploadAsyncWithRetries(BlobId blobId, ByteBuffer messageBuf, long bufferSize,
      CloudBlobMetadata blobMetadata) {
    // TODO: Add retries similar to synchronous operation
    InputStream uploadInputStream = new ByteBufferInputStream(messageBuf);
    return cloudDestination.uploadBlobAsync(blobId, bufferSize, blobMetadata, uploadInputStream);
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
    if (checkCacheState(messageInfo.getStoreKey().getID())) {
      return false;
    }
    if (isVcr) {
      // VCR only backs up blobs with expiration time above threshold.
      // Expired blobs are blocked by ReplicaThread.
      // TODO: VCR for non-backup also needs to replicate everything
      // We can change default cloudConfig.vcrMinTtlDays to 0 and override in config
      return !isExpiringSoon(messageInfo);
    } else {
      // Upload all live blobs
      return true;
    }
  }

  @Override
  public StoreBatchDeleteInfo batchDelete(List<MessageInfo> infos) throws StoreException {
    throw new UnsupportedOperationException("Method not supported");
  }

  @Override
  public List<LogInfo> getLogSegmentMetadataFiles(boolean includeActiveLogSegment) {
    throw new UnsupportedOperationException("Method not supported");
  }

  @Override
  public boolean isCompactionInProgress() {
    throw new UnsupportedOperationException("Method not supported");
  }

  @Override
  public String getSnapshotId(List<LogInfo> logSegments) {
    throw new UnsupportedOperationException("Method not supported");
  }

  @Override
  public void purge(List<MessageInfo> infosToPurge) throws StoreException {
    throw new UnsupportedOperationException("Method not supported");
  }

  @Override
  public void forceDelete(List<MessageInfo> infosToDelete) throws StoreException {
    throw new UnsupportedOperationException("Method not supported");
  }

  /**
   * Deletes all the messages in the list from the store asynchronously. When the lifeVersion is
   * {@link MessageInfo#LIFE_VERSION_FROM_FRONTEND}, this method is invoked by the responding to the frontend request.
   * Otherwise, it's invoked in the replication thread.
   * @param infos The list of messages that need to be deleted. Only the StoreKey, OperationTime, LifeVersion
   *                      should be used in this method.
   * @return a {@link CompletableFuture} that will eventually complete successfully when all the messages are deleted
   *         from store or will contain the {@link StoreException} if an error occurred
   */
  public CompletableFuture<Void> deleteAsync(List<MessageInfo> infos) {
    try {
      checkStarted();
      checkDuplicates(infos);
      List<CompletableFuture<Boolean>> operationFutures = new ArrayList<>();

      // Delete each message
      for (MessageInfo msgInfo : infos) {
        BlobId blobId = (BlobId) msgInfo.getStoreKey();
        // TODO: Add retries similar to synchronous operation
        operationFutures.add(deleteAsyncIfNeeded(blobId, msgInfo.getOperationTimeMs(), msgInfo.getLifeVersion()));
      }

      // Complete the result once all the individual delete operations complete. If any of the operation completes
      // exceptionally, complete the result exceptionally as well.
      return CompletableFuture.allOf(operationFutures.toArray(new CompletableFuture<?>[0])).exceptionally(throwable -> {
        unwrapCompletionExceptionAndThrowStoreException(throwable, CloudStorageException.class,
            this::mapToStoreException);
        return null;
      });
    } catch (StoreException e) {
      return FutureUtils.completedExceptionally(e);
    }
  }

  /**
   * Delete the specified blob if needed depending on the cache state asynchronously.
   * @param blobId the blob to delete
   * @param deletionTime the deletion time
   * @param lifeVersion life version of the blob.
   * @return a {@link CompletableFuture} that will eventually be {@code True} if the deletion was performed. If there was
   *         an exception during the delete, it will contain the related {@link CloudStorageException}.
   */
  private CompletableFuture<Boolean> deleteAsyncIfNeeded(BlobId blobId, long deletionTime, short lifeVersion) {
    String blobKey = blobId.getID();
    // Note: always check cache before operation attempt, since this could be a retry after a CONFLICT error,
    // in which case the cache may have been updated by another thread.
    if (!checkCacheState(blobKey, lifeVersion, BlobState.DELETED)) {
      return cloudDestination.deleteBlobAsync(blobId, deletionTime, lifeVersion, this::preDeleteValidation)
          .whenComplete((isDeleted, throwable) -> {
            if (throwable != null) {
              // Cache entry could be stale, evict it to force refresh on retry.
              removeFromCache(blobKey);
            } else {
              addToCache(blobKey, lifeVersion, BlobState.DELETED);
            }
          });
    }
    // This means that we definitely saw this delete for the same or smaller life version before.
    return FutureUtils.completedExceptionally(new CloudStorageException("Error updating blob metadata",
        new StoreException("Cannot delete id " + blobId.getID() + " since it is already marked as deleted in cloud.",
            StoreErrorCodes.IDDeleted)));
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
    try {
      checkStarted();
      // TODO: Add retries similar to synchronous operation
      return undeleteAsyncIfNeeded((BlobId) info.getStoreKey(), info.getLifeVersion()).exceptionally(throwable -> {
        unwrapCompletionExceptionAndThrowStoreException(throwable, CloudStorageException.class,
            this::mapToStoreException);
        return null;
      });
    } catch (StoreException e) {
      return FutureUtils.completedExceptionally(e);
    }
  }

  /**
   * Undelete the specified blob if needed depending on the cache state asynchronously.
   * @param blobId the blob to delete.
   * @param lifeVersion life version of the deleted blob.
   * @return a {@link CompletableFuture} that will eventually contain the final updated life version of the blob or the
   *         {@link CloudStorageException} or {@link StoreException} if an error occurred.
   */
  private CompletableFuture<Short> undeleteAsyncIfNeeded(BlobId blobId, short lifeVersion) {
    String blobKey = blobId.getID();
    // See note in deleteIfNeeded.
    if (!checkCacheState(blobKey, lifeVersion, BlobState.CREATED)) {
      return cloudDestination.undeleteBlobAsync(blobId, lifeVersion, this::preUndeleteValidation)
          .whenComplete((newLifeVersion, throwable) -> {
            if (throwable != null) {
              removeFromCache(blobKey);
            } else {
              addToCache(blobId.getID(), newLifeVersion, BlobState.CREATED);
            }
          });
    }
    return FutureUtils.completedExceptionally(
        new StoreException("Id " + blobId.getID() + " is already undeleted in cloud", StoreErrorCodes.IDUndeleted));
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
    try {
      checkStarted();
      // Note: We skipped uploading the blob on PUT record if the TTL was below threshold (threshold should be 0 for non DR cases).
      List<CompletableFuture<Boolean>> operationFutures = new ArrayList<>();
      for (MessageInfo msgInfo : infosToUpdate) {
        if (msgInfo.getExpirationTimeInMs() != Utils.Infinite_Time) {
          return FutureUtils.completedExceptionally(
              new StoreException("CloudBlobStore only supports removing the expiration time",
                  StoreErrorCodes.UpdateNotAllowed));
        }
        if (msgInfo.isTtlUpdated()) {
          // TODO: Add retries similar to synchronous operation
          operationFutures.add(updateTtlAsyncIfNeeded((BlobId) msgInfo.getStoreKey()));
        } else {
          logger.error("updateTtl() is called but msgInfo.isTtlUpdated is not set. msgInfo: {}", msgInfo);
          vcrMetrics.updateTtlNotSetError.inc();
        }
      }

      // Return a completable future that completes once all async TTL update operations are complete.
      return CompletableFuture.allOf(operationFutures.toArray(new CompletableFuture<?>[0]))
          .exceptionally((throwable) -> {
            unwrapCompletionExceptionAndThrowStoreException(throwable, CloudStorageException.class,
                this::mapToStoreException);
            return null;
          });
    } catch (StoreException e) {
      return FutureUtils.completedExceptionally(e);
    }
  }

  /**
   * Update the TTL of the specified blob if needed depending on the cache state asynchronously.
   * @param blobId the blob to update
   * @return a {@link CompletableFuture} that will eventually complete successfully if the update was performed or
   *         will contain a {@link CloudStorageException} if an error occurred.
   */
  private CompletableFuture<Boolean> updateTtlAsyncIfNeeded(BlobId blobId) {
    String blobKey = blobId.getID();
    // See note in deleteIfNeeded.
    if (!checkCacheState(blobKey, BlobState.TTL_UPDATED)) {
      return cloudDestination.updateBlobExpirationAsync(blobId, Utils.Infinite_Time, this::preTtlUpdateValidation)
          .handle((lifeVersion, throwable) -> {
            if (throwable != null) {
              // Cache entry could be stale, evict it to force refresh on retry.
              removeFromCache(blobKey);
              throw throwable instanceof CompletionException ? (CompletionException) throwable
                  : new CompletionException(throwable);
            }
            addToCache(blobKey, lifeVersion, BlobState.TTL_UPDATED);
            return (lifeVersion != -1);
          });
    }
    return CompletableFuture.completedFuture(false);
  }

  private StoreException mapToStoreException(CloudStorageException cse) {
    if (cse.getCause() instanceof StoreException) {
      return (StoreException) cse.getCause();
    }
    StoreErrorCodes errorCode =
        (cse.getStatusCode() == STATUS_NOT_FOUND) ? StoreErrorCodes.IDNotFound : StoreErrorCodes.IOError;
    return new StoreException(cse, errorCode);
  }

  private <T> void unwrapCompletionExceptionAndThrowStoreException(Throwable throwable, Class<T> clazz,
      Function<T, StoreException> exceptionFunction) throws CompletionException {
    Exception ex = Utils.extractFutureExceptionCause(throwable);

    if (clazz.isInstance(ex)) {
      throw new CompletionException(exceptionFunction.apply((T) ex));
    }
    throw throwable instanceof CompletionException ? (CompletionException) throwable
        : new CompletionException(throwable);
  }

  /**
   * Validate {@link CloudBlobMetadata} map to make sure it has metadata for all keys, and they meet the {@code storeGetOptions} requirements.
   * @param cloudBlobMetadataMap {@link CloudBlobMetadata} map.
   * @param storeGetOptions {@link StoreGetOptions} requirements.
   * @param currentTimestamp current time stamp.
   * @throws StoreException if the {@code CloudBlobMetadata} isnt valid
   */
  private void validateCloudMetadata(Map<String, CloudBlobMetadata> cloudBlobMetadataMap,
      EnumSet<StoreGetOptions> storeGetOptions, long currentTimestamp, List<? extends StoreKey> ids)
      throws StoreException {
    for (String key : cloudBlobMetadataMap.keySet()) {
      if (isBlobDeleted(cloudBlobMetadataMap.get(key)) && !storeGetOptions.contains(
          StoreGetOptions.Store_Include_Deleted)) {
        throw new StoreException("Id " + key + " has been deleted on the cloud", StoreErrorCodes.IDDeleted);
      }
      if (isBlobExpired(cloudBlobMetadataMap.get(key), currentTimestamp) && !storeGetOptions.contains(
          StoreGetOptions.Store_Include_Expired)) {
        throw new StoreException("Id " + key + " has expired on the cloud", StoreErrorCodes.TTLExpired);
      }
    }
    validateAccountAndContainer(cloudBlobMetadataMap, ids);
  }

  /**
   * Validate account id and container id for blobs in {@link CloudBlobMetadata} map match those in {@link StoreKey} list.
   * @param cloudBlobMetadataMap {@link Map} of {@link CloudBlobMetadata}.
   * @param storeKeys {@link List} of {@link StoreKey}s.
   */
  private void validateAccountAndContainer(Map<String, CloudBlobMetadata> cloudBlobMetadataMap,
      List<? extends StoreKey> storeKeys) throws StoreException {
    for (StoreKey key : storeKeys) {
      CloudBlobMetadata cloudBlobMetadata = cloudBlobMetadataMap.get(key.getID());
      // validate accountId and containerId
      if (!key.isAccountContainerMatch((short) cloudBlobMetadata.getAccountId(),
          (short) cloudBlobMetadata.getContainerId())) {
        if (storeConfig.storeValidateAuthorization) {
          throw new StoreException("GET authorization failure. Key: " + key.getID() + " Actual accountId: "
              + cloudBlobMetadata.getAccountId() + " Actual containerId: " + cloudBlobMetadata.getAccountId(),
              StoreErrorCodes.AuthorizationFailure);
        } else {
          logger.warn("GET authorization failure. Key: {} Actually accountId: {} Actually containerId: {}", key.getID(),
              cloudBlobMetadata.getAccountId(), cloudBlobMetadata.getContainerId());
        }
      }
    }
  }

  /**
   * Validates existing metadata in cloud destination against requested update for delete.
   * @param metadata existing {@link CloudBlobMetadata} in cloud.
   * @param key {@link StoreKey} being deleted.
   * @param updateFields {@link Map} of fields and values being updated.
   * @return false only for vcr if local cloud destination life version is more recent. true if validation successful.
   * @throws StoreException if validation fails.
   */
  private boolean preDeleteValidation(CloudBlobMetadata metadata, StoreKey key, Map<String, Object> updateFields)
      throws StoreException {
    validateAccountAndContainer(Collections.singletonMap(key.getID(), metadata), Collections.singletonList(key));
    short requestedLifeVersion = (short) updateFields.get(FIELD_LIFE_VERSION);
    if (isVcr) {
      // This is a delete request from vcr. Apply delete only if incoming life version is more recent. Don't throw
      // any exception because replication relies on findMissingKeys which in turn is dependent on {@link CloudDestination}
      // implementation and can have some inconsistencies.
      return (!metadata.isDeleted() || metadata.getLifeVersion() < requestedLifeVersion) && (metadata.getLifeVersion()
          <= requestedLifeVersion);
    }
    if (requestedLifeVersion == MessageInfo.LIFE_VERSION_FROM_FRONTEND) {
      // This is a delete request from frontend
      if (metadata.isDeleted()) {
        throw new StoreException("Cannot delete id " + key.getID() + " since it is already marked as deleted in cloud.",
            StoreErrorCodes.IDDeleted);
      }
      // this is delete request from frontend, we use life version only for validation.
      updateFields.remove(FIELD_LIFE_VERSION);
    }
    return true;
  }

  /**
   * Validates existing metadata in cloud destination against requested update for ttl.
   * Note that this method also has an unclean side effect of updating the {@code updateFields}.
   * @param metadata existing {@link CloudBlobMetadata} in cloud.
   * @param key {@link StoreKey} being updated.
   * @param updateFields {@link Map} of fields and values being updated.
   * @return false only for vcr if ttl is already applied on blob. true in all other cases if validation is successful.
   * @throws StoreException if validation fails.
   */
  private boolean preTtlUpdateValidation(CloudBlobMetadata metadata, StoreKey key, Map<String, Object> updateFields)
      throws StoreException {
    validateAccountAndContainer(Collections.singletonMap(key.getID(), metadata), Collections.singletonList(key));
    long now = System.currentTimeMillis();
    if (isVcr) {
      // For vcr don't update ttl if already updated. Don't throw any exception because replication relies on
      // findMissingKeys which in turn is dependent on {@link CloudDestination} implementation and can have some inconsistencies.
      return metadata.getExpirationTime() != Utils.Infinite_Time;
    }
    if (metadata.isDeleted()) {
      throw new StoreException("Cannot update TTL of " + key.getID() + " since it is already deleted in the index.",
          StoreErrorCodes.IDDeleted);
    } else if (metadata.getExpirationTime() != Utils.Infinite_Time
        && metadata.getExpirationTime() < now + ttlUpdateBufferTimeMs) {
      throw new StoreException(
          "TTL of " + key.getID() + " cannot be updated because it is too close to expiry. Minimum Op time (ms): " + now
              + ". ExpiresAtMs: " + metadata.getExpirationTime(), StoreErrorCodes.UpdateNotAllowed);
    }
    return true;
  }

  /**
   * Validates existing metadata in cloud destination against requested undelete.
   * Note that this method also has an unclean side effect of updating the {@code updateFields}.
   * @param metadata existing {@link CloudBlobMetadata} in cloud.
   * @param key {@link StoreKey} being updated.
   * @param updateFields {@link Map} of fields and values being updated.
   * @return false only for vcr if local cloud destination life version is more recent. true if validation successful.
   * @throws StoreException if validation fails.
   */
  private boolean preUndeleteValidation(CloudBlobMetadata metadata, StoreKey key, Map<String, Object> updateFields)
      throws StoreException {
    validateAccountAndContainer(Collections.singletonMap(key.getID(), metadata), Collections.singletonList(key));
    short requestedLifeVersion = (short) updateFields.get(FIELD_LIFE_VERSION);
    if (isVcr) {
      // This is an undelete request from vcr. Apply undelete only if incoming life version is more recent. Don't throw
      // any exception because replication relies on findMissingKeys which in turn is dependent on {@link CloudDestination}
      // implementation and can have some inconsistencies.
      return metadata.getLifeVersion() < requestedLifeVersion;
    }
    if (metadata.isExpired()) {
      throw new StoreException("Id " + key + " already expired in cloud ", StoreErrorCodes.TTLExpired);
    } else if (metadata.isUndeleted()) {
      throw new StoreException("Id " + key + " is already undeleted in cloud", StoreErrorCodes.IDUndeleted);
    } else if (!metadata.isDeleted()) {
      throw new StoreException("Id " + key + " is not deleted yet in cloud ", StoreErrorCodes.IDNotDeleted);
    } else if (metadata.getDeletionTime() + TimeUnit.MINUTES.toMillis(storeConfig.storeDeletedMessageRetentionMinutes)
        < System.currentTimeMillis()) {
      throw new StoreException("Id " + key + " already permanently deleted in cloud ",
          StoreErrorCodes.IDDeletedPermanently);
    }
    // Update life version to appropriate value for frontend requests.
    updateFields.put(FIELD_LIFE_VERSION, metadata.getLifeVersion() + 1);
    return true;
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
   * Check the blob state in the recent blob cache against one or more desired states.
   * Note that this check ignores the life version of the blob.
   * @param blobKey the blob key to lookup.
   * @param desiredStates the desired state(s) to check.  If empty, any cached state is accepted.
   * @return true if the blob key is in the cache in one of the desired states, otherwise false.
   */
  private boolean checkCacheState(String blobKey, BlobState... desiredStates) {
    return checkCacheState(blobKey, IGNORE_LIFE_VERSION, desiredStates);
  }

  /**
   * Check the blob state in the recent blob cache against one or more desired states and life version.
   * @param blobKey the blob key to lookup.
   * @param lifeVersion the life version to check.
   * @param desiredStates the desired state(s) to check. If empty, any cached state is accepted.
   * @return true if the blob key is in the cache in one of the desired states and has appropriate life
   * version, otherwise false.
   */
  private boolean checkCacheState(String blobKey, short lifeVersion, BlobState... desiredStates) {
    if (recentBlobCache == null) {
      return false;
    }
    // If the request is coming from frontend, and the desired states being checked are deleted/created (undelete),
    // then cache might not help. Operations like ttl update and put are once in lifetime of a blob. So a cache hit in
    // those cases definitely helps. A cache miss for ttl update or put, doesn't necessarily mean that we never saw
    // that operation for the given blob before. It only means that this cloud blob store instance didn't see it. For
    // this case, the pre<operation>Validation methods (e.g, {@code preDeleteValidation, preTtlUpdateValidation} etc)
    // will help do any validations if required.
    if (lifeVersion == MessageInfo.LIFE_VERSION_FROM_FRONTEND && !Collections.disjoint(Arrays.asList(desiredStates),
        Arrays.asList(BlobState.DELETED, BlobState.CREATED))) {
      return false;
    }
    // If we are here, in case of delete and undelete, we pass the life version to check against.
    // So this should be safe, as long as we claim a cache hit only when life version in cache is not older than
    // life version passed. Again this doesn't mean that a more recent life version was never seen for this blob. It
    // only means that this cloud blob store instance didn't see it. And for this case, the pre<operation>Validation
    // methods (e.g, {@code preDeleteValidation, preTtlUpdateValidation} etc) will help do any validations if required.
    BlobLifeState cachedState = recentBlobCache.get(blobKey);
    vcrMetrics.blobCacheLookupCount.inc();
    if (cachedState == null) {
      return false;
    }
    if (desiredStates == null || desiredStates.length == 0) {
      vcrMetrics.blobCacheHitCount.inc();
      return true;
    }
    for (BlobState desiredState : desiredStates) {
      if ((desiredState == cachedState.getBlobState() && (lifeVersion == IGNORE_LIFE_VERSION
          || lifeVersion <= cachedState.getLifeVersion()))
          || desiredState == BlobState.TTL_UPDATED && cachedState.isTtlUpdated()) {
        vcrMetrics.blobCacheHitCount.inc();
        return true;
      }
    }
    return false;
  }

  /**
   * Add a blob state mapping to the recent blob cache.
   * @param blobKey the blob key to cache.
   * @param lifeVersion life version to cache.
   * @param blobState the state of the blob.
   */
  // Visible for test.
  public void addToCache(String blobKey, short lifeVersion, BlobState blobState) {
    if (recentBlobCache == null) {
      return;
    }
    if (isVcr) {
      if (blobState == BlobState.TTL_UPDATED) {
        // In case of ttl update we update the ttl without taking into account the life version.
        // So make sure that we do not decrease the lifeVersion in cache due to an incoming ttl update.
        if (recentBlobCache.containsKey(blobKey)) {
          lifeVersion = (short) Math.max(lifeVersion, recentBlobCache.get(blobKey).getLifeVersion());
        }
      }
      recentBlobCache.put(blobKey, new BlobLifeState(blobState, lifeVersion, recentBlobCache.get(blobKey)));
    }
  }

  /**
   * Remove a blob state mapping from the recent blob cache.
   * @param blobKey the blob key to remove.
   */
  // Visible for test.
  public void removeFromCache(String blobKey) {
    if (recentBlobCache == null) {
      return;
    }
    if (isVcr) {
      logger.debug("Removing key {} from cache", blobKey);
      recentBlobCache.remove(blobKey);
    }
  }

  @Override
  public FindInfo findEntriesSince(FindToken token, long maxTotalSizeOfEntries, String hostname,
      String remoteReplicaPath) throws StoreException {
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
  public StoreStats getStoreStats() {
    return null;
  }

  @Override
  public boolean isKeyDeleted(StoreKey key) throws StoreException {
    checkStarted();
    // Not definitive, but okay for some deletes to be replayed.
    return checkCacheState(key.getID(), BlobState.DELETED);
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
    if (recentBlobCache != null) {
      recentBlobCache.clear();
    }
    currentState = ReplicaState.OFFLINE;
    logger.info("Stopped store: {}", this.toString());
  }

  @Override
  public boolean isStarted() {
    return true;
  }

  @Override
  public boolean isInitialized() {
    return true;
  }

  private void checkStarted() throws StoreException {
    // There is no concept of start/stop Azure cloud partition
  }

  /**
   * Check if the blob expires within the min ttl threshold config for vcr {@code CloudConfig#vcrMinTtlDays}.
   * @param messageInfo {@link MessageInfo} to check.
   * @return true if blob is expiring within threshold, false otherwise.
   */
  private boolean isExpiringSoon(MessageInfo messageInfo) {
    return messageInfo.getExpirationTimeInMs() != Utils.Infinite_Time
        && messageInfo.getExpirationTimeInMs() - messageInfo.getOperationTimeMs() < minTtlMillis;
  }

  /**
   * Detects duplicates in {@code writeSet}
   * @param infos the list of {@link MessageInfo} to detect duplicates in
   * @throws IllegalArgumentException if a duplicate is detected
   */
  private void checkDuplicates(List<MessageInfo> infos) throws IllegalArgumentException {
    List<StoreKey> keys = infos.stream().map(MessageInfo::getStoreKey).collect(Collectors.toList());
    checkStoreKeyDuplicates(keys);
  }

  /**
   * Detects duplicates in {@code keys}
   * @param keys list of {@link StoreKey} to detect duplicates in
   * @throws IllegalArgumentException if a duplicate is detected
   */
  private void checkStoreKeyDuplicates(List<? extends StoreKey> keys) throws IllegalArgumentException {
    if (keys.size() > 1) {
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

  /** The state of a blob. */
  public enum BlobState {CREATED, TTL_UPDATED, DELETED}

  /** The lifecycle state of a recently seen blob. */
  static class BlobLifeState {

    private final BlobState blobState;
    private final short lifeVersion;
    // this helps with duplicate ttl update requests for same blob.
    private final boolean isTtlUpdated;

    /**
     * Constructor for {@link BlobLifeState}.
     * @param blobState {@link BlobState} of the blob.
     * @param lifeVersion life version of the blob.
     * @param previousBlobLifeState previous life state of the blob to check is blob's ttl was updated previously.
     *                              can be null.
     */
    BlobLifeState(BlobState blobState, short lifeVersion, BlobLifeState previousBlobLifeState) {
      this.blobState = blobState;
      this.lifeVersion = lifeVersion;
      this.isTtlUpdated =
          (blobState == BlobState.TTL_UPDATED) || (previousBlobLifeState != null && previousBlobLifeState.isTtlUpdated);
    }

    /**
     * @return {@link BlobState} of the blob.
     */
    public BlobState getBlobState() {
      return blobState;
    }

    /**
     * @return life version of the blob.
     */
    public short getLifeVersion() {
      return lifeVersion;
    }

    /**
     * @return ttl update status.
     */
    public boolean isTtlUpdated() {
      return isTtlUpdated;
    }
  }

  /** A {@link Write} implementation used by this store to write data. */
  class CloudWriteChannel implements Write {
    private final CloudBlobStore cloudBlobStore;
    private final List<MessageInfo> messageInfoList;
    private int messageIndex = 0;

    CloudWriteChannel(CloudBlobStore cloudBlobStore, List<MessageInfo> messageInfoList) {
      this.cloudBlobStore = cloudBlobStore;
      this.messageInfoList = messageInfoList;
    }

    @Override
    public int appendFrom(ByteBuffer buffer) {
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

    /**
     * Appends the channel to the underlying write interface. Writes "size" number of bytes to the interface.
     * @param channel The channel from which data needs to be written from
     * @param size The amount of data in bytes to be written from the channel
     * @return a {@link CompletableFuture} that will eventually complete successfully once the bytes are written to the
     *         write interface or an exception if an error occurs.
     */
    public CompletableFuture<Void> appendAsyncFrom(ReadableByteChannel channel, long size) {
      // Upload the blob corresponding to the current message index
      MessageInfo messageInfo = messageInfoList.get(messageIndex++);
      if (messageInfo.getSize() != size) {
        throw new IllegalStateException("Mismatched buffer length for blob: " + messageInfo.getStoreKey().getID());
      }
      ByteBuffer messageBuf = ByteBuffer.allocate((int) size);
      int bytesRead = 0;
      try {
        while (bytesRead < size) {
          int readResult = channel.read(messageBuf);
          if (readResult == -1) {
            return FutureUtils.completedExceptionally(new IOException(
                "Channel read returned -1 before reading expected number of bytes, blobId=" + messageInfo.getStoreKey()
                    .getID()));
          }
          bytesRead += readResult;
        }
        messageBuf.flip();
        return cloudBlobStore.putBlobAsync(messageInfo, messageBuf, size);
      } catch (IOException e) {
        return FutureUtils.completedExceptionally(new StoreException(e, StoreErrorCodes.IOError));
      }
    }
  }

  /**
   * A local LRA cache of recent blobs processed by this store.
   */
  private class RecentBlobCache extends LinkedHashMap<String, BlobLifeState> {
    private final int maxEntries;

    public RecentBlobCache(int maxEntries) {
      // Use access order for eviction
      super(cacheInitialCapacity, cacheLoadFactor, true);
      this.maxEntries = maxEntries;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<String, BlobLifeState> eldest) {
      return (this.size() > maxEntries);
    }
  }
}
