/**
 * Copyright 2023 LinkedIn Corp. All rights reserved.
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

import com.azure.core.http.rest.Response;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.github.ambry.cloud.azure.AzureCloudConfig;
import com.github.ambry.cloud.azure.AzureMetrics;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ReplicaState;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.MessageFormatWriteSet;
import com.github.ambry.messageformat.MessageSievingInputStream;
import com.github.ambry.replication.FindToken;
import com.github.ambry.store.FindInfo;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.MessageWriteSet;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreGetOptions;
import com.github.ambry.store.StoreInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreStats;
import java.io.InputStream;
import java.time.Duration;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implements methods to write {@link com.github.ambry.clustermap.AmbryPartition} in Azure blob storage.
 */
public class CloudBlobStoreV2 implements Store {

  private final ClusterMap clusterMap;
  private final AzureMetrics azureMetrics;
  private final CloudConfig cloudConfig;
  private final AzureCloudConfig azureCloudConfig;
  private final Object vcrMetrics;
  private final BlobContainerClient blobContainerClient;
  private static final Logger logger = LoggerFactory.getLogger(CloudBlobStoreV2.class);

  public CloudBlobStoreV2(VerifiableProperties properties, MetricRegistry metricRegistry, ClusterMap clusterMap,
      BlobContainerClient blobContainerClient) {
    this.azureMetrics = new AzureMetrics(metricRegistry);
    this.cloudConfig = new CloudConfig(properties);
    this.clusterMap = clusterMap;
    this.azureCloudConfig = new AzureCloudConfig(properties);
    this.vcrMetrics = new VcrMetrics(metricRegistry);
    this.blobContainerClient = blobContainerClient;
  }

  public static final String ACCOUNT_ID = "accountId";
  public static final String CONTAINER_ID = "containerId";
  public static final String CRC = "crc";
  public static final String EXPIRATION_TIME = "expirationTimeInMs";
  public static final String LIFE_VERSION = "lifeVersion";
  public static final String OPERATION_TIME = "operationTimeMs";
  public static final String SIZE = "size";

  /**
   * @return a {@link HashMap} of metadata key-value pairs.
   */
  public Map<String, String> messageInfoToMap(MessageInfo messageInfo) {
    HashMap<String, String> hashMap = new HashMap<>();
    hashMap.put(ACCOUNT_ID, String.valueOf(messageInfo.getAccountId()));
    hashMap.put(CONTAINER_ID, String.valueOf(messageInfo.getContainerId()));
    hashMap.put(CRC, String.valueOf(messageInfo.getCrc()));
    hashMap.put(EXPIRATION_TIME, String.valueOf(messageInfo.getExpirationTimeInMs()));
    hashMap.put(LIFE_VERSION, String.valueOf(messageInfo.getLifeVersion()));
    hashMap.put(OPERATION_TIME, String.valueOf(messageInfo.getOperationTimeMs()));
    hashMap.put(SIZE, String.valueOf(messageInfo.getSize()));
    return hashMap;
  }

  /**
   * Detects duplicates in list of {@link MessageInfo}
   * @param messageInfos list of {@link MessageInfo} to check for duplicates
   */
  protected void checkDuplicates(List<MessageInfo> messageInfos) {
    if (messageInfos.size() < 2) {
      return;
    }
    HashMap<String, MessageInfo> seenKeys = new HashMap<>();
    for (MessageInfo messageInfo : messageInfos) {
      // Check for duplicates. We cannot continue as stream could be corrupted.
      // Server always sends one message per key that captures full state of the key including ttl-update & delete.
      // Emit a log and metric for investigation as it indicates a flaw in (de)serialization or replication.
      // FIXME: This check should be in replication layer to avoid multiple implementations at Store level.
      String blobId = messageInfo.getStoreKey().getID();
      if (seenKeys.containsKey(blobId)) {
        throw new IllegalArgumentException(
            String.format("Duplicate message in replication-batch, original message = %s, duplicate message = %s",
                seenKeys.get(blobId).toString(), messageInfo));
      }
      seenKeys.put(blobId, messageInfo);
    }
  }

  /**
   * Uploads Ambry blob to Azure blob storage.
   * If blob already exists, then it catches BLOB_ALREADY_EXISTS exception and proceeds to next blob.
   * It fails for any other exception.
   * @param messageSetToWrite The message set to write to the store
   *                          Only the StoreKey, OperationTime, ExpirationTime, LifeVersion should be used in this method.
   * @throws StoreException
   */
  @Override
  public void put(MessageWriteSet messageSetToWrite) throws StoreException {
    MessageSievingInputStream messageSievingInputStream =
        (MessageSievingInputStream) ((MessageFormatWriteSet) messageSetToWrite).getStreamToWrite();
    // This is a list of blob-ids from remote server
    List<MessageInfo> messageInfos = messageSievingInputStream.getValidMessageInfoList();
    checkDuplicates(messageInfos);
    // This is a list of data-streams associated with blob-ids. i-th stream is for i-th blob-id.
    // This allows us to pass stream to azure-sdk and let it handle read() logic.
    List<InputStream> messageStreamList = messageSievingInputStream.getValidMessageStreamList();
    Timer.Context storageTimer = null;
    MessageInfo messageInfo = null;
    // For-each loop must be outside try-catch. Loop must continue on BLOB_ALREADY_EXISTS exception
    for (int msgNum = 0; msgNum < messageInfos.size(); msgNum++) {
      try {
        messageInfo = messageInfos.get(msgNum);

        // Prepare to upload blob to Azure blob storage
        // There is no parallelism, but we still need to create and pass this object to SDK.
        BlobParallelUploadOptions blobParallelUploadOptions =
            new BlobParallelUploadOptions(messageStreamList.get(msgNum));
        // To avoid overwriting, pass "*" to setIfNoneMatch(String ifNoneMatch)
        // https://learn.microsoft.com/en-us/java/api/com.azure.storage.blob.blobclient?view=azure-java-stable
        blobParallelUploadOptions.setRequestConditions(new BlobRequestConditions().setIfNoneMatch("*"));
        blobParallelUploadOptions.setMetadata(messageInfoToMap(messageInfo));
        // Without content-type, get-blob floods log with warnings
        blobParallelUploadOptions.setHeaders(new BlobHttpHeaders().setContentType("application/octet-stream"));

        ////////////////////////////////// Upload blob to Azure blob storage ////////////////////////////////////////
        storageTimer = azureMetrics.blobUploadTime.time();
        Response<BlockBlobItem> blockBlobItemResponse =
            blobContainerClient.getBlobClient(messageInfo.getStoreKey().getID())
                .uploadWithResponse(blobParallelUploadOptions, Duration.ofMillis(cloudConfig.cloudRequestTimeout),
                    Context.NONE);
        ////////////////////////////////// Upload blob to Azure blob storage ////////////////////////////////////////

        // Metrics and log
        // Success rate is effective, Counter is ineffective because it just monotonically increases
        azureMetrics.blobUploadSuccessRate.mark();
        // Measure ingestion rate, helps decide fleet size
        azureMetrics.backupSuccessByteRate.mark(messageInfo.getSize());
        logger.debug("Successful upload of blob {} to Azure blob storage with statusCode = {}, etag = {}",
            messageInfo.getStoreKey().getID(), blockBlobItemResponse.getStatusCode(),
            blockBlobItemResponse.getValue().getETag());
      } catch (Exception e) {
        if (e instanceof BlobStorageException
            && ((BlobStorageException) e).getErrorCode() == BlobErrorCode.BLOB_ALREADY_EXISTS) {
          // Since VCR replicates from all replicas, a blob can be uploaded by at least two threads concurrently.
          logger.debug("Failed to upload blob {} to Azure blob storage because it already exists",
              messageInfo.getStoreKey().getID());
          azureMetrics.blobUploadConflictCount.inc();
          continue;
        }
        logger.error("Failed to upload blob {} to Azure blob storage because {}", messageInfo.getStoreKey().getID(),
            e.getMessage());
        azureMetrics.blobUploadErrorCount.inc();
        throw new RuntimeException(e);
      } finally {
        if (storageTimer != null) {
          storageTimer.stop();
          storageTimer = null;
        }
      } // try-catch
    } // for-each
  } // put()

  @Override
  public void delete(List<MessageInfo> messageInfos) throws StoreException {
    // TODO
  }

  @Override
  public short undelete(MessageInfo messageInfo) throws StoreException {
    // TODO
    return 0;
  }

  @Override
  public void updateTtl(List<MessageInfo> messageInfos) {
    // TODO
  }

  @Override
  public Set<StoreKey> findMissingKeys(List<StoreKey> storeKeys) {
    // TODO
    return new HashSet<>();
  }

  @Override
  public MessageInfo findKey(StoreKey storeKey) {
    // TODO
    return null;
  }

  @Override
  public boolean isStarted() {
    return true;
  }

  @Override
  public boolean isEmpty() {
    // TODO
    return false;
  }

  @Override
  public ReplicaState getCurrentState() {
    // TODO
    return null;
  }

  @Override
  public void setCurrentState(ReplicaState state) {
    // TODO
  }

  @Override
  public long getSizeInBytes() {
    // TODO
    return 0;
  }

  @Override
  public long getEndPositionOfLastPut() {
    // TODO
    return 0;
  }

  /////////////////////////////////////////// Unimplemented methods /////////////////////////////////////////////////

  @Override
  public void start() throws StoreException {

  }

  @Override
  public StoreInfo get(List<? extends StoreKey> ids, EnumSet<StoreGetOptions> storeGetOptions) throws StoreException {
    return null;
  }

  @Override
  public void forceDelete(List<MessageInfo> infosToDelete) {

  }

  @Override
  public FindInfo findEntriesSince(FindToken token, long maxTotalSizeOfEntries, String hostname,
      String remoteReplicaPath) throws StoreException {
    return null;
  }

  @Override
  public StoreStats getStoreStats() {
    return null;
  }

  @Override
  public boolean isDisabled() {
    return false;
  }

  @Override
  public boolean isKeyDeleted(StoreKey key) {
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

  }

  @Override
  public boolean recoverFromDecommission() {
    return false;
  }

  @Override
  public void shutdown() throws StoreException {

  }
}
