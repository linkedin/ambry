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
import com.github.ambry.utils.ByteBufferInputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
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
    MessageFormatWriteSet messageFormatWriteSet = (MessageFormatWriteSet) messageSetToWrite;
    Timer.Context storageTimer = null;
    // For-each loop must be outside try-catch. Loop must continue on BLOB_ALREADY_EXISTS exception
    for (MessageInfo messageInfo : messageFormatWriteSet.getMessageSetInfo()) {
      try {
        // Read blob from input stream. It is not possible to just pass the input stream to azure-SDK.
        // The SDK expects the stream to contain exactly the number of bytes to be read. If there are
        // more bytes, it fails assuming the stream is corrupted. The inputStream we have here is a
        // concatenation of many blobs and not just one blob.
        ByteBuffer messageBuf = ByteBuffer.allocate((int) messageInfo.getSize());
        if (messageFormatWriteSet.getStreamToWrite().read(messageBuf.array()) == -1) {
          throw new RuntimeException(
              String.format("Failed to read blob %s of %s bytes as end of stream has been reached",
                  messageInfo.getStoreKey().getID(), messageInfo.getSize()));
        }

        // Prepare to upload blob to Azure blob storage
        // There is no parallelism but we still need to create and pass this object to SDK.
        BlobParallelUploadOptions blobParallelUploadOptions =
            new BlobParallelUploadOptions(new ByteBufferInputStream(messageBuf));
        // To avoid overwriting, pass "*" to setIfNoneMatch(String ifNoneMatch)
        // https://learn.microsoft.com/en-us/java/api/com.azure.storage.blob.blobclient?view=azure-java-stable
        blobParallelUploadOptions.setRequestConditions(new BlobRequestConditions().setIfNoneMatch("*"));
        blobParallelUploadOptions.setMetadata(messageInfo.toMap());
        BlobHttpHeaders headers = new BlobHttpHeaders();
        // Without content-type, download blob warns lack of content-type which floods the log.
        headers.setContentType("application/octet-stream");
        blobParallelUploadOptions.setHeaders(headers);

        ////////////////////////////////// Upload blob to Azure blob storage ////////////////////////////////////////
        storageTimer = azureMetrics.blobUploadTime.time();
        Response<BlockBlobItem> blockBlobItemResponse =
            blobContainerClient.getBlobClient(messageInfo.getStoreKey().getID())
                .uploadWithResponse(blobParallelUploadOptions, Duration.ofMillis(cloudConfig.cloudRequestTimeout),
                    Context.NONE);
        ////////////////////////////////// Upload blob to Azure blob storage ////////////////////////////////////////

        // Metrics and log
        // Success rate is effective, Counter monotonically increases
        azureMetrics.blobUploadSuccessRate.mark();
        // Measure ingestion rate, helps decide fleet size
        azureMetrics.backupSuccessByteRate.mark(messageInfo.getSize());
        logger.trace("Successful upload of blob {} to Azure blob storage with statusCode = {}",
            messageInfo.getStoreKey().getID(), blockBlobItemResponse.getStatusCode());
      } catch (Exception e) {
        logger.error("Failed to upload blob {} to Azure blob storage because {}", messageInfo.getStoreKey().getID(),
            e.getMessage());
        if (e instanceof BlobStorageException
            && ((BlobStorageException) e).getErrorCode() == BlobErrorCode.BLOB_ALREADY_EXISTS) {
          azureMetrics.blobUploadConflictCount.inc();
          // This should never happen. If we invoke put(), then blob must be absent in the Store
          return;
        }
        azureMetrics.blobUploadErrorCount.inc();
        throw new RuntimeException(e);
      } finally {
        if (storageTimer != null) {
          storageTimer.stop();
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
