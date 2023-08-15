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

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobContainerItem;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobStorageException;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.cloud.azure.AzureCloudConfig;
import com.github.ambry.cloud.azure.AzureMetrics;
import com.github.ambry.cloud.azure.ConnectionStringBasedStorageClient;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.server.StoreManager;
import com.github.ambry.store.Store;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Storage manager that tracks {@link AzureStorage} objects.
 */
public class AzureStorageManager implements StoreManager {
  private static final Logger logger = LoggerFactory.getLogger(CloudStorageManager.class);
  protected final ConcurrentHashMap<PartitionId, AzureStorage> partitionToAzureStorage;
  protected final MetricRegistry metricRegistry;
  protected final CloudConfig cloudConfig;
  protected final AzureCloudConfig azureCloudConfig;
  protected final AzureMetrics azureMetrics;
  protected final BlobServiceClient azureStorageClient;
  protected final VerifiableProperties properties;
  protected final ClusterMap clusterMap;

  /**
   * Constructor
   * @param properties Configuration parameters
   * @param metricRegistry Metrics
   * @param clusterMap Cluster-map
   */
  public AzureStorageManager(VerifiableProperties properties, MetricRegistry metricRegistry, ClusterMap clusterMap) {
    this.partitionToAzureStorage = new ConcurrentHashMap<>();
    this.metricRegistry = metricRegistry;
    this.azureMetrics = new AzureMetrics(metricRegistry);
    this.cloudConfig = new CloudConfig(properties);
    this.azureCloudConfig = new AzureCloudConfig(properties);
    this.properties = properties;
    this.clusterMap = clusterMap;
    this.azureStorageClient = new ConnectionStringBasedStorageClient(cloudConfig, azureCloudConfig, azureMetrics).getStorageSyncClient();
    testAzureStorageConnectivity();
    logger.info("Created AzureStorageManager");
  }

  /**
   * Tests connection to Azure blob storage
   */
  protected void testAzureStorageConnectivity() {
    PagedIterable<BlobContainerItem> blobContainerItemPagedIterable = azureStorageClient.listBlobContainers();
    for (BlobContainerItem blobContainerItem : blobContainerItemPagedIterable) {
      logger.info("blobContainer = {}", blobContainerItem.getName());
      break;
    }
  }

  /**
   * Returns blob container in Azure for an Ambry partition
   * @param partitionId Ambry partition
   * @return blobContainerClient
   */
  protected BlobContainerClient getBlobStore(PartitionId partitionId) {
    BlobContainerClient blobContainerClient;
    try {
      blobContainerClient = azureStorageClient.getBlobContainerClient(String.valueOf(partitionId.getId()));
    } catch (BlobStorageException blobStorageException) {
      if (blobStorageException.getErrorCode().equals(BlobErrorCode.CONTAINER_NOT_FOUND)) {
        logger.error("Blob container for partition {} not found due to {}", partitionId.getId(),
            blobStorageException.getServiceMessage());
        return null;
      }
      logger.error("Failed to get blob container for partition {} due to {}", partitionId.getId(),
          blobStorageException.getServiceMessage());
      throw blobStorageException;
    }
    return blobContainerClient;
  }

  /**
   * Creates blob container in Azure for an Ambry partition
   * @param partitionId Ambry partition
   * @return blobContainerClient
   */
  protected BlobContainerClient createBlobStore(PartitionId partitionId) {
    BlobContainerClient blobContainerClient;
    try {
      blobContainerClient = azureStorageClient.createBlobContainer(String.valueOf(partitionId.getId()));
    } catch (BlobStorageException blobStorageException) {
      if (blobStorageException.getErrorCode().equals(BlobErrorCode.CONTAINER_ALREADY_EXISTS)) {
        logger.info("Blob container for partition {} already exists", partitionId.getId());
        return getBlobStore(partitionId);
      }
      logger.error("Failed to create blob container for partition {} due to {}", partitionId.getId(),
          blobStorageException.getServiceMessage());
      throw blobStorageException;
    }
    return blobContainerClient;
  }

  /**
   * Creates an object that stores blobs in Azure blob storage
   * @param partitionId Partition ID
   * @return {@link AzureStorage}
   */
  protected AzureStorage createOrGetBlobStore(PartitionId partitionId) {
    AzureStorage storage = partitionToAzureStorage.get(partitionId);
    if (storage != null) {
      return storage;
    }

    BlobContainerClient blobContainerClient = getBlobStore(partitionId);
    if (blobContainerClient == null) {
      blobContainerClient = createBlobStore(partitionId);
    }

    if (blobContainerClient == null) {
      // TODO : metric
      String errMsg = String.format("Blob container for partition %s is null", partitionId.getId());
      logger.error(errMsg);
      throw new RuntimeException(errMsg);
    }

    storage = new AzureStorage(properties, metricRegistry, clusterMap, blobContainerClient);
    partitionToAzureStorage.put(partitionId, storage);
    return storage;
  }

  /**
   * Creates a {@link AzureStorage} object for a replica if absent.
   * Else, returns the existing {@link AzureStorage} for the replica.
   * @param replica the {@link ReplicaId} of the {@link Store} which would be added.
   * @return True if created {@link AzureStorage} object, false otherwise.
   */
  @Override
  public boolean addBlobStore(ReplicaId replica) {
    return createOrGetBlobStore(replica.getPartitionId()) != null;
  }

  /**
   * Removes {@link AzureStorage} for a replica if it exists
   * @param id the {@link PartitionId} associated with store
   * @return True if removed {@link AzureStorage} object, false otherwise.
   */
  @Override
  public boolean removeBlobStore(PartitionId id) {
    return partitionToAzureStorage.remove(id, partitionToAzureStorage.get(id));
  }

  /**
   * Creates a {@link AzureStorage} object for a replica if absent.
   * Else, returns the existing {@link AzureStorage} for the replica.
   * @param id the {@link PartitionId} to find the store for.
   * @return The {@link AzureStorage} for the given partition
   */
  @Override
  public Store getStore(PartitionId id) {
    return createOrGetBlobStore(id);
  }

  @Override
  public boolean startBlobStore(PartitionId id) {
    return createOrGetBlobStore(id) != null;
  }

  @Override
  public boolean shutdownBlobStore(PartitionId id) {
    return true;
  }

  @Override
  public ServerErrorCode checkLocalPartitionStatus(PartitionId partition, ReplicaId localReplica) {
    return ServerErrorCode.No_Error;
  }

  /////////////////////////////////////////// Unimplemented methods /////////////////////////////////////////////////

  @Override
  public boolean scheduleNextForCompaction(PartitionId id) {
    throw new UnsupportedOperationException("Method not supported");
  }

  @Override
  public boolean controlCompactionForBlobStore(PartitionId id, boolean enabled) {
    throw new UnsupportedOperationException("Method not supported");
  }

  @Override
  public List<PartitionId> setBlobStoreStoppedState(List<PartitionId> partitionIds, boolean markStop) {
    throw new UnsupportedOperationException("Method not supported");
  }

  @Override
  public ReplicaId getReplica(String partitionName) {
    throw new UnsupportedOperationException("Method not supported");
  }

  @Override
  public Collection<PartitionId> getLocalPartitions() {
    throw new UnsupportedOperationException("Method not supported");
  }
}
