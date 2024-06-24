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
package com.github.ambry.cloud.azure;

import com.azure.core.http.rest.PagedResponse;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobListDetails;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.AccountService;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ServerConfig;
import com.github.ambry.config.VerifiableProperties;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides methods to interact with Azurite - Azure Storage Emulator.
 */
public class AzuriteUtils {
  public static final Logger logger = LoggerFactory.getLogger(AzuriteUtils.class);
  public static final String AZURITE_CONNECTION_STRING =  "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;" +
      "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;" +
      "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;";
  public static final String AZURITE_TABLE_CONNECTION_STRING =  "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;" +
      "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;" +
      "TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;";
  /**
   * Checks connection to Azurite
   * @return True if successful, else false
   */
  public boolean connectToAzurite() {
    /**
     * To run the azurite azure storage desktop emulator:
     * $ npm install -g azurite
     * $ azurite
     *
     * Run the tests on your desktop.
     */
    try {
      getAzuriteClient(getAzuriteConnectionProperties(), new MetricRegistry(), null, null);
      return true;
    } catch (Exception e) {
      logger.error("Failed to connect to Azurite due to {}", e.toString());
      return false;
    }
  }

  /**
   * Returns a client to connect to Azurite
   * @param metricRegistry Metrics
   * @param clusterMap Cluster map
   * @return Sync client to connect to Azurite
   */
  public AzureCloudDestinationSync getAzuriteClient(Properties properties, MetricRegistry metricRegistry,
      ClusterMap clusterMap, AccountService accountService)
      throws ReflectiveOperationException {
    return new AzureCloudDestinationSync(new VerifiableProperties(properties), metricRegistry,  clusterMap, accountService);
  }

  public Properties getAzuriteConnectionProperties() {
    Properties properties = new Properties();
    properties.setProperty(ClusterMapConfig.CLUSTERMAP_CLUSTER_NAME, "localhost");
    properties.setProperty(ClusterMapConfig.CLUSTERMAP_DATACENTER_NAME, "localhost");
    properties.setProperty(ClusterMapConfig.CLUSTERMAP_HOST_NAME, "localhost");
    properties.setProperty(ClusterMapConfig.CLUSTERMAP_RESOLVE_HOSTNAMES, String.valueOf(false));
    properties.setProperty(AzureCloudConfig.COSMOS_ENDPOINT, "does_not_matter");
    properties.setProperty(AzureCloudConfig.COSMOS_DATABASE, "does_not_matter");
    properties.setProperty(AzureCloudConfig.COSMOS_COLLECTION, "does_not_matter");
    properties.setProperty(AzureCloudConfig.AZURE_STORAGE_CONNECTION_STRING, AzuriteUtils.AZURITE_CONNECTION_STRING);
    properties.setProperty(AzureCloudConfig.AZURE_TABLE_CONNECTION_STRING,
        AzuriteUtils.AZURITE_TABLE_CONNECTION_STRING);
    properties.setProperty(AzureCloudConfig.AZURE_NAME_SCHEME_VERSION, String.valueOf(1)); // always 1 for vcr-2.0
    properties.setProperty(AzureCloudConfig.AZURE_BLOB_CONTAINER_STRATEGY,
        AzureBlobLayoutStrategy.BlobContainerStrategy.PARTITION.name()); // always PARTITION for vcr-2.0
    properties.setProperty(ServerConfig.SERVER_MESSAGE_TRANSFORMER, ""); // not using for backups
    properties.setProperty(CloudConfig.CLOUD_MAX_ATTEMPTS, "1"); // 1 for testing
    properties.setProperty(AzureCloudConfig.AZURE_BLOB_STORAGE_MAX_RESULTS_PER_PAGE, String.valueOf(1)); // 1 for testing
    return properties;
  }

  /**
   * FOR TESTS ONLY !!
   * Clears all blobs in an Azure container
   * @param testPartitionId
   * @param azureCloudDestinationSync
   * @param verifiableProperties
   */
  public void clearContainer(PartitionId testPartitionId, AzureCloudDestinationSync azureCloudDestinationSync,
      VerifiableProperties verifiableProperties) {
    ClusterMapConfig clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    AzureBlobLayoutStrategy
        azureBlobLayoutStrategy = new AzureBlobLayoutStrategy(clusterMapConfig.clusterMapClusterName,
        new AzureCloudConfig(verifiableProperties));
    String blobContainerName = azureBlobLayoutStrategy.getClusterAwareAzureContainerName(
        String.valueOf(testPartitionId.getId()));
    BlobContainerClient blobContainerClient = azureCloudDestinationSync.getBlobStore(blobContainerName);
    if (blobContainerClient == null) {
      logger.info("Blob container {} does not exist", blobContainerName);
      return;
    }
    ListBlobsOptions listBlobsOptions =
        new ListBlobsOptions().setDetails(new BlobListDetails().setRetrieveMetadata(true));
    String continuationToken = null;
    for (PagedResponse<BlobItem> blobItemPagedResponse :
        blobContainerClient.listBlobs(listBlobsOptions, null).iterableByPage(continuationToken)) {
      continuationToken = blobItemPagedResponse.getContinuationToken();
      for (BlobItem blobItem : blobItemPagedResponse.getValue()) {
        blobContainerClient.getBlobClient(blobItem.getName()).delete();
      }
      if (continuationToken == null) {
        logger.info("Reached end-of-partition as Azure blob storage continuationToken is null");
        break;
      }
    }
  }
}
