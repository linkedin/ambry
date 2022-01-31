/**
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.tools.admin;

import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobContainersOptions;
import com.azure.storage.blob.specialized.BlockBlobAsyncClient;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.cloud.azure.AzureBlobDataAccessor;
import com.github.ambry.cloud.azure.AzureBlobLayoutStrategy;
import com.github.ambry.cloud.azure.AzureCloudConfig;
import com.github.ambry.cloud.azure.AzureMetrics;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.Utils;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility to delete the replica tokens for all partitions.
 * Very dangerous tool.  Please know what you're doing and wear a hard hat at all times.
 * Required properties:
 *  - "azure.storage.connection.string" for the storage account.
 *  - "cosmos.endpoint", "cosmos.database", "cosmos.collection" , "cosmos.key" (set to any nonempty value)
 *  - "clustermap.cluster.name" used to restrict blob container name search (e.g. "main-123")
 * Optional properties:
 *  - "vcr.proxy.host" name of the proxy host to tunnel through.
 *  - "azure.blob.container.strategy" if the account is sharded by container instead of partition.
 */
public class AzureTokenResetTool {

  private static final Logger logger = LoggerFactory.getLogger(AzureTokenResetTool.class);
  private static BlobServiceAsyncClient storageAsyncClient;

  public static void main(String[] args) {
    String commandName = AzureTokenResetTool.class.getSimpleName();
    try {
      VerifiableProperties verifiableProperties = ToolUtils.getVerifiableProperties(args);
      String clusterName = verifiableProperties.getString(ClusterMapConfig.CLUSTERMAP_CLUSTER_NAME);
      CloudConfig cloudConfig = new CloudConfig(verifiableProperties);
      AzureCloudConfig azureCloudConfig = new AzureCloudConfig(verifiableProperties);
      AzureMetrics azureMetrics = new AzureMetrics(new MetricRegistry());
      AzureBlobLayoutStrategy blobLayoutStrategy = new AzureBlobLayoutStrategy(clusterName, azureCloudConfig);
      AzureBlobDataAccessor dataAccessor =
          new AzureBlobDataAccessor(cloudConfig, azureCloudConfig, blobLayoutStrategy, azureMetrics);
      storageAsyncClient = dataAccessor.getStorageClient();

      int tokensDeleted = resetTokens(clusterName);
      logger.info("Deleted tokens for {} partitions", tokensDeleted);
    } catch (Exception ex) {
      ex.printStackTrace();
      logger.error("Command {} failed", commandName, ex);
      System.exit(1);
    }
  }

  /**
   * Reset the offset token by deleting the blob from the container.
   * @param containerPrefix the prefix used to filter on Azure containers
   *                       (in case the storage account hosts multiple Ambry clusters).
   * @return the number of tokens successfully reset.
   */
  public static int resetTokens(String containerPrefix) throws BlobStorageException {
    AtomicInteger tokensDeleted = new AtomicInteger(0);
    ListBlobContainersOptions listOptions = new ListBlobContainersOptions().setPrefix(containerPrefix);
    storageAsyncClient.listBlobContainers(listOptions).toIterable().forEach(blobContainerItem -> {
      BlockBlobAsyncClient blockBlobAsyncClient =
          storageAsyncClient.getBlobContainerAsyncClient(blobContainerItem.getName())
              .getBlobAsyncClient(ReplicationConfig.REPLICA_TOKEN_FILE_NAME)
              .getBlockBlobAsyncClient();
      try {
        if (blockBlobAsyncClient.exists().toFuture().join()) {
          blockBlobAsyncClient.delete().toFuture().join();
          tokensDeleted.incrementAndGet();
          logger.info("Deleted token for partition {}", blobContainerItem.getName());
        }
      } catch (Exception exception) {
        exception = Utils.extractFutureExceptionCause(exception);
        logger.error("Failed delete for {}", blobContainerItem.getName(), exception);
      }
    });
    return tokensDeleted.get();
  }
}
