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
package com.github.ambry.cloud.azure;

import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.commons.BlobId;
import com.github.ambry.utils.Utils;

// TODO: if blobs are organized by container, then need a different container for tokens
// Use separate dedicated token container, using replicaTokens//partitionPath/replicaTokens

public class AzureBlobLayoutStrategy {

  private static final String SEPARATOR = "-";
  private static final String TOKEN_CONTAINER_NAME = "replicatokens";
  private final String clusterName;
  private int currentVersion;
  private BlobContainerStrategy blobContainerStrategy;

  enum BlobContainerStrategy {
    PARTITION, CONTAINER
  }

  static class BlobLayout {
    public String containerName;
    public String blobFilePath;

    public BlobLayout(String containerName, String blobFilePath) {
      this.containerName = containerName;
      this.blobFilePath = blobFilePath;
    }
  }

  public AzureBlobLayoutStrategy(String clusterName, AzureCloudConfig azureCloudConfig) {
    this.clusterName = clusterName;
    currentVersion = 0;  // azureCloudConfig.nameSchemeVersion;
    blobContainerStrategy = BlobContainerStrategy.PARTITION; // azureCloudConfig.blobContainerStrategy
  }

  /**
   * @return the name of the Azure storage container to store the specified blob.
   * @param blobMetadata the blob metadata.
   */
  public BlobLayout getDataBlobLayout(CloudBlobMetadata blobMetadata) {
    String baseContainerName =
        (blobContainerStrategy == BlobContainerStrategy.PARTITION) ? blobMetadata.getPartitionId()
            : blobMetadata.getAccountId() + SEPARATOR + blobMetadata.getContainerId();
    String blobName = getAzureBlobName(blobMetadata.getId(), blobMetadata.getNameSchemeVersion());
    return new BlobLayout(baseContainerName, blobName);
  }

  /**
   * @return the name of the Azure storage container to store the specified blob.
   * @param blobId the id of the blob.
   */
  public BlobLayout getDataBlobLayout(BlobId blobId) {
    return getDataBlobLayout(toMetadata(blobId));
  }

  public BlobLayout getTokenBlobLayout(String partitionPath, String tokenFileName) {
    if (blobContainerStrategy == BlobContainerStrategy.PARTITION) {
      return new BlobLayout(getClusterAwareAzureContainerName(partitionPath), tokenFileName);
    } else {
      return new BlobLayout(getClusterAwareAzureContainerName(TOKEN_CONTAINER_NAME),
          partitionPath + "/" + tokenFileName);
    }
  }

  private String getAzureBlobName(String blobIdStr, int nameVersion) {
    switch (nameVersion) {
      default:
        // Use the last four chars as prefix to assist in Azure sharding, since beginning of blobId has little variation.
        return blobIdStr.substring(blobIdStr.length() - 4) + SEPARATOR + blobIdStr;
    }
  }

  private String getClusterAwareAzureContainerName(String inputName) {
    String containerName = clusterName + SEPARATOR + inputName;
    return containerName.toLowerCase();
  }

  private CloudBlobMetadata toMetadata(BlobId blobId) {
    return new CloudBlobMetadata(blobId, 0, Utils.Infinite_Time, 0,
        CloudBlobMetadata.EncryptionOrigin.NONE).setNameSchemeVersion(currentVersion);
  }
}
