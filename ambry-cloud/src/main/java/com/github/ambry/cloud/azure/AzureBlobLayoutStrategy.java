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


/**
 * Strategy class that decides where Ambry blobs and tokens are stored in the Azure storage.
 * The decision is governed by two {@link AzureCloudConfig} properties:
 * <ol>
 * <li>nameSchemeVersion: version of the translation scheme for blob names.</li>
 * <li>blobContainerStrategy: strategy for organizing Azure containers.</li>
 * </ol>
 */
public class AzureBlobLayoutStrategy {

  private static final String DASH = "-";
  private static final String UNDERSCORE = "_";
  private static final String BLOB_NAME_SEPARATOR = DASH;
  // Note: Azure container name needs to be lower case
  private static final String TOKEN_CONTAINER_NAME = "replicatokens";
  private final String clusterName;
  private int currentVersion;
  private BlobContainerStrategy blobContainerStrategy;
  private final String containerNameSeparator;

  /**
   * Enum for deciding how Azure storage containers are organized.
   */
  enum BlobContainerStrategy {

    /** Each Azure container corresponds to an Ambry partitionId. */
    PARTITION,

    /** Each Azure container corresponds to an Ambry accountId-containerId combination. */
    CONTAINER;

    /**
     * @return {@link BlobContainerStrategy} using case-insensitive string match.
     * @param enumVal the enum string value.
     * @throws IllegalArgumentException for invalid enumVal.
     */
    static BlobContainerStrategy get(String enumVal) {
      return BlobContainerStrategy.valueOf(enumVal.toUpperCase());
    }
  }

  /** A struct holding Azure container name and file path within the container. */
  static class BlobLayout {
    final String containerName;
    final String blobFilePath;

    /**
     * @param containerName Name of the Azure storage container.
     * @param blobFilePath Name of the file path for the blob within the container.
     */
    BlobLayout(String containerName, String blobFilePath) {
      this.containerName = containerName;
      this.blobFilePath = blobFilePath;
    }
  }

  /**
   * Constuctor for {@link AzureBlobLayoutStrategy}.
   * @param clusterName Name of the Ambry cluster.
   * @param azureCloudConfig The {@link AzureCloudConfig} to use.
   * @throws IllegalArgumentException if invalid config properties are present.
   */
  public AzureBlobLayoutStrategy(String clusterName, AzureCloudConfig azureCloudConfig) {
    this.clusterName = clusterName;
    currentVersion = azureCloudConfig.azureNameSchemeVersion;
    blobContainerStrategy = BlobContainerStrategy.get(azureCloudConfig.azureBlobContainerStrategy);
    // Since account and container Ids can technically be negative numbers, use underscore instead of dash
    // to avoid confusion.
    containerNameSeparator = (blobContainerStrategy == BlobContainerStrategy.CONTAINER) ? UNDERSCORE : DASH;
  }

  /** Test constructor */
  public AzureBlobLayoutStrategy(String clusterName) {
    this.clusterName = clusterName;
    currentVersion = 0;
    blobContainerStrategy = BlobContainerStrategy.PARTITION;
    containerNameSeparator = DASH;
  }

  /**
   * @return the {@link BlobLayout} for the specified blob.
   * @param blobMetadata the {@link CloudBlobMetadata for the data blob.
   */
  public BlobLayout getDataBlobLayout(CloudBlobMetadata blobMetadata) {
    return new BlobLayout(getAzureContainerName(blobMetadata), getAzureBlobName(blobMetadata));
  }

  /**
   * @return the {@link BlobLayout} for the specified blob.
   * @param blobId the id of the blob.
   */
  public BlobLayout getDataBlobLayout(BlobId blobId) {
    return getDataBlobLayout(toMetadata(blobId));
  }

  /**
   * @return the {@link BlobLayout} for token files in the specified partition.
   * @param partitionPath the lexical partitionId name.
   * @param tokenFileName the name of the token file to store.
   */
  public BlobLayout getTokenBlobLayout(String partitionPath, String tokenFileName) {
    if (blobContainerStrategy == BlobContainerStrategy.PARTITION) {
      return new BlobLayout(getClusterAwareAzureContainerName(partitionPath), tokenFileName);
    } else {
      // Use separate dedicated token container, using replicaTokens//partitionPath/replicaTokens
      return new BlobLayout(getClusterAwareAzureContainerName(TOKEN_CONTAINER_NAME),
          partitionPath + "/" + tokenFileName);
    }
  }

  /**
   * Gets the Azure container name for the blob, depending on the configured strategy.
   * @param blobMetadata the {@link CloudBlobMetadata} to store.
   * @return the container name to use.
   */
  private String getAzureContainerName(CloudBlobMetadata blobMetadata) {
    String baseContainerName =
        (blobContainerStrategy == BlobContainerStrategy.PARTITION) ? blobMetadata.getPartitionId()
            : blobMetadata.getAccountId() + containerNameSeparator + blobMetadata.getContainerId();
    return getClusterAwareAzureContainerName(baseContainerName);
  }

  /**
   * @return the blob name to use in Azure storage.
   * @param blobMetadata the {@link CloudBlobMetadata} to store.
   */
  private String getAzureBlobName(CloudBlobMetadata blobMetadata) {
    String blobIdStr = blobMetadata.getId();
    int nameVersion = blobMetadata.getNameSchemeVersion();
    switch (nameVersion) {
      default:
        // Use the last four chars as prefix to assist in Azure sharding, since beginning of blobId has little variation.
        return blobIdStr.substring(blobIdStr.length() - 4) + BLOB_NAME_SEPARATOR + blobIdStr;
    }
  }

  /**
   * Converts an input container name to one that is multi-cluster friendly.
   * @param inputName the input container name.
   * @return a container name scoped to the cluster.
   */
  private String getClusterAwareAzureContainerName(String inputName) {
    String containerName = clusterName + containerNameSeparator + inputName;
    return containerName.toLowerCase();
  }

  /**
   * Convert a {@link BlobId} to a {@link CloudBlobMetadata}.
   * @param blobId the input {@link BlobId}.
   * @return the output {@link CloudBlobMetadata}.
   */
  private CloudBlobMetadata toMetadata(BlobId blobId) {
    return new CloudBlobMetadata(blobId, 0, Utils.Infinite_Time, 0,
        CloudBlobMetadata.EncryptionOrigin.NONE).setNameSchemeVersion(currentVersion);
  }
}
