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
import com.github.ambry.cloud.azure.AzureBlobLayoutStrategy.BlobLayout;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.VerifiableProperties;
import java.util.Properties;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.*;


/** Test cases for {@link AzureBlobLayoutStrategy} */
@RunWith(MockitoJUnitRunner.class)
public class AzureBlobLayoutStrategyTest {

  private Properties configProps = new Properties();
  private BlobId blobId;
  private final String clusterName = "main";
  private final String tokenFileName = "replicaTokens";
  private final String partitionPath = String.valueOf(AzureTestUtils.partition);

  @Before
  public void setup() {
    blobId = AzureTestUtils.generateBlobId();
    AzureTestUtils.setConfigProperties(configProps);
  }

  /** Test default strategy and make sure it is partition-based */
  @Test
  public void testDefaultStrategy() {
    AzureCloudConfig azureCloudConfig = new AzureCloudConfig(new VerifiableProperties(configProps));
    AzureBlobLayoutStrategy strategy = new AzureBlobLayoutStrategy(clusterName, azureCloudConfig);
    BlobLayout layout = strategy.getDataBlobLayout(blobId);
    // Container name should be main-666
    String expectedContainerName = clusterName + "-" + partitionPath;
    String blobIdStr = blobId.getID();
    String expectedBlobName = blobIdStr.substring(blobIdStr.length() - 4) + "-" + blobIdStr;
    checkLayout(layout, expectedContainerName, expectedBlobName);
    CloudBlobMetadata blobMetadata = new CloudBlobMetadata(blobId, 0, 0, 0, null);
    layout = strategy.getDataBlobLayout(blobMetadata);
    checkLayout(layout, expectedContainerName, expectedBlobName);

    // Tokens should go in same container
    layout = strategy.getTokenBlobLayout(partitionPath, tokenFileName);
    checkLayout(layout, expectedContainerName, tokenFileName);
  }

  /** Test container layout strategy */
  @Test
  public void testContainerStrategy() {
    configProps.setProperty(AzureCloudConfig.AZURE_BLOB_CONTAINER_STRATEGY, "container");
    AzureCloudConfig azureCloudConfig = new AzureCloudConfig(new VerifiableProperties(configProps));
    AzureBlobLayoutStrategy strategy = new AzureBlobLayoutStrategy(clusterName, azureCloudConfig);
    BlobLayout layout = strategy.getDataBlobLayout(blobId);
    // Container name should be main-101-5
    String expectedContainerName =
        String.format("%s_%d_%d", clusterName, AzureTestUtils.accountId, AzureTestUtils.containerId);
    String blobIdStr = blobId.getID();
    String expectedBlobName = blobIdStr.substring(blobIdStr.length() - 4) + "-" + blobIdStr;
    checkLayout(layout, expectedContainerName, expectedBlobName);
    CloudBlobMetadata blobMetadata = new CloudBlobMetadata(blobId, 0, 0, 0, null);
    layout = strategy.getDataBlobLayout(blobMetadata);
    checkLayout(layout, expectedContainerName, expectedBlobName);

    // Tokens should go in their own container: main_replicaTokens
    layout = strategy.getTokenBlobLayout(partitionPath, tokenFileName);
    expectedContainerName = clusterName + "_" + tokenFileName.toLowerCase();
    expectedBlobName = partitionPath + "/" + tokenFileName;
    checkLayout(layout, expectedContainerName, expectedBlobName);
  }

  /** Test that only valid strategy properties are accepted. */
  @Test
  public void testValidStrategies() {
    // Valid ones
    configProps.setProperty(AzureCloudConfig.AZURE_BLOB_CONTAINER_STRATEGY, "CONTAINER");
    AzureCloudConfig azureCloudConfig = new AzureCloudConfig(new VerifiableProperties(configProps));
    AzureBlobLayoutStrategy strategy = new AzureBlobLayoutStrategy(clusterName, azureCloudConfig);
    configProps.setProperty(AzureCloudConfig.AZURE_BLOB_CONTAINER_STRATEGY, "Partition");
    azureCloudConfig = new AzureCloudConfig(new VerifiableProperties(configProps));
    strategy = new AzureBlobLayoutStrategy(clusterName, azureCloudConfig);

    // Invalid ones
    configProps.setProperty(AzureCloudConfig.AZURE_BLOB_CONTAINER_STRATEGY, "SomethingElse");
    azureCloudConfig = new AzureCloudConfig(new VerifiableProperties(configProps));
    try {
      strategy = new AzureBlobLayoutStrategy(clusterName, azureCloudConfig);
      fail("Expected failure");
    } catch (IllegalArgumentException ex) {
    }
  }

  /**
   * Verify the layout is as expected.
   * @param layout the {@link BlobLayout} to check.
   * @param expectedContainerName the expected container name.
   * @param expectedBlobPath the expected blob path.
   */
  private static void checkLayout(BlobLayout layout, String expectedContainerName, String expectedBlobPath) {
    assertEquals("Unexpected container name", expectedContainerName, layout.containerName);
    assertEquals("Unexpected blob path", expectedBlobPath, layout.blobFilePath);
  }
}
