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

import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.DirectConnectionConfig;
import com.azure.cosmos.GatewayConnectionConfig;
import com.azure.cosmos.ThrottlingRetryOptions;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.Container;
import com.github.ambry.account.ContainerBuilder;
import com.github.ambry.cloud.CloudBlobMetadata;
import com.github.ambry.cloud.CloudDestinationFactory;
import com.github.ambry.cloud.CloudRequestAgent;
import com.github.ambry.cloud.CloudStorageException;
import com.github.ambry.cloud.VcrMetrics;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.AccountTestUtils;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.cloud.azure.AzureTestUtils.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Test for {@link AzureContainerCompactor} class.
 */
@Ignore
@RunWith(MockitoJUnitRunner.class)
public class AzureContainerCompactorIntegrationTest {

  private static final Logger logger = LoggerFactory.getLogger(AzureContainerCompactor.class);
  private final Random random = new Random();

  private final AzureCloudDestination cloudDestination;
  private final AzureContainerCompactor azureContainerCompactor;
  private final ClusterMap clusterMap;
  private final AzureCloudConfig azureCloudConfig;
  private final CloudConfig cloudConfig;
  private final CloudRequestAgent cloudRequestAgent;
  private final long testPartitionId = 666;
  private final int blobSize = 1024;
  private final byte dataCenterId = 66;
  private final List<String> assignedPartitions = Arrays.asList(String.valueOf(testPartitionId));
  private final List<? extends PartitionId> allPartitions = Arrays.asList(new MockPartitionId(testPartitionId, null));

  public AzureContainerCompactorIntegrationTest() throws ReflectiveOperationException, CloudStorageException {
    // TODO Create the required cosmos table as well as the required azure blob.
    String PROPS_FILE_NAME = "azure-test.properties";
    Properties testProperties = new Properties();
    VerifiableProperties verifiableProperties;
    try (InputStream input = this.getClass().getClassLoader().getResourceAsStream(PROPS_FILE_NAME)) {
      if (input == null) {
        throw new IllegalStateException("Could not find resource: " + PROPS_FILE_NAME);
      }
      testProperties.load(input);
    } catch (IOException ex) {
      throw new IllegalStateException("Could not load properties from resource: " + PROPS_FILE_NAME);
    }
    testProperties.setProperty("clustermap.cluster.name", "Integration-Test");
    testProperties.setProperty("clustermap.datacenter.name", "uswest");
    testProperties.setProperty("clustermap.host.name", "localhost");
    testProperties.setProperty("kms.default.container.key",
        "B374A26A71490437AA024E4FADD5B497FDFF1A8EA6FF12F6FB65AF2720B59CCF");

    testProperties.setProperty(CloudConfig.CLOUD_DELETED_BLOB_RETENTION_DAYS, String.valueOf(1));
    testProperties.setProperty(AzureCloudConfig.AZURE_PURGE_BATCH_SIZE, "10");
    verifiableProperties = new VerifiableProperties(testProperties);
    cloudConfig = new CloudConfig(verifiableProperties);
    MetricRegistry registry = new MetricRegistry();
    clusterMap = mock(ClusterMap.class);
    doReturn(allPartitions).when(clusterMap).getAllPartitionIds(null);
    CloudDestinationFactory cloudDestinationFactory =
        Utils.getObj(cloudConfig.cloudDestinationFactoryClass, verifiableProperties, registry, clusterMap);
    cloudDestination = (AzureCloudDestination) cloudDestinationFactory.getCloudDestination();
    azureCloudConfig = new AzureCloudConfig(verifiableProperties);
    MetricRegistry metricRegistry = new MetricRegistry();
    azureContainerCompactor = new AzureContainerCompactor(cloudDestination.getAzureBlobDataAccessor(),
        cloudDestination.getCosmosDataAccessor(), cloudConfig, azureCloudConfig, new VcrMetrics(metricRegistry),
        new AzureMetrics(metricRegistry));
    cloudRequestAgent =
        new CloudRequestAgent(new CloudConfig(verifiableProperties), new VcrMetrics(new MetricRegistry()));
  }

  @After
  public void destroy() throws IOException, CosmosException {
    cleanup();
    // TODO destroy the abs blob and cosmos db
    if (cloudDestination != null) {
      cloudDestination.close();
    }
  }

  @Test
  public void testDeprecateContainers() throws CloudStorageException, CosmosException {
    cleanup();
    // Add new containers and verify that they are persisted in cloud.
    Set<Container> containers = generateContainers(5);
    cloudDestination.deprecateContainers(containers);
    verifyCosmosData(containers);
    verifyCheckpoint(containers);

    // Add more containers and verify that they are persisted after the checkpoint.
    containers.addAll(generateContainers(5));
    cloudDestination.deprecateContainers(containers);
    verifyCosmosData(containers);
    verifyCheckpoint(containers);
  }

  @Test
  public void testCompactAssignedDeprecatedContainers() throws CloudStorageException, CosmosException {
    // Create a deprecated container.
    Set<Container> containers = generateContainers(1);
    cloudDestination.deprecateContainers(containers);
    verifyCosmosData(containers);
    verifyCheckpoint(containers);
    Container testContainer = containers.iterator().next();

    // Create blobs in the deprecated container and test partition.
    int numBlobs = 100;
    PartitionId partitionId = new MockPartitionId(testPartitionId, MockClusterMap.DEFAULT_PARTITION_CLASS);
    long creationTime = System.currentTimeMillis();
    Map<BlobId, byte[]> blobIdtoDataMap =
        createUnencryptedPermanentBlobs(numBlobs, dataCenterId, testContainer.getParentAccountId(),
            testContainer.getId(), partitionId, blobSize, cloudRequestAgent, cloudDestination, creationTime);

    // Assert that blobs exist.
    Map<String, CloudBlobMetadata> metadataMap =
        getBlobMetadataWithRetry(new ArrayList<>(blobIdtoDataMap.keySet()), partitionId.toPathString(),
            cloudRequestAgent, cloudDestination);
    assertEquals("Unexpected size of returned metadata map", numBlobs, metadataMap.size());

    // compact the deprecated container.
    cloudDestination.getContainerCompactor()
        .compactAssignedDeprecatedContainers(Collections.singletonList(partitionId));

    // Assert that deprecated container's blobs don't exist anymore.
    assertTrue("Expected empty set after container compaction",
        getBlobMetadataWithRetry(new ArrayList<>(blobIdtoDataMap.keySet()), partitionId.toPathString(),
            cloudRequestAgent, cloudDestination).isEmpty());
    cleanup();
  }

  /**
   * Verifies that the data stored in cosmos table is as expected.
   * @param containers {@link Set} of {@link Container}s that should be present in cosmos.
   */
  private void verifyCosmosData(Set<Container> containers) throws CosmosException {
    Set<CosmosContainerDeletionEntry> entries = cloudDestination.getCosmosDataAccessor().getDeprecatedContainers(100);
    Assert.assertEquals(containers.size(), entries.size());
    Set<Short> containerIds = containers.stream().map(Container::getId).collect(Collectors.toSet());
    for (CosmosContainerDeletionEntry entry : entries) {
      Assert.assertTrue(containerIds.contains(entry.getContainerId()));
    }
  }

  /**
   * Verifies that the correct checkpoint is stored in ABS.
   * @param containers {@link Set} of {@link Container}s for which checkpoint is to be verified.
   * @throws CloudStorageException
   */
  private void verifyCheckpoint(Set<Container> containers) throws CloudStorageException {
    long maxDeleteTimestamp =
        containers.stream().max(Comparator.comparing(Container::getDeleteTriggerTime)).get().getDeleteTriggerTime();
    Assert.assertEquals(azureContainerCompactor.getLatestContainerDeletionTime(), maxDeleteTimestamp);
  }

  /**
   * Generate specified number of {@link Container}s.
   * @param numContainers number of {@link Container}s to generate.
   * @return {@link Set} of {@link Container}s.
   */
  private Set<Container> generateContainers(int numContainers) {
    Set<Container> containers = new HashSet<>();
    long currentTimestamp = System.currentTimeMillis();
    AtomicInteger ctr = new AtomicInteger(0);
    List<ContainerBuilder> containerBuilders =
        AccountTestUtils.generateContainerBuilders(numContainers, Utils.getRandomShort(random));
    containers.addAll(containerBuilders.stream().map(containerBuilder -> {
      containerBuilder.setDeleteTriggerTime(currentTimestamp - numContainers + ctr.getAndIncrement())
          .setStatus(
              random.nextBoolean() ? Container.ContainerStatus.DELETE_IN_PROGRESS : Container.ContainerStatus.INACTIVE);
      return containerBuilder.build();
    }).collect(Collectors.toSet()));
    return containers;
  }

  /**
   * Cleanup entries in the cosmos db container deletion table.
   * @throws CosmosException in case of any exception.
   */
  private void cleanup() throws CosmosException {
    // Note: retry decisions are made at CloudBlobStore level.  Configure Cosmos with no retries.
    ThrottlingRetryOptions throttlingRetryOptions = new ThrottlingRetryOptions();
    throttlingRetryOptions.setMaxRetryAttemptsOnThrottledRequests(0);
    CosmosClientBuilder cosmosClientBuilder = new CosmosClientBuilder().endpoint(azureCloudConfig.cosmosEndpoint)
        .key(azureCloudConfig.cosmosKey)
        .throttlingRetryOptions(throttlingRetryOptions)
        .consistencyLevel(com.azure.cosmos.ConsistencyLevel.SESSION);
    if (azureCloudConfig.cosmosDirectHttps) {
      logger.info("Using CosmosDB DirectHttps connection mode");
      cosmosClientBuilder.directMode(new DirectConnectionConfig());
    } else {
      cosmosClientBuilder.gatewayMode(new GatewayConnectionConfig());
    }
    CosmosAsyncClient cosmosAsyncClient = cosmosClientBuilder.buildAsyncClient();

    Set<CosmosContainerDeletionEntry> entries = cloudDestination.getCosmosDataAccessor().getDeprecatedContainers(100);
    AtomicBoolean error = new AtomicBoolean(false);
    while (!entries.isEmpty() && !error.get()) {
      entries.stream().forEach(entry -> {
        try {
          cloudRequestAgent.doWithRetries(() -> CosmosDataAccessor.executeCosmosAction(
              () -> cosmosAsyncClient.getDatabase(azureCloudConfig.cosmosDatabase)
                  .getContainer(azureCloudConfig.cosmosDeletedContainerCollection)
                  .deleteItem(entry.getId(), new com.azure.cosmos.models.PartitionKey(entry.getId()))
                  .block(), null), "Test Cleanup", entry.getId());
        } catch (CloudStorageException ex) {
          logger.warn("Failed to delete container deprecation entry {}. Unable to cleanup", entry);
          error.set(true);
        }
      });
      entries = cloudDestination.getCosmosDataAccessor().getDeprecatedContainers(100);
    }
  }
}
