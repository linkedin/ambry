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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.Container;
import com.github.ambry.account.ContainerBuilder;
import com.github.ambry.cloud.CloudDestinationFactory;
import com.github.ambry.cloud.CloudStorageException;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.AccountTestUtils;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.io.InputStream;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;


/**
 * Test for {@link AzureContainerCompactor} class.
 */
@Ignore
@RunWith(MockitoJUnitRunner.class)
public class AzureContainerCompactorIntegrationTest {

  private final Random random = new Random();

  private final AzureCloudDestination cloudDestination;
  private final AzureContainerCompactor azureContainerCompactor;
  private final ClusterMap clusterMap;

  public AzureContainerCompactorIntegrationTest() throws ReflectiveOperationException {
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
    CloudConfig cloudConfig = new CloudConfig(verifiableProperties);
    MetricRegistry registry = new MetricRegistry();
    CloudDestinationFactory cloudDestinationFactory =
        Utils.getObj(cloudConfig.cloudDestinationFactoryClass, verifiableProperties, registry);
    clusterMap = Mockito.mock(ClusterMap.class);
    cloudDestination = (AzureCloudDestination) cloudDestinationFactory.getCloudDestination();
    azureContainerCompactor = new AzureContainerCompactor(cloudDestination.getAzureBlobDataAccessor(),
        cloudDestination.getCosmosDataAccessor(), cloudConfig, null, null);
  }

  @After
  public void destroy() throws IOException {
    // TODO destroy the abs blob and cosmos db
    if (cloudDestination != null) {
      cloudDestination.close();
    }
  }

  @Test
  public void testDeprecatedContainers() throws CloudStorageException {
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

  /**
   * Verifies that the data stored in cosmos table is as expected.
   * @param containers {@link Set} of {@link Container}s that should be present in cosmos.
   */
  private void verifyCosmosData(Set<Container> containers) {
    Set<ContainerDeletionEntry> entries = cloudDestination.getCosmosDataAccessor().getDeprecatedContainers(100);
    Assert.assertEquals(containers.size(), entries.size());
    Set<Short> containerIds = containers.stream().map(Container::getId).collect(Collectors.toSet());
    for (ContainerDeletionEntry entry : entries) {
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
}
