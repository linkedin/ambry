/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.MockReplicaId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaType;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.store.Store;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import org.junit.Assert;
import org.junit.Test;


/**
 * Test for {@code CloudStorageManager}
 */
public class CloudStorageManagerTest {
  private final ClusterMap clusterMap;

  public CloudStorageManagerTest() throws IOException {
    clusterMap = new MockClusterMap();
  }

  /**
   * Creates a {@link CloudStorageManager} object.
   * @return {@link CloudStorageManager} object.
   * @throws IOException
   */
  private CloudStorageManager createNewCloudStorageManager() throws IOException {
    Properties props = new Properties();
    props.setProperty("clustermap.cluster.name", "dummy");
    props.setProperty("clustermap.datacenter.name", "dummy");
    props.setProperty("clustermap.host.name", "dummy");
    props.setProperty("cloud.blob.crypto.agent.factory.class",
        "com.github.ambry.cloud.TestCloudBlobCryptoAgentFactory");

    VerifiableProperties properties = new VerifiableProperties(props);
    VcrMetrics vcrMetrics = new VcrMetrics(new MetricRegistry());
    CloudDestination cloudDestination = new LatchBasedInMemoryCloudDestination(Collections.emptyList());
    MockClusterMap clusterMap = new MockClusterMap();
    return new CloudStorageManager(properties, vcrMetrics, cloudDestination, clusterMap);
  }

  /**
   * Test {@code CloudStorageManager#addBlobStore}, {@code CloudStorageManager#startBlobStore}, {@code CloudStorageManager#removeBlobStore}
   * @throws IOException
   */
  @Test
  public void addStartAndRemoveBlobStoreTest() throws IOException {
    CloudStorageManager cloudStorageManager = createNewCloudStorageManager();
    ReplicaId mockReplicaId = clusterMap.getReplicaIds(clusterMap.getDataNodeIds().get(0)).get(0);
    PartitionId partitionId = mockReplicaId.getPartitionId();

    //start store for Partitionid not added to the store
    Assert.assertFalse(cloudStorageManager.startBlobStore(partitionId));

    //remove store for Partitionid not added to the store
    Assert.assertFalse(cloudStorageManager.removeBlobStore(partitionId));

    //add a replica to the store
    Assert.assertTrue(cloudStorageManager.addBlobStore(mockReplicaId));

    //add an already added replica to the store
    Assert.assertTrue(cloudStorageManager.addBlobStore(mockReplicaId));

    //try start for the added paritition
    Assert.assertTrue(cloudStorageManager.startBlobStore(partitionId));

    //try remove for an added partition
    Assert.assertTrue(cloudStorageManager.removeBlobStore(partitionId));
  }

  /**
   * Test {@code CloudStorageManager#shutdownBlobStore}
   * @throws IOException
   */
  @Test
  public void shutdownBlobStoreTest() throws IOException {
    CloudStorageManager cloudStorageManager = createNewCloudStorageManager();
    ReplicaId mockReplicaId = clusterMap.getReplicaIds(clusterMap.getDataNodeIds().get(0)).get(0);
    PartitionId partitionId = mockReplicaId.getPartitionId();

    //try shutdown for a store of partition that was never added
    Assert.assertFalse(cloudStorageManager.shutdownBlobStore(partitionId));

    //add and start a replica to the store
    Assert.assertTrue(cloudStorageManager.addBlobStore(mockReplicaId));

    //try shutdown for a store of partition that was added
    Assert.assertTrue(cloudStorageManager.shutdownBlobStore(partitionId));

    //try shutdown for a removed partition
    Assert.assertTrue(cloudStorageManager.removeBlobStore(partitionId));
    Assert.assertFalse(cloudStorageManager.shutdownBlobStore(partitionId));
  }

  /**
   * Test {@code CloudStorageManager#getStore}
   * @throws IOException
   */
  @Test
  public void getStoreTest() throws IOException {
    CloudStorageManager cloudStorageManager = createNewCloudStorageManager();
    ReplicaId mockReplicaId = clusterMap.getReplicaIds(clusterMap.getDataNodeIds().get(0)).get(0);
    PartitionId partitionId = mockReplicaId.getPartitionId();

    //try get for a partition that doesn't exist
    Store store = cloudStorageManager.getStore(partitionId);
    Assert.assertNotNull(store);
    Assert.assertTrue(store.isStarted());

    //add and start a replica to the store
    Assert.assertTrue(cloudStorageManager.addBlobStore(mockReplicaId));

    //try get for an added replica
    store = cloudStorageManager.getStore(partitionId);
    Assert.assertNotNull(store);
    Assert.assertTrue(store.isStarted());

    //try get for a removed replica
    Assert.assertTrue(cloudStorageManager.removeBlobStore(partitionId));
    store = cloudStorageManager.getStore(partitionId);
    Assert.assertNotNull(store);
    Assert.assertTrue(store.isStarted());
  }

  /**
   * Test {@code CloudStorageManager#checkLocalPartitionStatus}
   * @throws IOException
   */
  @Test
  public void checkLocalPartitionStatusTest() throws IOException {
    CloudStorageManager cloudStorageManager = createNewCloudStorageManager();
    ReplicaId mockReplicaId = clusterMap.getReplicaIds(clusterMap.getDataNodeIds().get(0)).get(0);
    PartitionId partitionId = mockReplicaId.getPartitionId();

    //try checkLocalPartitionStatus for a partition that doesn't exist
    Assert.assertEquals(
        cloudStorageManager.checkLocalPartitionStatus(partitionId, new MockReplicaId(ReplicaType.DISK_BACKED)),
        ServerErrorCode.No_Error);

    //add and start a replica to the store
    Assert.assertTrue(cloudStorageManager.addBlobStore(mockReplicaId));

    //try checkLocalPartitionStatus for an added replica
    Assert.assertEquals(
        cloudStorageManager.checkLocalPartitionStatus(partitionId, new MockReplicaId(ReplicaType.DISK_BACKED)),
        ServerErrorCode.No_Error);

    //stop a replica on the store
    Assert.assertTrue("Failed in stopping replica", cloudStorageManager.shutdownBlobStore(partitionId));

    //try checkLocalPartitionStatus for a stopped replica (stopped blob store)
    Assert.assertEquals(cloudStorageManager.checkLocalPartitionStatus(partitionId, mockReplicaId),
        ServerErrorCode.No_Error);

    //try checkLocalPartitionStatus for a removed replica
    Assert.assertTrue(cloudStorageManager.removeBlobStore(partitionId));
    Assert.assertEquals(
        cloudStorageManager.checkLocalPartitionStatus(partitionId, new MockReplicaId(ReplicaType.DISK_BACKED)),
        ServerErrorCode.No_Error);
  }

  /**
   * Test {@code CloudStorageManager#scheduleNextForCompaction}
   */
  @Test
  public void scheduleNextForCompactionTest() throws IOException {
    CloudStorageManager cloudStorageManager = createNewCloudStorageManager();
    try {
      cloudStorageManager.scheduleNextForCompaction(new MockPartitionId());
      Assert.fail("CloudStorageManager scheduleNextForCompaction should throw unimplemented exception");
    } catch (UnsupportedOperationException e) {
    }
  }

  /**
   * Test {@code CloudStorageManager#controlCompactionForBlobStore}
   */
  @Test
  public void controlCompactionForBlobStoreTest() throws IOException {
    CloudStorageManager cloudStorageManager = createNewCloudStorageManager();
    try {
      cloudStorageManager.controlCompactionForBlobStore(new MockPartitionId(), true);
      Assert.fail("CloudStorageManager controlCompactionForBlobStore should throw unimplemented exception");
    } catch (UnsupportedOperationException e) {
    }
    try {
      cloudStorageManager.controlCompactionForBlobStore(new MockPartitionId(), false);
      Assert.fail("CloudStorageManager controlCompactionForBlobStore should throw unimplemented exception");
    } catch (UnsupportedOperationException e) {
    }
  }

  /**
   * Test {@code CloudStorageManager#setBlobStoreStoppedState}
   */
  @Test
  public void setBlobStoreStoppedState() throws IOException {
    CloudStorageManager cloudStorageManager = createNewCloudStorageManager();
    try {
      cloudStorageManager.setBlobStoreStoppedState(Collections.emptyList(), true);
      Assert.fail("CloudStorageManager setBlobStoreStoppedState should throw unimplemented exception");
    } catch (UnsupportedOperationException e) {
    }
    try {
      cloudStorageManager.setBlobStoreStoppedState(Collections.emptyList(), false);
      Assert.fail("CloudStorageManager setBlobStoreStoppedState should throw unimplemented exception");
    } catch (UnsupportedOperationException e) {
    }
  }
}
