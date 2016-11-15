/*
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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

package com.github.ambry.store;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


public class StorageManagerTest {
  private static final Random RANDOM = new Random();
  private MockClusterMap clusterMap;

  /**
   * Startup the {@link MockClusterMap} for a test.
   * @throws IOException
   */
  @Before
  public void initializeCluster() throws IOException {
    clusterMap = new MockClusterMap(false, 1, 3, 3);
  }

  /**
   * Cleanup the {@link MockClusterMap} after a test.
   * @throws IOException
   */
  @After
  public void cleanupCluster() throws IOException {
    clusterMap.cleanup();
  }

  /**
   * Test that stores on a disk without a valid mount path are not started.
   * @throws Exception
   */
  @Test
  public void mountPathNotFoundTest() throws Exception {
    MockDataNodeId dataNode = clusterMap.getDataNodes().get(0);
    List<ReplicaId> replicas = clusterMap.getReplicaIds(dataNode);
    List<String> mountPaths = dataNode.getMountPaths();
    String mountPathToDelete = mountPaths.get(RANDOM.nextInt(mountPaths.size()));
    deleteDirectory(new File(mountPathToDelete));
    StorageManager storageManager = createAndStartStoreManager(replicas);
    for (ReplicaId replica : replicas) {
      if (replica.getMountPath().equals(mountPathToDelete)) {
        assertNull("This store should not be accessible.", storageManager.getStore(replica.getPartitionId()));
      } else {
        Store store = storageManager.getStore(replica.getPartitionId());
        assertTrue("Store should be started", ((BlobStore) store).isStarted());
      }
    }
    shutdownAndAssertStoresInaccessible(storageManager, replicas);
  }

  /**
   * Tests that {@link StorageManager} can start even when certain stores cannot be started. Checks that these stores
   * are not accessible. We can make the replica path non-readable to induce a store starting failure.
   * @throws Exception
   */
  @Test
  public void storeStartFailureTest() throws Exception {
    MockDataNodeId dataNode = clusterMap.getDataNodes().get(0);
    List<ReplicaId> replicas = clusterMap.getReplicaIds(dataNode);
    Set<Integer> badReplicaIndexes = new HashSet<>(Arrays.asList(2, 7));
    for (Integer badReplicaIndex : badReplicaIndexes) {
      new File(replicas.get(badReplicaIndex).getReplicaPath()).setReadable(false);
    }
    StorageManager storageManager = createAndStartStoreManager(replicas);
    for (int i = 0; i < replicas.size(); i++) {
      ReplicaId replica = replicas.get(i);
      if (badReplicaIndexes.contains(i)) {
        assertNull("This store should not be accessible.", storageManager.getStore(replica.getPartitionId()));
      } else {
        Store store = storageManager.getStore(replica.getPartitionId());
        assertTrue("Store should be started", ((BlobStore) store).isStarted());
      }
    }
    shutdownAndAssertStoresInaccessible(storageManager, replicas);
  }

  /**
   * Tests that {@link StorageManager} can start when all of the stores on one disk fail to start. Checks that these
   * stores are not accessible. We can make the replica path non-readable to induce a store starting failure.
   * @throws Exception
   */
  @Test
  public void storeStartFailureOnOneDiskTest() throws Exception {
    MockDataNodeId dataNode = clusterMap.getDataNodes().get(0);
    List<ReplicaId> replicas = clusterMap.getReplicaIds(dataNode);
    List<String> mountPaths = dataNode.getMountPaths();
    String badDiskMountPath = mountPaths.get(RANDOM.nextInt(mountPaths.size()));
    for (ReplicaId replica : replicas) {
      if (replica.getMountPath().equals(badDiskMountPath)) {
        new File(replica.getReplicaPath()).setReadable(false);
      }
    }
    StorageManager storageManager = createAndStartStoreManager(replicas);
    for (ReplicaId replica : replicas) {
      if (replica.getMountPath().equals(badDiskMountPath)) {
        assertNull("This store should not be accessible.", storageManager.getStore(replica.getPartitionId()));
      } else {
        Store store = storageManager.getStore(replica.getPartitionId());
        assertTrue("Store should be started", ((BlobStore) store).isStarted());
      }
    }
    shutdownAndAssertStoresInaccessible(storageManager, replicas);
  }

  /**
   * Test that stores for all partitions on a node have been started and partitions not present on this node are
   * inaccessible.
   * @throws Exception
   */
  @Test
  public void successfulStartTest() throws Exception {
    MockDataNodeId dataNode = clusterMap.getDataNodes().get(0);
    List<ReplicaId> replicas = clusterMap.getReplicaIds(dataNode);
    StorageManager storageManager = createAndStartStoreManager(replicas);
    for (ReplicaId replica : replicas) {
      Store store = storageManager.getStore(replica.getPartitionId());
      assertTrue("Store should be started", ((BlobStore) store).isStarted());
    }
    MockPartitionId invalidPartition = new MockPartitionId(Long.MAX_VALUE, Collections.<MockDataNodeId>emptyList(), 0);
    assertNull("Should not have found a store for an invalid partition.", storageManager.getStore(invalidPartition));
    shutdownAndAssertStoresInaccessible(storageManager, replicas);
  }

  /**
   * Create a {@link StorageManager} and start stores for the passed in set of replicas.
   * @param replicas the list of replicas for the {@link StorageManager} to use.
   * @return a started {@link StorageManager}
   * @throws StoreException
   */
  private static StorageManager createAndStartStoreManager(List<ReplicaId> replicas)
      throws StoreException, InterruptedException {
    StorageManager storageManager =
        new StorageManager(new StoreConfig(new VerifiableProperties(new Properties())), Utils.newScheduler(1, false),
            new MetricRegistry(), replicas, new MockIdFactory(), new DummyMessageStoreRecovery(),
            new DummyMessageStoreHardDelete(), SystemTime.getInstance());
    storageManager.start();
    return storageManager;
  }

  /**
   * Shutdown a {@link StorageManager} and assert that the stores cannot be accessed for the provided replicas.
   * @param storageManager the {@link StorageManager} to shutdown.
   * @param replicas the {@link ReplicaId}s to check for store inaccessibility.
   * @throws StoreException
   */
  private static void shutdownAndAssertStoresInaccessible(StorageManager storageManager, List<ReplicaId> replicas)
      throws StoreException {
    storageManager.shutdown();
    for (ReplicaId replica : replicas) {
      assertNull(storageManager.getStore(replica.getPartitionId()));
    }
  }

  /**
   * Delete a directory recursively.
   * @param file the directory to delete.
   */
  private static void deleteDirectory(File file) {
    File[] contents = file.listFiles();
    if (contents != null) {
      for (File f : contents) {
        deleteDirectory(f);
      }
    }
    file.delete();
  }
}

