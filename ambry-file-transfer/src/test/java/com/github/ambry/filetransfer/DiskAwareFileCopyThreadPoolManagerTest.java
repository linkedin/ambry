/**
 * Copyright 2025 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.filetransfer;

import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.filecopy.MockFileCopyHandlerFactory;
import com.github.ambry.filetransfer.handler.FileCopyHandler;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

import static org.junit.Assert.*;

public class DiskAwareFileCopyThreadPoolManagerTest {

  /**
   * Tests basic thread submission and scheduling
   */
  @Test
  public void testBasicThreadSubmission() throws InterruptedException {
    // Setup test environment
    List<String> mountPaths = new ArrayList<>();
    mountPaths.add("/tmp/1");
    mountPaths.add("/tmp/2");
    Port port = new Port(1, PortType.PLAINTEXT);
    MockDataNodeId dataNodeId = new MockDataNodeId("peerNode", Collections.singletonList(port), mountPaths, "peerDatacenter");

    // Create thread pool manager with 2 threads per disk
    DiskAwareFileCopyThreadPoolManager threadPoolManager = new DiskAwareFileCopyThreadPoolManager(dataNodeId.getDiskIds(), 2);
    Thread threadPoolManagerThread = new Thread(threadPoolManager);
    threadPoolManagerThread.start();

    // Create test tracking map
    final Map<String, Integer> successFailCount = new HashMap<>();
    successFailCount.put("success", 0);
    successFailCount.put("fail", 0);

    // Submit threads for each disk
    for (DiskId diskId : dataNodeId.getDiskIds()) {
      PartitionId partitionId = new MockPartitionId(100, MockClusterMap.DEFAULT_PARTITION_CLASS,
          Collections.singletonList(dataNodeId), 0);
      ReplicaId replicaId = partitionId.getReplicaIds().get(0);

      // Submit max threads per disk
      for (int i = 0; i < 2; i++) {
        FileCopyStatusListener listener = new FileCopyStatusListener() {
          @Override
          public void onFileCopySuccess() {
            successFailCount.put("success", successFailCount.get("success") + 1);
          }

          @Override
          public ReplicaId getReplicaId() {
            return null;
          }

          @Override
          public void onFileCopyFailure(Exception e) {
            successFailCount.put("fail", successFailCount.get("fail") + 1);
          }
        };

        FileCopyHandler handler = new MockFileCopyHandlerFactory().getFileCopyHandler();
        threadPoolManager.submitReplicaForHydration(replicaId, listener, handler);
      }
    }

    // Wait for threads to complete
    Thread.sleep(1000);

    // Verify results
    assertEquals("All copy operations should succeed", 4, successFailCount.get("success").intValue());
    assertEquals("No operations should fail", 0, successFailCount.get("fail").intValue());

    // Cleanup
    threadPoolManager.shutdown();
    threadPoolManagerThread.join();
  }

  /**
   * Tests that we cannot submit more threads than the per-disk limit
   */
  @Test
  public void testThreadLimitPerDisk() {
    List<String> mountPaths = new ArrayList<>();
    mountPaths.add("/tmp/1");
    Port port = new Port(1, PortType.PLAINTEXT);
    MockDataNodeId dataNodeId = new MockDataNodeId("peerNode", Collections.singletonList(port), mountPaths, "peerDatacenter");

    int threadsPerDisk = 2;
    DiskAwareFileCopyThreadPoolManager threadPoolManager = new DiskAwareFileCopyThreadPoolManager(dataNodeId.getDiskIds(), threadsPerDisk);

    // Get available disks for hydration before submitting any threads
    List<DiskId> availableDisks = threadPoolManager.getDiskIdsToHydrate();
    assertEquals("Should have all disks available initially", dataNodeId.getDiskIds().size(), availableDisks.size());

    // Submit max threads for the disk
    DiskId diskId = dataNodeId.getDiskIds().get(0);
    PartitionId partitionId = new MockPartitionId(100, MockClusterMap.DEFAULT_PARTITION_CLASS,
        Collections.singletonList(dataNodeId), 0);
    ReplicaId replicaId = partitionId.getReplicaIds().get(0);

    for (int i = 0; i < threadsPerDisk; i++) {
      threadPoolManager.submitReplicaForHydration(replicaId,
          new MockFileCopyStatusListener(),
          new MockFileCopyHandlerFactory().getFileCopyHandler());
    }

    // Verify no more disks are available for hydration
    availableDisks = threadPoolManager.getDiskIdsToHydrate();
    assertEquals("Should have no disks available after reaching thread limit", 0, availableDisks.size());
  }

  /**
   * Helper class for testing
   */
  private static class MockFileCopyStatusListener implements FileCopyStatusListener {
    @Override
    public void onFileCopySuccess() {
    }

    @Override
    public ReplicaId getReplicaId() {
      return null;
    }

    @Override
    public void onFileCopyFailure(Exception e) {
    }
  }
}
