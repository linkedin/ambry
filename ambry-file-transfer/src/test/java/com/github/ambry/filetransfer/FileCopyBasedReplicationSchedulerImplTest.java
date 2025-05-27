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

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaSyncUpManager;
import com.github.ambry.config.FileCopyBasedReplicationConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.filetransfer.handler.FileCopyHandler;
import com.github.ambry.filetransfer.handler.FileCopyHandlerFactory;
import com.github.ambry.replica.prioritization.PrioritizationManager;
import com.github.ambry.server.StoreManager;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class FileCopyBasedReplicationSchedulerImplTest {
  private FileCopyBasedReplicationSchedulerImpl scheduler;
  private FileCopyHandlerFactory mockHandlerFactory;
  private FileCopyBasedReplicationConfig mockConfig;
  private ClusterMap mockClusterMap;
  private PrioritizationManager mockPrioritizationManager;
  private ReplicaSyncUpManager mockReplicaSyncUpManager;
  private StoreManager mockStoreManager;
  private StoreConfig mockStoreConfig;
  private DataNodeId mockDataNodeId;
  private FileCopyHandler mockFileCopyHandler;
  private DiskId mockDiskId;
  private PartitionId mockPartitionId;
  private ReplicaId mockReplicaId;

  @Before
  public void setUp() {
    mockHandlerFactory = mock(FileCopyHandlerFactory.class);
    mockFileCopyHandler = mock(FileCopyHandler.class);
    mockClusterMap = mock(ClusterMap.class);
    mockPrioritizationManager = mock(PrioritizationManager.class);
    mockReplicaSyncUpManager = mock(ReplicaSyncUpManager.class);
    mockStoreManager = mock(StoreManager.class);
    mockDataNodeId = mock(DataNodeId.class);

    Properties properties = new Properties();
    properties.setProperty("filecopy.number.of.file.copy.threads", "2");
    properties.setProperty("filecopy.scheduler.wait.time.secs", "1");
    properties.setProperty("filecopy.replica.timeout.secs", "10");
    properties.setProperty("filecopy.parallel.partition.hydration.count.per.disk", "1");
    properties.put("store.file.copy.in.progress.file.name", "fileCopyInProgress");
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    mockStoreConfig = new StoreConfig(verifiableProperties);
    mockConfig  = new FileCopyBasedReplicationConfig(verifiableProperties);

    mockDiskId = mock(DiskId.class);
    mockPartitionId = mock(PartitionId.class);
    mockReplicaId = mock(ReplicaId.class);

    when(mockDataNodeId.getDiskIds()).thenReturn(Collections.singletonList(mockDiskId));
    when(mockReplicaId.getDiskId()).thenReturn(mockDiskId);
    when(mockReplicaId.getPartitionId()).thenReturn(mockPartitionId);
    when(mockPartitionId.toPathString()).thenReturn("mockPartition");
    when(mockPrioritizationManager.getPartitionListForDisk(mockDiskId, 1))
        .thenReturn(Collections.singletonList(mockReplicaId));
    when(mockHandlerFactory.getFileCopyHandler()).thenReturn(mockFileCopyHandler);

    scheduler = new FileCopyBasedReplicationSchedulerImpl(
        mockHandlerFactory,
        mockConfig,
        mockClusterMap,
        mockPrioritizationManager,
        mockReplicaSyncUpManager,
        mockStoreManager,
        mockStoreConfig,
        mockDataNodeId
    );
  }

  @Test
  public void testInitializationAndShutdown() throws InterruptedException {
    Thread thread = new Thread(scheduler);
    thread.start();
    Thread.sleep(20); // Allow some time for the scheduler to start
    assertTrue("Scheduler should be running", scheduler.isRunning());

    scheduler.shutdown();
    Thread.sleep(20); // Allow some time for the scheduler to shut down
    assertFalse("Scheduler should not be running after shutdown", scheduler.isRunning());
  }


  @Test
  public void testFileCopySuccessHandling() {
    FileCopyBasedReplicationSchedulerImpl.FileCopyStatusListenerImpl listener =
        scheduler.new FileCopyStatusListenerImpl(mockReplicaSyncUpManager, mockReplicaId);

    // Simulate success
    listener.onFileCopySuccess();

    // Verify interactions
    verify(mockReplicaSyncUpManager, times(1)).onFileCopyComplete(mockReplicaId);
    assertFalse("Replica should be removed from in-flight replicas", scheduler.getInFlightReplicas().contains(mockReplicaId));
  }

  @Test
  public void testFileCopyFailureHandling() {

    FileCopyBasedReplicationSchedulerImpl.FileCopyStatusListenerImpl listener =
        scheduler.new FileCopyStatusListenerImpl(mockReplicaSyncUpManager, mockReplicaId);

    // Simulate failure
    Exception testException = new Exception("Test Exception");
    listener.onFileCopyFailure(testException);

    // Verify interactions
    verify(mockReplicaSyncUpManager, times(1)).onFileCopyError(mockReplicaId);
    assertFalse("Replica should be removed from in-flight replicas", scheduler.getInFlightReplicas().contains(mockReplicaId));
  }

  @Test
  public void testStarvedReplicaHandling() {
    when(mockReplicaId.getPartitionId()).thenReturn(mock(PartitionId.class));
    when(mockReplicaId.getDiskId()).thenReturn(mock(DiskId.class));

    scheduler.getReplicaToStartTimeMap().put(mockReplicaId, System.currentTimeMillis() / 1000 - 20);

    List<ReplicaId> starvedReplicas = scheduler.findStarvedReplicas();
    assertEquals("One replica should be starved", 1, starvedReplicas.size());
    assertEquals("Starved replica should match", mockReplicaId, starvedReplicas.get(0));
  }

  @Test
  public void testErrorHandlingDuringFileCopy() {
    FileCopyHandler mockHandler = mock(FileCopyHandler.class);

    when(mockReplicaId.getPartitionId()).thenReturn(mockPartitionId);
    when(mockPartitionId.toPathString()).thenReturn("mockPartition");
    when(mockReplicaId.getReplicaPath()).thenReturn("/mock/mount/path");
    FileCopyBasedReplicationSchedulerImpl.FileCopyStatusListenerImpl listener =
        scheduler.new FileCopyStatusListenerImpl(mockReplicaSyncUpManager, mockReplicaId);

    try {
      doThrow(new RuntimeException("Test Exception")).when(mockHandler).copy(null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    listener.onFileCopyFailure(new Exception("Test Exception"));
    verify(mockReplicaSyncUpManager, times(1)).onFileCopyError(mockReplicaId);
  }

}
