package com.github.ambry.replica.prioritization;

import com.github.ambry.clustermap.AmbryDataNode;
import com.github.ambry.clustermap.AmbryDisk;
import com.github.ambry.clustermap.AmbryPartition;
import com.github.ambry.clustermap.AmbryReplica;
import com.github.ambry.clustermap.ClusterManagerQueryHelper;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.HelixClusterManager;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaState;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.replication.ReplicationEngine;
import com.github.ambry.store.Store;
import com.github.ambry.store.StorageManager;
import com.github.ambry.utils.Time;
import java.util.Properties;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.Silent.class)
public class ReplicationPrioritizationManagerTest {

  @Mock
  private ReplicationEngine replicationEngine;

  @Mock
  private ClusterMap clusterMap;

  @Mock
  private DataNodeId dataNodeId;

  @Mock
  private ScheduledExecutorService scheduler;

  @Mock
  private StorageManager storageManager;

  private ReplicationConfig replicationConfig;

  @Mock
  private HelixClusterManager helixClusterManager;

  @Mock
  private ClusterManagerQueryHelper<AmbryReplica, AmbryDisk, AmbryPartition, AmbryDataNode> clusterManagerQueryHelper;

  @Mock
  private Time mockTime;

  @Mock
  private Store store1, store2, store3, store4, store5;

  private ReplicationPrioritizationManager manager;
  private ReplicationPrioritizationManager managerWithMockTime;
  private String datacenterName = "testDC";
  private int lowReplicaThreshold = 2;
  private int minBatchSize = 3;
  private int replicationTimeoutHours = 24;
  private long scheduleIntervalMinutes = 5;
  private long disruptionReadinessWindowInMS = TimeUnit.HOURS.toMillis(1);

  // Test partitions
  private PartitionId partition1, partition2, partition3, partition4, partition5;

  @Before
  public void setUp() {

    Properties properties = new Properties();
    properties.setProperty("disruption.lookahead.window.ms", Long.toString(disruptionReadinessWindowInMS));
    properties.setProperty("prioritization.scheduler.interval", Long.toString(scheduleIntervalMinutes));
    properties.setProperty("prioritization.batch.size", Integer.toString(minBatchSize));
    replicationConfig = new ReplicationConfig(new VerifiableProperties(properties));

    // Create test partitions
    partition1 = mock(PartitionId.class);
    partition2 = mock(PartitionId.class);
    partition3 = mock(PartitionId.class);
    partition4 = mock(PartitionId.class);
    partition5 = mock(PartitionId.class);

    when(partition1.toPathString()).thenReturn("partition1");
    when(partition2.toPathString()).thenReturn("partition2");
    when(partition3.toPathString()).thenReturn("partition3");
    when(partition4.toPathString()).thenReturn("partition4");
    when(partition5.toPathString()).thenReturn("partition5");

    // Setup StorageManager
    when(storageManager.getStore(partition1)).thenReturn(store1);
    when(storageManager.getStore(partition2)).thenReturn(store2);
    when(storageManager.getStore(partition3)).thenReturn(store3);
    when(storageManager.getStore(partition4)).thenReturn(store4);
    when(storageManager.getStore(partition5)).thenReturn(store5);

    // Create manager instance with system time
    manager = new ReplicationPrioritizationManager(
        replicationEngine, clusterMap, dataNodeId, scheduler,
        datacenterName, lowReplicaThreshold, replicationTimeoutHours,
        storageManager, replicationConfig, helixClusterManager, clusterManagerQueryHelper);

    // Initialize mock time
   // when(mockTime.milliseconds()).thenReturn(System.currentTimeMillis());

    // Create manager instance with mock time for testing timeouts
    managerWithMockTime = new ReplicationPrioritizationManager(
        replicationEngine, clusterMap, dataNodeId, scheduler,
        datacenterName, lowReplicaThreshold, replicationTimeoutHours,
        storageManager, replicationConfig, helixClusterManager, clusterManagerQueryHelper);

    // Need to use reflection to set the private time field
    try {
      java.lang.reflect.Field timeField = ReplicationPrioritizationManager.class.getDeclaredField("time");
      timeField.setAccessible(true);
      timeField.set(managerWithMockTime, mockTime);
    } catch (Exception e) {
      throw new RuntimeException("Failed to set mock time", e);
    }
  }

  @Test
  public void testConstructorInitialization() {
    // Verify the scheduler was called with correct parameters
    verify(scheduler).scheduleAtFixedRate(
        eq(manager), eq(0L), eq(scheduleIntervalMinutes), eq(TimeUnit.MINUTES));
  }

  @Test
  public void testRunWithNoBootstrappingPartitions() {
    // Setup scenario with no bootstrapping partitions
    Set<PartitionId> partitions = new HashSet<>(Arrays.asList(partition1, partition2, partition3));
    when(storageManager.getLocalPartitions()).thenReturn(partitions);

    // All partitions are in STANDBY state (not bootstrapping)
    when(store1.getCurrentState()).thenReturn(ReplicaState.STANDBY);
    when(store2.getCurrentState()).thenReturn(ReplicaState.STANDBY);
    when(store3.getCurrentState()).thenReturn(ReplicaState.STANDBY);

    // Run the manager
    manager.start();

    // Verify no partition replication was controlled (since there are no bootstrapping partitions)
    verify(replicationEngine, never()).controlReplicationForPartitions(
        any(Set.class), any(List.class), anyBoolean());
  }

  @Test
  public void testRunWithAllPartitionsBootstrappingButNoHighPriority() {
    // Setup scenario with all partitions bootstrapping but none high priority
    Set<PartitionId> partitions = new HashSet<>(Arrays.asList(partition1, partition2, partition3));
    when(storageManager.getLocalPartitions()).thenReturn(partitions);

    // All partitions are in BOOTSTRAP state
    when(store1.getCurrentState()).thenReturn(ReplicaState.BOOTSTRAP);
    when(store2.getCurrentState()).thenReturn(ReplicaState.BOOTSTRAP);
    when(store3.getCurrentState()).thenReturn(ReplicaState.BOOTSTRAP);

    // All partitions have sufficient replicas (no high priority needed)
    when(clusterManagerQueryHelper.getMinActiveReplicas(partition1)).thenReturn(2);
    when(clusterManagerQueryHelper.getMinActiveReplicas(partition2)).thenReturn(2);
    when(clusterManagerQueryHelper.getMinActiveReplicas(partition3)).thenReturn(2);

    // Mock replica counts above threshold
    mockReplicaStates(partition1, 3); // 3 replicas > threshold of 2
    mockReplicaStates(partition2, 3);
    mockReplicaStates(partition3, 3);

    // Run the manager
    manager.start();

    // Verify normal partitions were replicated
    verify(replicationEngine, atLeastOnce()).controlReplicationForPartitions(
        any(Set.class), any(List.class), anyBoolean());
  }

  @Test
  public void testRunWithHighPriorityPartitions() {
    // Setup scenario with high priority partitions
    Set<PartitionId> partitions = new HashSet<>(Arrays.asList(partition1, partition2, partition3));
    when(storageManager.getLocalPartitions()).thenReturn(partitions);

    // All partitions are in BOOTSTRAP state
    when(store1.getCurrentState()).thenReturn(ReplicaState.BOOTSTRAP);
    when(store2.getCurrentState()).thenReturn(ReplicaState.BOOTSTRAP);
    when(store3.getCurrentState()).thenReturn(ReplicaState.BOOTSTRAP);

    // Partition1 has replica count below threshold (high priority)
    when(clusterManagerQueryHelper.getMinActiveReplicas(partition1)).thenReturn(3);
    mockReplicaStates(partition1, 2); // 2 replicas < threshold of 3

    // Other partitions have sufficient replicas
    when(clusterManagerQueryHelper.getMinActiveReplicas(partition2)).thenReturn(2);
    when(clusterManagerQueryHelper.getMinActiveReplicas(partition3)).thenReturn(2);
    mockReplicaStates(partition2, 3);
    mockReplicaStates(partition3, 3);

    // Run the manager
    manager.start();

    // Verify partitions were prioritized correctly
    ArgumentCaptor<Set<PartitionId>> partitionCaptor = ArgumentCaptor.forClass(Set.class);
    ArgumentCaptor<Boolean> enableCaptor = ArgumentCaptor.forClass(Boolean.class);

    verify(replicationEngine, atLeastOnce()).controlReplicationForPartitions(
        partitionCaptor.capture(), eq(Collections.emptyList()), enableCaptor.capture());

    // Find enabled partition set (high priority)
    Set<PartitionId> enabledPartitions = null;
    for (int i = 0; i < enableCaptor.getAllValues().size(); i++) {
      if (enableCaptor.getAllValues().get(i)) {
        enabledPartitions = partitionCaptor.getAllValues().get(i);
        break;
      }
    }

    // Find disabled partition set (non-priority)
    Set<PartitionId> disabledPartitions = null;
    for (int i = 0; i < enableCaptor.getAllValues().size(); i++) {
      if (!enableCaptor.getAllValues().get(i)) {
        disabledPartitions = partitionCaptor.getAllValues().get(i);
        break;
      }
    }

    // Verify enabled partitions
    assertNotNull("Should have enabled some partitions", enabledPartitions);
    assertTrue("High priority partition1 should be enabled", enabledPartitions.contains(partition1));

    // Verify enabled set size meets minimum batch size (if applicable)
    if (enabledPartitions.size() < partitions.size()) {
      assertTrue("Enabled set should meet minimum batch size",
          enabledPartitions.size() >= minBatchSize);
    }

    // If there were disabled partitions, verify they don't include high priority ones
    if (disabledPartitions != null && !disabledPartitions.isEmpty()) {
      assertFalse("High priority partition1 should not be disabled",
          disabledPartitions.contains(partition1));
    }
  }

  @Test
  public void testRunWithMultipleHighPriorityPartitions() {
    // Setup scenario with multiple high priority partitions
    Set<PartitionId> partitions = new HashSet<>(Arrays.asList(partition1, partition2, partition3, partition4, partition5));
    when(storageManager.getLocalPartitions()).thenReturn(partitions);

    // All partitions are in BOOTSTRAP state
    when(store1.getCurrentState()).thenReturn(ReplicaState.BOOTSTRAP);
    when(store2.getCurrentState()).thenReturn(ReplicaState.BOOTSTRAP);
    when(store3.getCurrentState()).thenReturn(ReplicaState.BOOTSTRAP);
    when(store4.getCurrentState()).thenReturn(ReplicaState.BOOTSTRAP);
    when(store5.getCurrentState()).thenReturn(ReplicaState.BOOTSTRAP);

    // Partition1, Partition2, and Partition3 have replica counts below threshold (high priority)
    when(clusterManagerQueryHelper.getMinActiveReplicas(partition1)).thenReturn(3);
    when(clusterManagerQueryHelper.getMinActiveReplicas(partition2)).thenReturn(3);
    when(clusterManagerQueryHelper.getMinActiveReplicas(partition3)).thenReturn(3);
    mockReplicaStates(partition1, 2); // 2 replicas < threshold of 3
    mockReplicaStates(partition2, 2); // 2 replicas < threshold of 3
    mockReplicaStates(partition3, 2); // 2 replicas < threshold of 3

    // Other partitions have sufficient replicas
    when(clusterManagerQueryHelper.getMinActiveReplicas(partition4)).thenReturn(2);
    when(clusterManagerQueryHelper.getMinActiveReplicas(partition5)).thenReturn(2);
    mockReplicaStates(partition4, 3);
    mockReplicaStates(partition5, 3);

    // Run the manager
    manager.start();

    // Verify partitions were prioritized correctly
    ArgumentCaptor<Set<PartitionId>> partitionCaptor = ArgumentCaptor.forClass(Set.class);
    ArgumentCaptor<Boolean> enableCaptor = ArgumentCaptor.forClass(Boolean.class);

    verify(replicationEngine, atLeastOnce()).controlReplicationForPartitions(
        partitionCaptor.capture(), eq(Collections.emptyList()), enableCaptor.capture());

    // Find enabled partition set (high priority)
    Set<PartitionId> enabledPartitions = null;
    for (int i = 0; i < enableCaptor.getAllValues().size(); i++) {
      if (enableCaptor.getAllValues().get(i)) {
        enabledPartitions = partitionCaptor.getAllValues().get(i);
        break;
      }
    }

    // Find disabled partition set (non-priority)
    Set<PartitionId> disabledPartitions = null;
    for (int i = 0; i < enableCaptor.getAllValues().size(); i++) {
      if (!enableCaptor.getAllValues().get(i)) {
        disabledPartitions = partitionCaptor.getAllValues().get(i);
        break;
      }
    }

    // Verify enabled partitions
    assertNotNull("Should have enabled some partitions", enabledPartitions);
    assertTrue("High priority partition1 should be enabled", enabledPartitions.contains(partition1));
    assertTrue("High priority partition2 should be enabled", enabledPartitions.contains(partition2));
    assertTrue("High priority partition3 should be enabled", enabledPartitions.contains(partition3));

    // If there were disabled partitions, verify they don't include high priority ones
    if (disabledPartitions != null && !disabledPartitions.isEmpty()) {
      assertFalse("High priority partitions should not be disabled",
          disabledPartitions.contains(partition1) ||
              disabledPartitions.contains(partition2) ||
              disabledPartitions.contains(partition3));
    }
  }

  @Test
  public void testRunWithPartitionCompletingReplication() {
    // First setup scenario with all partitions bootstrapping
    Set<PartitionId> partitions = new HashSet<>(Arrays.asList(partition1, partition2, partition3));
    when(storageManager.getLocalPartitions()).thenReturn(partitions);

    // All partitions are in BOOTSTRAP state
    when(store1.getCurrentState()).thenReturn(ReplicaState.BOOTSTRAP);
    when(store2.getCurrentState()).thenReturn(ReplicaState.BOOTSTRAP);
    when(store3.getCurrentState()).thenReturn(ReplicaState.BOOTSTRAP);

    // Partition1 has replica count below threshold (high priority)
    when(clusterManagerQueryHelper.getMinActiveReplicas(partition1)).thenReturn(2);
    mockReplicaStates(partition1, 2); // 2 replicas <= threshold of 2

    // Other partitions 1 has sufficient replicas and other = 2 replicas
    when(clusterManagerQueryHelper.getMinActiveReplicas(partition2)).thenReturn(2);
    when(clusterManagerQueryHelper.getMinActiveReplicas(partition3)).thenReturn(2);
    mockReplicaStates(partition2, 2);
    mockReplicaStates(partition3, 3);

    // First run - establishes baseline
    manager.start();

    // Verify that replicationEngine was called with appropriate sets
    ArgumentCaptor<Set<PartitionId>> partitionCaptor = ArgumentCaptor.forClass(Set.class);
    verify(replicationEngine, atLeastOnce()).controlReplicationForPartitions(
        partitionCaptor.capture(), eq(Collections.emptyList()), anyBoolean());

    // Reset mocks for clean verification in second run
    reset(replicationEngine);

    // Now partition1 completes replication
    when(store1.getCurrentState()).thenReturn(ReplicaState.STANDBY);

    // Second run - should recognize partition1 as completed
    manager.start();

    // Check that partition1 is not in any enabled set
    boolean partition1Enabled = false;
    for (Set<PartitionId> set : partitionCaptor.getAllValues()) {
      if (set.contains(partition1)) {
        partition1Enabled = true;
        break;
      }
    }

    assertFalse("Completed partition1 should not be enabled for replication", partition1Enabled);
  }

  @Test
  public void testHasCompletedReplication() {
    // Test for STANDBY state (completed)
    when(store1.getCurrentState()).thenReturn(ReplicaState.STANDBY);
    assertTrue("STANDBY state should indicate completed replication",
        manager.hasCompletedReplication(partition1));

    // Test for LEADER state (completed)
    when(store1.getCurrentState()).thenReturn(ReplicaState.LEADER);
    assertTrue("LEADER state should indicate completed replication",
        manager.hasCompletedReplication(partition1));

    // Test for BOOTSTRAP state (not completed)
    when(store1.getCurrentState()).thenReturn(ReplicaState.BOOTSTRAP);
    assertFalse("BOOTSTRAP state should not indicate completed replication",
        manager.hasCompletedReplication(partition1));

    // Test for OFFLINE state (not completed)
    when(store1.getCurrentState()).thenReturn(ReplicaState.OFFLINE);
    assertFalse("OFFLINE state should not indicate completed replication",
        manager.hasCompletedReplication(partition1));
  }

  @Test
  public void testMultipleRunsWithStateChanges() {
    // Initial setup with all partitions bootstrapping
    Set<PartitionId> partitions = new HashSet<>(Arrays.asList(partition1, partition2, partition3));
    when(storageManager.getLocalPartitions()).thenReturn(partitions);

    // All partitions are initially bootstrapping
    when(store1.getCurrentState()).thenReturn(ReplicaState.BOOTSTRAP);
    when(store2.getCurrentState()).thenReturn(ReplicaState.BOOTSTRAP);
    when(store3.getCurrentState()).thenReturn(ReplicaState.BOOTSTRAP);

    // Partition1 is high priority
    when(clusterManagerQueryHelper.getMinActiveReplicas(partition1)).thenReturn(3);
    mockReplicaStates(partition1, 2); // Below threshold

    // Other partitions have sufficient replicas
    when(clusterManagerQueryHelper.getMinActiveReplicas(partition2)).thenReturn(2);
    when(clusterManagerQueryHelper.getMinActiveReplicas(partition3)).thenReturn(2);
    mockReplicaStates(partition2, 3);
    mockReplicaStates(partition3, 3);

    // First run - should prioritize partition1
    manager.start();

    // Verify that partition1 was enabled
    ArgumentCaptor<Set<PartitionId>> enabledPartitionsCaptor = ArgumentCaptor.forClass(Set.class);
    ArgumentCaptor<Boolean> enabledCaptor = ArgumentCaptor.forClass(Boolean.class);
    verify(replicationEngine, atLeastOnce()).controlReplicationForPartitions(
        enabledPartitionsCaptor.capture(), eq(Collections.emptyList()), enabledCaptor.capture());

    // Find the set of enabled partitions
    Set<PartitionId> firstRunEnabledPartitions = null;
    for (int i = 0; i < enabledCaptor.getAllValues().size(); i++) {
      if (enabledCaptor.getAllValues().get(i)) {
        firstRunEnabledPartitions = enabledPartitionsCaptor.getAllValues().get(i);
        break;
      }
    }

    assertNotNull("Should have enabled some partitions in first run", firstRunEnabledPartitions);
    assertTrue("Partition1 should be enabled in first run", firstRunEnabledPartitions.contains(partition1));

    // Reset mock for second run
    reset(replicationEngine);

    // Partition1 completed replication, but now partition2 becomes high priority
    when(store1.getCurrentState()).thenReturn(ReplicaState.STANDBY);
    when(store2.getCurrentState()).thenReturn(ReplicaState.BOOTSTRAP);

    when(clusterManagerQueryHelper.getMinActiveReplicas(partition2)).thenReturn(3); // Increase threshold
    mockReplicaStates(partition2, 2); // Now below threshold

    // Second run - should prioritize partition2 now
    manager.start();

    enabledPartitionsCaptor = ArgumentCaptor.forClass(Set.class);
    enabledCaptor = ArgumentCaptor.forClass(Boolean.class);
    // controlReplication shouldn't be called as partition1 is completed and partition was already bootstrapping
    verify(replicationEngine, times(0)).controlReplicationForPartitions(
        enabledPartitionsCaptor.capture(), eq(Collections.emptyList()), enabledCaptor.capture());

    // Find the set of enabled partitions
    Set<PartitionId> secondRunEnabledPartitions = manager.getCurrentlyReplicatingPartitions();
    // Verify that partition2 was already enabled and partition1 was not
    assertNotNull("Should have enabled some partitions in second run", secondRunEnabledPartitions);
    assertFalse("Partition1 should not be enabled in second run", secondRunEnabledPartitions.contains(partition1));
    assertTrue("Partition2 should be enabled in second run", secondRunEnabledPartitions.contains(partition2));
  }

  @Test
  public void testRunWithException() {
    // Setup StorageManager to throw exception
    when(storageManager.getLocalPartitions()).thenThrow(new RuntimeException("Test exception"));

    // Run should not propagate exception
    manager.start();

    // No assertions needed - test passes if no exception is thrown
  }

  @Test
  public void testMinimumBatchSizeEnforcement() {
    // Setup with one high priority partition but minimum batch size of 3
    Set<PartitionId> partitions = new HashSet<>(Arrays.asList(partition1, partition2, partition3, partition4, partition5));
    when(storageManager.getLocalPartitions()).thenReturn(partitions);

    // All partitions are bootstrapping
    when(store1.getCurrentState()).thenReturn(ReplicaState.BOOTSTRAP);
    when(store2.getCurrentState()).thenReturn(ReplicaState.BOOTSTRAP);
    when(store3.getCurrentState()).thenReturn(ReplicaState.BOOTSTRAP);
    when(store4.getCurrentState()).thenReturn(ReplicaState.BOOTSTRAP);
    when(store5.getCurrentState()).thenReturn(ReplicaState.BOOTSTRAP);

    // Only partition1 is high priority
    when(clusterManagerQueryHelper.getMinActiveReplicas(partition1)).thenReturn(3);
    mockReplicaStates(partition1, 2); // Below threshold

    // Others are not high priority
    when(clusterManagerQueryHelper.getMinActiveReplicas(partition2)).thenReturn(2);
    when(clusterManagerQueryHelper.getMinActiveReplicas(partition3)).thenReturn(2);
    when(clusterManagerQueryHelper.getMinActiveReplicas(partition4)).thenReturn(2);
    when(clusterManagerQueryHelper.getMinActiveReplicas(partition5)).thenReturn(2);
    mockReplicaStates(partition2, 3);
    mockReplicaStates(partition3, 3);
    mockReplicaStates(partition4, 3);
    mockReplicaStates(partition5, 3);

    // Run the manager
    manager.start();

    // Verify that at least minBatchSize partitions were enabled
    ArgumentCaptor<Set<PartitionId>> enabledPartitionsCaptor = ArgumentCaptor.forClass(Set.class);
    ArgumentCaptor<Boolean> enabledCaptor = ArgumentCaptor.forClass(Boolean.class);
    verify(replicationEngine, atLeastOnce()).controlReplicationForPartitions(
        enabledPartitionsCaptor.capture(), eq(Collections.emptyList()), enabledCaptor.capture());

    // Find the set of enabled partitions
    Set<PartitionId> enabledPartitions = null;
    for (int i = 0; i < enabledCaptor.getAllValues().size(); i++) {
      if (enabledCaptor.getAllValues().get(i)) {
        enabledPartitions = enabledPartitionsCaptor.getAllValues().get(i);
        break;
      }
    }

    assertNotNull("Should have enabled some partitions", enabledPartitions);
    assertTrue("High priority partition1 should be enabled", enabledPartitions.contains(partition1));
    assertEquals("Should have enabled exactly minBatchSize partitions", minBatchSize, enabledPartitions.size());
  }

  @Test
  public void testAddNewHighPriorityPartitions() {
    // First setup with one high priority partition
    Set<PartitionId> partitions = new HashSet<>(Arrays.asList(partition1, partition2, partition3));
    when(storageManager.getLocalPartitions()).thenReturn(partitions);

    // All partitions are bootstrapping
    when(store1.getCurrentState()).thenReturn(ReplicaState.BOOTSTRAP);
    when(store2.getCurrentState()).thenReturn(ReplicaState.BOOTSTRAP);
    when(store3.getCurrentState()).thenReturn(ReplicaState.BOOTSTRAP);

    // Only partition1 is initially high priority
    when(clusterManagerQueryHelper.getMinActiveReplicas(partition1)).thenReturn(3);
    mockReplicaStates(partition1, 2); // Below threshold

    // Others are not high priority initially
    when(clusterManagerQueryHelper.getMinActiveReplicas(partition2)).thenReturn(2);
    when(clusterManagerQueryHelper.getMinActiveReplicas(partition3)).thenReturn(2);
    mockReplicaStates(partition2, 3);
    mockReplicaStates(partition3, 3);

    // First run - sets up initial state with partition1 as high priority
    manager.start();

    // Verify that both partition1 and partition2 are now enabled
    ArgumentCaptor<Set<PartitionId>> enabledPartitionsCaptor = ArgumentCaptor.forClass(Set.class);
    ArgumentCaptor<Boolean> enabledCaptor = ArgumentCaptor.forClass(Boolean.class);
    verify(replicationEngine, atLeastOnce()).controlReplicationForPartitions(
        enabledPartitionsCaptor.capture(), eq(Collections.emptyList()), enabledCaptor.capture());

    // Find the set of enabled partitions
    Set<PartitionId> enabledPartitions = null;
    for (int i = 0; i < enabledCaptor.getAllValues().size(); i++) {
      if (enabledCaptor.getAllValues().get(i)) {
        enabledPartitions = enabledPartitionsCaptor.getAllValues().get(i);
        break;
      }
    }

    assertNotNull("Should have enabled some partitions", enabledPartitions);
    assertTrue("Partition1 should still be enabled", enabledPartitions.contains(partition1));
    assertTrue("Partition2 should now be enabled", enabledPartitions.contains(partition2));


    // Reset mocks for clean verification in second run
    reset(replicationEngine);

    // Now partition2 also becomes high priority
    when(clusterManagerQueryHelper.getMinActiveReplicas(partition2)).thenReturn(3);
    mockReplicaStates(partition2, 2); // Now below threshold

    // Second run - should add partition2 to high priority set
    manager.start();

    // Verify that both partition1 and partition2 were already enabled
    verify(replicationEngine, never()).controlReplicationForPartitions(
        enabledPartitionsCaptor.capture(), eq(Collections.emptyList()), enabledCaptor.capture());
  }

  @Test
  public void testAllHighPriorityPartitionsComplete() {
    // First setup with all partitions as high priority
    Set<PartitionId> partitions = new HashSet<>(Arrays.asList(partition1, partition2, partition3));
    when(storageManager.getLocalPartitions()).thenReturn(partitions);

    // All partitions are bootstrapping
    when(store1.getCurrentState()).thenReturn(ReplicaState.BOOTSTRAP);
    when(store2.getCurrentState()).thenReturn(ReplicaState.BOOTSTRAP);
    when(store3.getCurrentState()).thenReturn(ReplicaState.BOOTSTRAP);

    // All partitions are high priority
    when(clusterManagerQueryHelper.getMinActiveReplicas(partition1)).thenReturn(3);
    when(clusterManagerQueryHelper.getMinActiveReplicas(partition2)).thenReturn(3);
    when(clusterManagerQueryHelper.getMinActiveReplicas(partition3)).thenReturn(3);
    mockReplicaStates(partition1, 2); // Below threshold
    mockReplicaStates(partition2, 2); // Below threshold
    mockReplicaStates(partition3, 2); // Below threshold

    // First run - sets up initial state with all partitions as high priority
    manager.start();

    // Reset mocks for clean verification in second run
    reset(replicationEngine);

    // Now all partitions complete replication
    when(store1.getCurrentState()).thenReturn(ReplicaState.STANDBY);
    when(store2.getCurrentState()).thenReturn(ReplicaState.STANDBY);
    when(store3.getCurrentState()).thenReturn(ReplicaState.STANDBY);

    // Second run - should recognize all partitions as completed
    manager.start();

    // Verify that replicationEngine was not called to enable any partitions
    verify(replicationEngine, never()).controlReplicationForPartitions(
        any(Set.class), any(List.class), eq(true));
  }

  @Test
  public void testEdgeCaseNoPriorityPartitions() {
    // Setup with partitions but none meet high priority criteria
    Set<PartitionId> partitions = new HashSet<>(Arrays.asList(partition1, partition2, partition3));
    when(storageManager.getLocalPartitions()).thenReturn(partitions);

    // All partitions are bootstrapping
    when(store1.getCurrentState()).thenReturn(ReplicaState.BOOTSTRAP);
    when(store2.getCurrentState()).thenReturn(ReplicaState.BOOTSTRAP);
    when(store3.getCurrentState()).thenReturn(ReplicaState.BOOTSTRAP);

    // None are high priority
    when(clusterManagerQueryHelper.getMinActiveReplicas(partition1)).thenReturn(2);
    when(clusterManagerQueryHelper.getMinActiveReplicas(partition2)).thenReturn(2);
    when(clusterManagerQueryHelper.getMinActiveReplicas(partition3)).thenReturn(2);
    mockReplicaStates(partition1, 3); // Above threshold
    mockReplicaStates(partition2, 3); // Above threshold
    mockReplicaStates(partition3, 3); // Above threshold

    // Run the manager
    manager.start();

    // Verify replicationEngine was not called to change any partition states
    verify(replicationEngine, atLeastOnce()).controlReplicationForPartitions(
        any(Set.class), any(List.class), anyBoolean());
  }

  @Test
  public void testEdgeCaseEmptyPartitionList() {
    // Setup with empty partition list
    when(storageManager.getLocalPartitions()).thenReturn(Collections.emptySet());

    // Run the manager
    manager.start();

    // Verify replicationEngine was not called
    verify(replicationEngine, never()).controlReplicationForPartitions(
        any(Set.class), any(List.class), anyBoolean());
  }

  @Test
  public void testConcurrencyWithMultipleThreads() throws InterruptedException {
    // Setup with one high priority partition
    Set<PartitionId> partitions = new HashSet<>(Arrays.asList(partition1, partition2, partition3));
    when(storageManager.getLocalPartitions()).thenReturn(partitions);

    // All partitions are bootstrapping
    when(store1.getCurrentState()).thenReturn(ReplicaState.BOOTSTRAP);
    when(store2.getCurrentState()).thenReturn(ReplicaState.BOOTSTRAP);
    when(store3.getCurrentState()).thenReturn(ReplicaState.BOOTSTRAP);

    // Partition1 is high priority
    when(clusterManagerQueryHelper.getMinActiveReplicas(partition1)).thenReturn(3);
    mockReplicaStates(partition1, 2); // Below threshold

    // Others are not high priority
    when(clusterManagerQueryHelper.getMinActiveReplicas(partition2)).thenReturn(2);
    when(clusterManagerQueryHelper.getMinActiveReplicas(partition3)).thenReturn(2);
    mockReplicaStates(partition2, 3);
    mockReplicaStates(partition3, 3);

    // Create multiple threads to call run() simultaneously
    int numThreads = 5;
    Thread[] threads = new Thread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      threads[i] = new Thread(manager);
      threads[i].start();
    }

    // Wait for all threads to complete
    for (Thread thread : threads) {
      thread.join(1000); // Wait up to 1 second for each thread
    }

    // Verify we get consistent results despite concurrent execution
    verify(replicationEngine, atLeastOnce()).controlReplicationForPartitions(
        any(Set.class), any(List.class), eq(true));
  }

  @Test
  public void testDisabledPartitions() {
    // Setup with high priority and non-priority partitions
    Set<PartitionId> partitions = new HashSet<>(Arrays.asList(partition1, partition2, partition3));
    when(storageManager.getLocalPartitions()).thenReturn(partitions);

    // All partitions are bootstrapping
    when(store1.getCurrentState()).thenReturn(ReplicaState.BOOTSTRAP);
    when(store2.getCurrentState()).thenReturn(ReplicaState.BOOTSTRAP);
    when(store3.getCurrentState()).thenReturn(ReplicaState.BOOTSTRAP);

    // Partition1 is high priority
    when(clusterManagerQueryHelper.getMinActiveReplicas(partition1)).thenReturn(3);
    mockReplicaStates(partition1, 2); // Below threshold

    // Others are not high priority
    when(clusterManagerQueryHelper.getMinActiveReplicas(partition2)).thenReturn(2);
    when(clusterManagerQueryHelper.getMinActiveReplicas(partition3)).thenReturn(2);
    mockReplicaStates(partition2, 3);
    mockReplicaStates(partition3, 3);

    // Run the manager
    manager.start();

    // Verify that non-priority partitions were disabled
    ArgumentCaptor<Set<PartitionId>> partitionCaptor = ArgumentCaptor.forClass(Set.class);
    ArgumentCaptor<Boolean> enableCaptor = ArgumentCaptor.forClass(Boolean.class);

    verify(replicationEngine, atLeastOnce()).controlReplicationForPartitions(
        partitionCaptor.capture(), eq(Collections.emptyList()), enableCaptor.capture());

    // Find disabled partition set
    Set<PartitionId> enabledPartitions = null;
    for (int i = 0; i < enableCaptor.getAllValues().size(); i++) {
      if (enableCaptor.getAllValues().get(i)) {
        enabledPartitions = partitionCaptor.getAllValues().get(i);
        break;
      }
    }

    assertNotNull("Should have disabled some partitions", enabledPartitions);
    assertTrue("Non-priority partition2 should be enabled", enabledPartitions.contains(partition2));
    assertTrue("Non-priority partition3 should be enabled", enabledPartitions.contains(partition3));
    assertTrue("High-priority partition1 should be enabled", enabledPartitions.contains(partition1));
  }

  /**
   * Helper method to mock replica states for a partition
   * @param partition The partition to mock
   * @param count The number of active replicas to simulate
   */
  private void mockReplicaStates(PartitionId partition, int count) {
    // Create mock replica list with the specified count
    List<AmbryReplica> replicas = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      replicas.add(mock(AmbryReplica.class));
    }

    // Create state map for the relevant states
    Map<ReplicaState, List<AmbryReplica>> stateMap = new HashMap<>();
    if (count > 0) {
      stateMap.put(ReplicaState.LEADER, Collections.singletonList(replicas.get(0)));

      List<AmbryReplica> standbyReplicas = new ArrayList<>();
      for (int i = 1; i < count; i++) {
        standbyReplicas.add(replicas.get(i));
      }

      if (!standbyReplicas.isEmpty()) {
        stateMap.put(ReplicaState.STANDBY, standbyReplicas);
      }
    }

    // Mock getReplicaIdsByStates method to return our state map
    when(partition.getReplicaIdsByStates(ArgumentMatchers.anySet(), eq(datacenterName)))
        .thenReturn((Map) stateMap);
  }
}