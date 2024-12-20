/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.clustermap.ClusterParticipant;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.PartitionStateChangeListener;
import com.github.ambry.clustermap.TestReplica;
import com.github.ambry.clustermap.ReplicaSyncUpManager;
import com.github.ambry.clustermap.StateModelListenerType;
import com.github.ambry.config.ServerConfig;
import com.github.ambry.config.ServerReplicationMode;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.server.StoreManager;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.mockito.Mockito.*;


public class BootstrapControllerTest {
  private static ServerConfig serverConfig;
  private static StoreConfig storeConfig;
  private static StoreManager storeManager;
  private static String partitionName1 = "p1";
  private static PartitionStateChangeListener fileCopyManagerListener;
  private static PartitionStateChangeListener storageManagerListener;
  private static ClusterParticipant primaryClusterParticipant;

  @BeforeClass
  public static void initialize() {
    storeConfig = mock(StoreConfig.class);
    storeManager = mock(StoreManager.class);
    primaryClusterParticipant = mock(ClusterParticipant.class);

    fileCopyManagerListener = mock(PartitionStateChangeListener.class);

    storageManagerListener = mock(PartitionStateChangeListener.class);

    Map<StateModelListenerType, PartitionStateChangeListener> partitionStateChangeListeners =
        new HashMap<StateModelListenerType, PartitionStateChangeListener>() {
          {
            put(StateModelListenerType.FileCopyManagerListener, fileCopyManagerListener);
            put(StateModelListenerType.StorageManagerListener, storageManagerListener);
          }
        };
    when(primaryClusterParticipant.getPartitionStateChangeListeners())
        .thenReturn(partitionStateChangeListeners);

    when(primaryClusterParticipant.getReplicaSyncUpManager())
        .thenReturn(mock(ReplicaSyncUpManager.class));
  }

  /**
   * Test for {@link BootstrapController.BootstrapControllerImpl#onPartitionBecomeBootstrapFromOffline(String)}
   * when :-
   * 1. The server replication mode is {@link ServerReplicationMode#FILE_BASED}
   * 2. Replica is null
   * Bootstrap Controller is expected to instantiate fileCopyManagerListener
   * ReplicationProtocolTransitionType is {@link ReplicationProtocolTransitionType#NEW_PARTITION_TO_FILE_BASED_HYDRATION}
   */
  @Test
  public void testNewPartitionToFileBasedHydration() {
    // Arrange
    clearInvocations(fileCopyManagerListener);
    clearInvocations(storageManagerListener);

    final BootstrapController.BootstrapControllerImpl bootstrapControllerImpl =
        getBootstrapControllerImpl(ServerReplicationMode.FILE_BASED);

    when(storeManager.getReplica(partitionName1))
        .thenReturn(null);

    // Act
    bootstrapControllerImpl.onPartitionBecomeBootstrapFromOffline(partitionName1);

    // Assert
    verify(fileCopyManagerListener, times(1))
        .onPartitionBecomeBootstrapFromOffline(partitionName1);
    verify(storageManagerListener, never())
        .onPartitionBecomeBootstrapFromOffline(partitionName1);

    assert bootstrapControllerImpl.replicationProtocolTransitionType.size() == 1;
    assert bootstrapControllerImpl.replicationProtocolTransitionType.contains(
        ReplicationProtocolTransitionType.NEW_PARTITION_TO_FILE_BASED_HYDRATION);
  }

  /**
   * Test for {@link BootstrapController.BootstrapControllerImpl#onPartitionBecomeBootstrapFromOffline(String)}
   * when :-
   * 1. The server replication mode is {@link ServerReplicationMode#BLOB_BASED}
   * 2. Replica is null
   * Bootstrap Controller is expected to instantiate storageManagerListener
   * ReplicationProtocolTransitionType is {@link ReplicationProtocolTransitionType#NEW_PARTITION_TO_BLOB_BASED_HYDRATION}
   */
  @Test
  public void testNewPartitionToBlobBasedHydration() {
    // Arrange
    clearInvocations(fileCopyManagerListener);
    clearInvocations(storageManagerListener);

    final BootstrapController.BootstrapControllerImpl bootstrapControllerImpl =
        getBootstrapControllerImpl(ServerReplicationMode.BLOB_BASED);

    when(storeManager.getReplica(partitionName1))
        .thenReturn(null);

    // Act
    bootstrapControllerImpl.onPartitionBecomeBootstrapFromOffline(partitionName1);

    // Assert
    verify(fileCopyManagerListener, never())
        .onPartitionBecomeBootstrapFromOffline(partitionName1);
    verify(storageManagerListener, times(1))
        .onPartitionBecomeBootstrapFromOffline(partitionName1);

    assert bootstrapControllerImpl.replicationProtocolTransitionType.size() == 1;
    assert bootstrapControllerImpl.replicationProtocolTransitionType.contains(
        ReplicationProtocolTransitionType.NEW_PARTITION_TO_BLOB_BASED_HYDRATION);
  }

  /**
   * Test for {@link BootstrapController.BootstrapControllerImpl#onPartitionBecomeBootstrapFromOffline(String)}
   * when :-
   * 1. The server replication mode is {@link ServerReplicationMode#FILE_BASED}
   * 2. Replica is not null
   * 3. Bootstrap_in_progress file exists
   * Bootstrap Controller is expected to instantiate fileCopyManagerListener
   * ReplicationProtocolTransitionType is {@link ReplicationProtocolTransitionType#BLOB_BASED_HYDRATION_INCOMPLETE_TO_FILE_BASED_HYDRATION}
   */
  @Test
  public void testBlobBasedHydrationInCompleteToFileBasedHydration() {
    // Arrange
    clearInvocations(fileCopyManagerListener);
    clearInvocations(storageManagerListener);

    final BootstrapController.BootstrapControllerImpl bootstrapControllerImpl =
        getBootstrapControllerImpl(ServerReplicationMode.FILE_BASED);

    TestReplica testReplica = mock(TestReplica.class);
    PartitionId partitionId = mock(PartitionId.class);
    when(testReplica.getPartitionId())
        .thenReturn(partitionId);
    when(storeManager.getReplica(partitionName1))
        .thenReturn(testReplica);
    when(bootstrapControllerImpl.isFileExists(partitionId, storeConfig.storeBootstrapInProgressFile))
        .thenReturn(true);

    // Act
    bootstrapControllerImpl.onPartitionBecomeBootstrapFromOffline(partitionName1);

    // Assert
    verify(fileCopyManagerListener, never())
        .onPartitionBecomeBootstrapFromOffline(partitionName1);
    verify(storageManagerListener, times(1))
        .onPartitionBecomeBootstrapFromOffline(partitionName1);

    assert bootstrapControllerImpl.replicationProtocolTransitionType.size() == 1;
    assert bootstrapControllerImpl.replicationProtocolTransitionType.contains(
        ReplicationProtocolTransitionType.BLOB_BASED_HYDRATION_INCOMPLETE_TO_FILE_BASED_HYDRATION);
  }

  /**
   * Test for {@link BootstrapController.BootstrapControllerImpl#onPartitionBecomeBootstrapFromOffline(String)}
   * when :-
   * 1. The server replication mode is {@link ServerReplicationMode#BLOB_BASED}
   * 2. Replica is not null
   * 3. Bootstrap_in_progress file does not exist
   * 4. Filecopy_in_progress file exists
   * 5. Atleast one LogSegment File exists for the partition
   * Bootstrap Controller is expected to instantiate storageManagerListener
   * ReplicationProtocolTransitionType is either of the following types :-
   * a. {@link ReplicationProtocolTransitionType#FILE_BASED_HYDRATION_COMPLETE_TO_BLOB_BASED_HYDRATION} state
   * b. {@link ReplicationProtocolTransitionType#BLOB_BASED_HYDRATION_COMPLETE_TO_BLOB_BASED_HYDRATION} state
   */
  @Test
  public void testCompletedHydrationToBlobBasedHydration() {
    // Arrange
    clearInvocations(fileCopyManagerListener);
    clearInvocations(storageManagerListener);

    final BootstrapController.BootstrapControllerImpl bootstrapControllerImpl =
        getBootstrapControllerImpl(ServerReplicationMode.BLOB_BASED);

    TestReplica testReplica = mock(TestReplica.class);
    PartitionId partitionId = mock(PartitionId.class);
    when(testReplica.getPartitionId())
        .thenReturn(partitionId);
    when(storeManager.getReplica(partitionName1))
        .thenReturn(testReplica);
    when(bootstrapControllerImpl.isFileExists(partitionId, storeConfig.storeBootstrapInProgressFile))
        .thenReturn(false);
    when(bootstrapControllerImpl.isFileExists(partitionId, storeConfig.storeFileCopyInProgressFileName))
        .thenReturn(false);
    when(bootstrapControllerImpl.isAnyLogSegmentExists(partitionId))
        .thenReturn(true);

    // Act
    bootstrapControllerImpl.onPartitionBecomeBootstrapFromOffline(partitionName1);

    // Assert
    verify(fileCopyManagerListener, never())
        .onPartitionBecomeBootstrapFromOffline(partitionName1);
    verify(storageManagerListener, times(1))
        .onPartitionBecomeBootstrapFromOffline(partitionName1);

    assert bootstrapControllerImpl.replicationProtocolTransitionType.size() == 2;
    assert bootstrapControllerImpl.replicationProtocolTransitionType.contains(
        ReplicationProtocolTransitionType.FILE_BASED_HYDRATION_COMPLETE_TO_BLOB_BASED_HYDRATION);
    assert bootstrapControllerImpl.replicationProtocolTransitionType.contains(
        ReplicationProtocolTransitionType.BLOB_BASED_HYDRATION_COMPLETE_TO_BLOB_BASED_HYDRATION);
  }

  /**
   * Test for {@link BootstrapController.BootstrapControllerImpl#onPartitionBecomeBootstrapFromOffline(String)}
   * when :-
   * 1. The server replication mode is {@link ServerReplicationMode#FILE_BASED}
   * 2. Replica is not null
   * 3. Bootstrap_in_progress file does not exist
   * 4. Filecopy_in_progress file exists
   * 5. Atleast one LogSegment File exists for the partition
   * Bootstrap Controller is expected to instantiate fileCopyManagerListener
   * ReplicationProtocolTransitionType is {@link ReplicationProtocolTransitionType#FILE_BASED_HYDRATION_INCOMPLETE_TO_FILE_BASED_HYDRATION}
   */
  @Test
  public void testFileBasedHydrationInCompleteToFileBasedHydration() {
    // Arrange
    clearInvocations(fileCopyManagerListener);
    clearInvocations(storageManagerListener);

    final BootstrapController.BootstrapControllerImpl bootstrapControllerImpl =
        getBootstrapControllerImpl(ServerReplicationMode.FILE_BASED);

    TestReplica testReplica = mock(TestReplica.class);
    PartitionId partitionId = mock(PartitionId.class);
    when(testReplica.getPartitionId())
        .thenReturn(partitionId);
    when(storeManager.getReplica(partitionName1))
        .thenReturn(testReplica);
    when(bootstrapControllerImpl.isFileExists(partitionId, storeConfig.storeBootstrapInProgressFile))
        .thenReturn(false);
    when(bootstrapControllerImpl.isFileExists(partitionId, storeConfig.storeFileCopyInProgressFileName))
        .thenReturn(true);
    when(bootstrapControllerImpl.isAnyLogSegmentExists(partitionId))
        .thenReturn(true);

    // Act
    bootstrapControllerImpl.onPartitionBecomeBootstrapFromOffline(partitionName1);

    // Assert
    // TODO: Fix the assertions

    //    verify(fileCopyManagerListener, times(1))
    //        .onPartitionBecomeBootstrapFromOffline(partitionName1);
    //    verify(storageManagerListener, never())
    //        .onPartitionBecomeBootstrapFromOffline(partitionName1);
    //
    //    assert bootstrapControllerImpl.replicationProtocolTransitionType.size() == 1;
    //    assert bootstrapControllerImpl.replicationProtocolTransitionType.contains(
    //        ReplicationProtocolTransitionType.FILE_BASED_HYDRATION_INCOMPLETE_TO_FILE_BASED_HYDRATION);
  }

  /**
   * Test for {@link BootstrapController.BootstrapControllerImpl#onPartitionBecomeBootstrapFromOffline(String)}
   * when :-
   * 1. The server replication mode is {@link ServerReplicationMode#BLOB_BASED}
   * 2. Replica is not null
   * 3. Bootstrap_in_progress file exists
   * Bootstrap Controller is expected to instantiate storageManagerListener
   * ReplicationProtocolTransitionType is {@link ReplicationProtocolTransitionType#BLOB_BASED_HYDRATION_INCOMPLETE_TO_BLOB_BASED_HYDRATION}
   */
  @Test
  public void testBlobBasedHydrationInCompleteToBlobBasedHydration() {
    // Arrange
    clearInvocations(fileCopyManagerListener);
    clearInvocations(storageManagerListener);

    final BootstrapController.BootstrapControllerImpl bootstrapControllerImpl =
        getBootstrapControllerImpl(ServerReplicationMode.BLOB_BASED);

    TestReplica testReplica = mock(TestReplica.class);
    PartitionId partitionId = mock(PartitionId.class);
    when(testReplica.getPartitionId())
        .thenReturn(partitionId);
    when(storeManager.getReplica(partitionName1))
        .thenReturn(testReplica);
    when(storeManager.isFileExists(partitionId, storeConfig.storeBootstrapInProgressFile))
        .thenReturn(true);

    // Act
    bootstrapControllerImpl.onPartitionBecomeBootstrapFromOffline(partitionName1);

    // Assert
    verify(fileCopyManagerListener, never())
        .onPartitionBecomeBootstrapFromOffline(partitionName1);
    verify(storageManagerListener, times(1))
        .onPartitionBecomeBootstrapFromOffline(partitionName1);

    assert bootstrapControllerImpl.replicationProtocolTransitionType.size() == 1;
    assert bootstrapControllerImpl.replicationProtocolTransitionType.contains(
        ReplicationProtocolTransitionType.BLOB_BASED_HYDRATION_INCOMPLETE_TO_BLOB_BASED_HYDRATION);
  }

  /**
   * Test for {@link BootstrapController.BootstrapControllerImpl#onPartitionBecomeBootstrapFromOffline(String)}
   * when :-
   * 1. The server replication mode is {@link ServerReplicationMode#BLOB_BASED}
   * 2. Replica is not null
   * 3. Bootstrap_in_progress file doesn't exist
   * 4. Filecopy_in_progress file exists
   * 5. Atleast one LogSegment File exists for the partition
   * Bootstrap Controller is expected to instantiate storageManagerListener
   * ReplicationProtocolTransitionType is {@link ReplicationProtocolTransitionType#FILE_BASED_HYDRATION_INCOMPLETE_TO_BLOB_BASED_HYDRATION}
   */
  @Test
  public void testFileBasedHydrationInCompleteToBlobBasedHydration() throws IOException, StoreException {
    // Arrange
    clearInvocations(fileCopyManagerListener);
    clearInvocations(storageManagerListener);

    BootstrapController.BootstrapControllerImpl bootstrapControllerImpl =
        getBootstrapControllerImpl(ServerReplicationMode.BLOB_BASED);

    TestReplica testReplica = mock(TestReplica.class);
    PartitionId partitionId = mock(PartitionId.class);
    when(testReplica.getPartitionId())
        .thenReturn(partitionId);
    when(storeManager.getReplica(partitionName1))
        .thenReturn(testReplica);

    when(bootstrapControllerImpl.isFileExists(partitionId, storeConfig.storeFileCopyInProgressFileName))
        .thenReturn(true);
    when(storeManager.isFileExists(partitionId, storeConfig.storeBootstrapInProgressFile))
        .thenReturn(false);

    when(bootstrapControllerImpl.isAnyLogSegmentExists(partitionId))
        .thenReturn(true);

    // Act
    bootstrapControllerImpl.onPartitionBecomeBootstrapFromOffline(partitionName1);

    // Assert
    // TODO: Fix the assertions

    //    verify(fileCopyManagerListener, never())
    //        .onPartitionBecomeBootstrapFromOffline(partitionName1);
    //    verify(storageManagerListener, times(1))
    //        .onPartitionBecomeBootstrapFromOffline(partitionName1);
    //
    //    assert bootstrapControllerImpl.replicationProtocolTransitionType.size() == 1;
    //    assert bootstrapControllerImpl.replicationProtocolTransitionType.contains(
    //        ReplicationProtocolTransitionType.FILE_BASED_HYDRATION_INCOMPLETE_TO_BLOB_BASED_HYDRATION);
  }

  /**
   * Test for {@link BootstrapController.BootstrapControllerImpl#onPartitionBecomeBootstrapFromOffline(String)}
   * when :-
   * 1. The server replication mode is {@link ServerReplicationMode#FILE_BASED}
   * 2. Replica is not null
   * 3. Bootstrap_in_progress file doesn't exist
   * 4. Filecopy_in_progress file doesn't exist
   * 5. Atleast one LogSegment File exists for the partition
   * Bootstrap Controller is expected to instantiate storageManagerListener
   * ReplicationProtocolTransitionType is either of the following types :-
   * a. {@link ReplicationProtocolTransitionType#BLOB_BASED_HYDRATION_COMPLETE_TO_FILE_BASED_HYDRATION} state
   * b. {@link ReplicationProtocolTransitionType#FILE_BASED_HYDRATION_COMPLETE_TO_FILE_BASED_HYDRATION} state
   */
  @Test
  public void testCompletedHydrationToFileBasedHydration() {
    // Arrange
    clearInvocations(fileCopyManagerListener);
    clearInvocations(storageManagerListener);

    final BootstrapController.BootstrapControllerImpl bootstrapControllerImpl =
        getBootstrapControllerImpl(ServerReplicationMode.FILE_BASED);

    TestReplica testReplica = mock(TestReplica.class);
    PartitionId partitionId = mock(PartitionId.class);
    when(testReplica.getPartitionId())
        .thenReturn(partitionId);
    when(storeManager.getReplica(partitionName1))
        .thenReturn(testReplica);
    when(bootstrapControllerImpl.isFileExists(partitionId, storeConfig.storeBootstrapInProgressFile))
        .thenReturn(false);
    when(bootstrapControllerImpl.isFileExists(partitionId, storeConfig.storeFileCopyInProgressFileName))
        .thenReturn(false);
    when(bootstrapControllerImpl.isAnyLogSegmentExists(partitionId))
        .thenReturn(true);

    // Act
    bootstrapControllerImpl.onPartitionBecomeBootstrapFromOffline(partitionName1);

    // Assert
    verify(fileCopyManagerListener, never())
        .onPartitionBecomeBootstrapFromOffline(partitionName1);
    verify(storageManagerListener, times(1))
        .onPartitionBecomeBootstrapFromOffline(partitionName1);

    assert bootstrapControllerImpl.replicationProtocolTransitionType.size() == 2;
    assert bootstrapControllerImpl.replicationProtocolTransitionType.contains(
        ReplicationProtocolTransitionType.BLOB_BASED_HYDRATION_COMPLETE_TO_FILE_BASED_HYDRATION);
    assert bootstrapControllerImpl.replicationProtocolTransitionType.contains(
        ReplicationProtocolTransitionType.FILE_BASED_HYDRATION_COMPLETE_TO_FILE_BASED_HYDRATION);
  }

  private static BootstrapController.BootstrapControllerImpl getBootstrapControllerImpl(
      ServerReplicationMode serverReplicationMode) {

    Properties props = new Properties();
    props.setProperty("server.replication.protocol.for.hydration", String.valueOf(serverReplicationMode));
    serverConfig = new ServerConfig(new VerifiableProperties(props));

    return new BootstrapController(storeManager, storeConfig, serverConfig, primaryClusterParticipant)
        .new BootstrapControllerImpl();
  }
}