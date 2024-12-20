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
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.StateModelListenerType;
import com.github.ambry.clustermap.StateTransitionException;
import com.github.ambry.config.ServerConfig;
import com.github.ambry.config.ServerReplicationMode;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.server.StoreManager;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.clustermap.StateTransitionException.TransitionErrorCode.*;


/**
 * BootstrapController is responsible for managing the hydration protocol
 * for Offline -> Bootstrap state transition of partitions.
 */
public class BootstrapController {
  private boolean isRunning = false;
  private final String BOOTSTRAP_IN_PROGRESS_FILE_NAME;
  private final String FILECOPY_IN_PROGRESS_FILE_NAME;
  private final Pattern allLogSegmentFilesPattern = Pattern.compile(".*_log.*");
  private final ServerConfig serverConfig;
  private final StoreManager storeManager;
  private final PartitionStateChangeListener storageManagerListener;
  private final PartitionStateChangeListener fileCopyManagerListener;
  private final ClusterParticipant primaryClusterParticipant;

  private static final Logger logger = LoggerFactory.getLogger(BootstrapController.class);

  public BootstrapController(
      @Nonnull StoreManager storeManager, @Nonnull StoreConfig storeConfig,
      @Nonnull ServerConfig serverConfig, @Nonnull ClusterParticipant primaryClusterParticipant) {
    this.serverConfig = serverConfig;
    this.storeManager = storeManager;
    this.primaryClusterParticipant = primaryClusterParticipant;

    this.BOOTSTRAP_IN_PROGRESS_FILE_NAME = storeConfig.storeBootstrapInProgressFile;
    this.FILECOPY_IN_PROGRESS_FILE_NAME = storeConfig.storeFileCopyInProgressFileName;

    primaryClusterParticipant.registerPartitionStateChangeListener(
        StateModelListenerType.BootstrapControllerListener, new BootstrapControllerImpl());
    logger.info("Bootstrap Controller's state change listener registered!");

    Map<StateModelListenerType, PartitionStateChangeListener> partitionStateChangeListeners =
        primaryClusterParticipant.getPartitionStateChangeListeners();

    this.storageManagerListener =
        partitionStateChangeListeners.get(StateModelListenerType.StorageManagerListener);
    this.fileCopyManagerListener =
        partitionStateChangeListeners.get(StateModelListenerType.FileCopyManagerListener);
  }

  void start() {
    if(!isRunning) {
      //Start the FileStore
      isRunning = true;
    }
  }

  void shutdown() {
    //Implement shutdown Hook.
    isRunning = false;
  }

  boolean isRunning() {
    return isRunning;
  }

  class BootstrapControllerImpl implements PartitionStateChangeListener {
    EnumSet<ReplicationProtocolTransitionType> replicationProtocolTransitionType;

    /**
     * TODO: Add context
     * @param partitionName of the partition.
     */
    @Override
    public void onPartitionBecomeBootstrapFromOffline(@Nonnull String partitionName) {
      logger.info("Bootstrap Controller's state change listener invoked for partition `{}`, state change `{}`",
          partitionName, "Offline -> Bootstrap");

      ReplicaId replica = storeManager.getReplica(partitionName);
      PartitionStateChangeListener listenerToInvoke = null;

      if (null == replica) {
        if (isFileCopyFeatureEnabled()) {
          // "New partition -> FC"
          // This is a new partition placement and FileCopy bootstrap protocol is enabled.
          listenerToInvoke = fileCopyManagerListener;

          replicationProtocolTransitionType = EnumSet.of(
              ReplicationProtocolTransitionType.NEW_PARTITION_TO_FILE_BASED_HYDRATION);
          logStateChange("New partition -> FC", partitionName);
        } else {
          // "New partition -> R"
          // This is a new partition placement and FileCopy bootstrap protocol is disabled.
          listenerToInvoke = storageManagerListener;

          replicationProtocolTransitionType = EnumSet.of(
              ReplicationProtocolTransitionType.NEW_PARTITION_TO_BLOB_BASED_HYDRATION);
          logStateChange("New partition -> R", partitionName);
        }
      } else {
        if (isFileCopyFeatureEnabled()) {
          if (isFileExists(replica.getPartitionId(), BOOTSTRAP_IN_PROGRESS_FILE_NAME)) {
            // R.Incomplete -> FC
            // Last attempt with blob based bootstrap protocol had failed for this partition.
            // FileCopy bootstrap protocol is enabled but we will still continue with blob based bootstrap protocol.
            listenerToInvoke = storageManagerListener;

            replicationProtocolTransitionType = EnumSet.of(
                ReplicationProtocolTransitionType.BLOB_BASED_HYDRATION_INCOMPLETE_TO_FILE_BASED_HYDRATION);
            logStateChange("R.Incomplete -> FC", partitionName);
          } else if (isAnyLogSegmentExists(replica.getPartitionId())) {
            if (isFileExists(replica.getPartitionId(), FILECOPY_IN_PROGRESS_FILE_NAME)) {
              // FC.Incomplete -> FC
              // Last attempt with FileCopy bootstrap protocol had failed for this partition.
              // We will resume the boostrap with FileCopy bootstrap protocol.
              listenerToInvoke = fileCopyManagerListener;

              replicationProtocolTransitionType = EnumSet.of(
                  ReplicationProtocolTransitionType.FILE_BASED_HYDRATION_INCOMPLETE_TO_FILE_BASED_HYDRATION);
              logStateChange("FC.Incomplete -> FC", partitionName);
            } else {
              // R.complete -> FC or FC.complete -> FC
              // Last attempt either with blob based or file based bootstrap protocol had succeeded for this partition.
              // We'll continue with blob based bootstrap protocol for this partition to catch up with its peers.
              // as part of Bootstrap->Standby state transition.
              listenerToInvoke = storageManagerListener;

              replicationProtocolTransitionType = EnumSet.of(
                  ReplicationProtocolTransitionType.BLOB_BASED_HYDRATION_COMPLETE_TO_FILE_BASED_HYDRATION,
                  ReplicationProtocolTransitionType.FILE_BASED_HYDRATION_COMPLETE_TO_FILE_BASED_HYDRATION);
              logStateChange("R.complete -> FC or FC.complete -> FC", partitionName);
            }
          }
        } else {
          if (isFileExists(replica.getPartitionId(), BOOTSTRAP_IN_PROGRESS_FILE_NAME)) {
            // R.Incomplete -> R
            // Last attempt with blob based bootstrap protocol had failed for this partition.
            // FileCopy bootstrap protocol is disabled and we will continue with blob based bootstrap protocol.
            listenerToInvoke = storageManagerListener;

            replicationProtocolTransitionType = EnumSet.of(
                ReplicationProtocolTransitionType.BLOB_BASED_HYDRATION_INCOMPLETE_TO_BLOB_BASED_HYDRATION);
            logStateChange("R.Incomplete -> R", partitionName);
          } else if (isAnyLogSegmentExists(replica.getPartitionId())) {
            if (isFileExists(replica.getPartitionId(), FILECOPY_IN_PROGRESS_FILE_NAME)) {
              // FC.Incomplete -> R
              // Last attempt with FileCopy bootstrap protocol had failed for this partition.
              // First we delete FileCopy data and then continue with blob based bootstrap protocol.
              try {
                deleteFileCopyData(replica.getPartitionId());
              } catch (IOException | StoreException e) {
                String message = "Failed `deleteFileCopyData` step for " + partitionName;
                logger.error(message);
                throw new StateTransitionException(message, BootstrapControllerFailure);
              }
              listenerToInvoke = storageManagerListener;

              replicationProtocolTransitionType = EnumSet.of(
                  ReplicationProtocolTransitionType.FILE_BASED_HYDRATION_INCOMPLETE_TO_BLOB_BASED_HYDRATION);
              logStateChange("FC.Incomplete -> R", partitionName);
            } else {
              // R.complete -> R or FC.complete -> R
              // Last attempt either with blob based or file based bootstrap protocol had succeeded for this partition.
              // We'll continue with blob based bootstrap protocol for this partition to catch up with its peers.
              // as part of Bootstrap->Standby state transition.
              listenerToInvoke = storageManagerListener;

              replicationProtocolTransitionType = EnumSet.of(
                  ReplicationProtocolTransitionType.BLOB_BASED_HYDRATION_COMPLETE_TO_BLOB_BASED_HYDRATION,
                  ReplicationProtocolTransitionType.FILE_BASED_HYDRATION_COMPLETE_TO_BLOB_BASED_HYDRATION);
              logStateChange("R.complete -> R or FC.complete -> R", partitionName);
            }
          }
        }
      }

      assert listenerToInvoke != null;
      listenerToInvoke.onPartitionBecomeBootstrapFromOffline(partitionName);
      if (listenerToInvoke == fileCopyManagerListener) {
        try {
          primaryClusterParticipant.getReplicaSyncUpManager().waitForFileCopyCompleted(partitionName);
        } catch (InterruptedException e) {
          String message = "Failed `waitForFileCopyCompleted` step for " + partitionName;
          logger.error(message);
          throw new StateTransitionException(message, BootstrapControllerFailure);
        } catch (Exception e) {
          logger.error(e.getMessage());
          throw new StateTransitionException(e.getMessage(), BootstrapControllerFailure);
        }
      }
    }

    @Override
    public void onPartitionBecomeStandbyFromBootstrap(String partitionName) {
      // no op
    }

    @Override
    public void onPartitionBecomeLeaderFromStandby(String partitionName) {
      // no op
    }

    @Override
    public void onPartitionBecomeStandbyFromLeader(String partitionName) {
      // no op
    }

    @Override
    public void onPartitionBecomeInactiveFromStandby(String partitionName) {
      // no op
    }

    @Override
    public void onPartitionBecomeOfflineFromInactive(String partitionName) {
      // no op
    }

    @Override
    public void onPartitionBecomeDroppedFromOffline(String partitionName) {
      // no op
    }

    private void logStateChange(String stateChange, String partitionName) {
      logger.info("BootstrapController State change `{}` for partition `{}`", stateChange, partitionName);
    }

    // Helper method to check if FileCopy bootstrap protocol is enabled.
    private boolean isFileCopyFeatureEnabled() {
      return serverConfig.serverReplicationProtocolForHydration.equals(ServerReplicationMode.FILE_BASED);
    }

    // Helper method to check if a file exists in the partition.
    boolean isFileExists(
        @Nonnull PartitionId partitionId, @Nonnull String fileName) {
      return storeManager.isFileExists(partitionId, fileName);
    }

    // Helper method to check if any log segment files exist in the partition.
    boolean isAnyLogSegmentExists(@Nonnull PartitionId partitionId) {
      try {
        return storeManager.isFilesExistForPattern(partitionId, allLogSegmentFilesPattern);
      } catch (IOException e) {
        String message = "Failed `isFilesExistForPattern` step for " + partitionId;
        logger.error(message);
        throw new StateTransitionException(message, BootstrapControllerFailure);
      }
    }

    // Helper method to delete the file copy data.
    void deleteFileCopyData(@Nonnull PartitionId partitionId) throws IOException, StoreException {
      // Currently we’ll delete all datasets by removing this partition's BlobStore
      // TODO: An optimisation could be explored to only delete incomplete datasets.
      // TODO: Write logic to cleanup the files inside the directory

      throw new UnsupportedOperationException("Not implemented yet");
    }
  }
}