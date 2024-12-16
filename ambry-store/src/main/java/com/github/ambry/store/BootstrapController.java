package com.github.ambry.store;

import com.github.ambry.clustermap.ClusterParticipant;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.PartitionStateChangeListener;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.StateModelListenerType;
import com.github.ambry.clustermap.StateTransitionException;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.server.StoreManager;
import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.clustermap.StateTransitionException.TransitionErrorCode.*;


public class BootstrapController {

  static final String BOOTSTRAP_IN_PROGRESS_FILE_NAME = "bootstrap_in_progress";
  static final String FILECOPY_IN_PROGRESS_FILE_NAME = "filecopy_in_progress";
  private final Pattern allLogSegmentFilesPattern = Pattern.compile("\\d+\\.log");
  private final StoreConfig storeConfig;
  private final StoreManager storeManager;
  private final PartitionStateChangeListener storageManagerListener;
  private final PartitionStateChangeListener fileCopyManagerListener;

  private static final Logger logger = LoggerFactory.getLogger(BootstrapController.class);

  public BootstrapController(
      @Nonnull StoreManager storeManager,
      @Nonnull StoreConfig storeConfig,
      @Nonnull ClusterParticipant clusterParticipant) {
    this.storeConfig = storeConfig;
    this.storeManager = storeManager;

    clusterParticipant.registerPartitionStateChangeListener(
        StateModelListenerType.BootstrapControllerListener, new BootstrapControllerImpl());

    Map<StateModelListenerType, PartitionStateChangeListener> partitionStateChangeListeners =
        storeManager.getPrimaryClusterParticipant().getPartitionStateChangeListeners();

    this.storageManagerListener =
        partitionStateChangeListeners.get(StateModelListenerType.StorageManagerListener);

    this.fileCopyManagerListener =
        partitionStateChangeListeners.get(StateModelListenerType.FileCopyManagerListener);

    logger.info("Bootstrap Controller's state change listener registered!");
  }

  class BootstrapControllerImpl implements PartitionStateChangeListener {

    @Override
    public void onPartitionBecomeBootstrapFromOffline(@Nonnull String partitionName) {
      ReplicaId replica = storeManager.getReplica(partitionName);
      PartitionStateChangeListener listenerToInvoke = null;

      /**
       * This algo is based on this design doc :-
       * https://docs.google.com/document/d/1u57C0BU8oMMuMHC6Fh_B-6R612EaQb2AxtnBDlN6sAs
       * The decision branches can be better understood with the doc.
       */
      if (null == replica) {
        // there can be two scenarios:
        // 1. this is the first time to add new replica onto current node;
        // 2. last replica addition failed at some point before updating InstanceConfig in Helix
        if (isFileCopyFeatureEnabled()) {
          // "New partition -> FC"
          listenerToInvoke = fileCopyManagerListener;
          logStateChange("New partition -> FC", partitionName);
        } else {
          // "New partition -> R"
          listenerToInvoke = storageManagerListener;
          logStateChange("New partition -> R", partitionName);
        }
      } else {
        if (isFileCopyFeatureEnabled()) {
          if (isFileExists(replica.getPartitionId(), BOOTSTRAP_IN_PROGRESS_FILE_NAME)) {
            // R.Incomplete -> FC
            listenerToInvoke = storageManagerListener;
            logStateChange("R.Incomplete -> FC", partitionName);
          } else if (isAnyLogSegmentExists(replica.getPartitionId())) {
            if (isFileExists(replica.getPartitionId(), FILECOPY_IN_PROGRESS_FILE_NAME)) {
              // FC.Incomplete -> FC
              listenerToInvoke = fileCopyManagerListener;
              logStateChange("FC.Incomplete -> FC", partitionName);
            } else {
              // R.complete -> FC or FC.complete -> FC
              listenerToInvoke = storageManagerListener;
              logStateChange("R.complete -> FC or FC.complete -> FC", partitionName);
            }
          }
        } else {
          if (isFileExists(replica.getPartitionId(), BOOTSTRAP_IN_PROGRESS_FILE_NAME)) {
            // R.Incomplete -> R
            listenerToInvoke = storageManagerListener;
            logStateChange("R.Incomplete -> R", partitionName);
          } else if (isAnyLogSegmentExists(replica.getPartitionId())) {
            if (isFileExists(replica.getPartitionId(), FILECOPY_IN_PROGRESS_FILE_NAME)) {
              // FC.Incomplete -> R
              try {
                deleteFileCopyData(replica.getPartitionId());
              } catch (IOException | StoreException e) {
                String message = "Failed `deleteFileCopyData` step for " + partitionName;
                throw new StateTransitionException(message, BootstrapControllerFailure);
              }
              listenerToInvoke = storageManagerListener;
              logStateChange("FC.Incomplete -> R", partitionName);
            } else {
              // R.complete -> R or FC.complete -> R
              listenerToInvoke = storageManagerListener;
              logStateChange("R.complete -> R or FC.complete -> R", partitionName);
            }
          }
        }
      }

      assert listenerToInvoke != null;
      listenerToInvoke.onPartitionBecomeBootstrapFromOffline(partitionName);
      if (listenerToInvoke == fileCopyManagerListener) {
        try {
          storeManager.getPrimaryClusterParticipant().getReplicaSyncUpManager().waitForFileCopyCompleted(partitionName);
        } catch (InterruptedException e) {
          String message = "Failed `waitForFileCopyCompleted` step for " + partitionName;
          throw new StateTransitionException(message, BootstrapControllerFailure);
        }
      }
    }

    private void logStateChange(String stateChange, String partitionName) {
      logger.info("BootstrapController State change `{}` for partition `{}`", stateChange, partitionName);
    }

    @Override
    public void onPartitionBecomeStandbyFromBootstrap(String partitionName) {
      throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void onPartitionBecomeLeaderFromStandby(String partitionName) {
      throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void onPartitionBecomeStandbyFromLeader(String partitionName) {
      throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void onPartitionBecomeInactiveFromStandby(String partitionName) {
      throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void onPartitionBecomeOfflineFromInactive(String partitionName) {
      throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void onPartitionBecomeDroppedFromOffline(String partitionName) {
      throw new UnsupportedOperationException("Not implemented");
    }

    private boolean isFileCopyFeatureEnabled() {
      return storeConfig.fileCopyFeatureEnabled.equals("true");
    }

    private boolean isFileExists(
        @Nonnull PartitionId partitionId, @Nonnull String fileName) {
      return storeManager.isFileExists(partitionId, fileName);
    }

    private boolean isAnyLogSegmentExists(@Nonnull PartitionId partitionId) {
      try {
        return storeManager.isFilesExistForPattern(partitionId, allLogSegmentFilesPattern);
      } catch (IOException e) {
        String message = "Failed `isFilesExistForPattern` step for " + partitionId;
        throw new StateTransitionException(message, BootstrapControllerFailure);
      }
    }

    private void deleteFileCopyData(@Nonnull PartitionId partitionId) throws IOException, StoreException {
      // Currently we’ll delete all datasets by removing this partition's BlobStore
      // An optimisation could be explored to only delete incomplete datasets
      // More about this can be found here :-
      // https://docs.google.com/document/d/1u57C0BU8oMMuMHC6Fh_B-6R612EaQb2AxtnBDlN6sAs/edit?disco=AAABZyj2rHo
      storeManager.removeBlobStore(partitionId);
    }
  }
}