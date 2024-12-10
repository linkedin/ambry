package com.github.ambry.store;

import com.github.ambry.clustermap.ClusterParticipant;
import com.github.ambry.clustermap.PartitionStateChangeListener;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.StateModelListenerType;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.server.StoreManager;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BootstrapController {

  static final String BOOTSTRAP_IN_PROGRESS_FILE_NAME = "bootstrap_in_progress";
  static final String FILECOPY_IN_PROGRESS_FILE_NAME = "filecopy_in_progress";
  static final String FILECOPY_METADATA_FILE_NAME = "filecopy_metadata";
  private final StoreConfig storeConfig;

  private static final Logger logger = LoggerFactory.getLogger(BootstrapController.class);

  public BootstrapController(
      @Nonnull StoreManager storeManager,
      @Nonnull StoreConfig storeConfig,
      @Nonnull ClusterParticipant clusterParticipant) {
    this.storeConfig = storeConfig;

    clusterParticipant.registerPartitionStateChangeListener(
        StateModelListenerType.BootstrapControllerListener, new BootstrapControllerImpl(storeManager));
    logger.info("Bootstrap Controller's state change listener registered!");
  }

  class BootstrapControllerImpl implements PartitionStateChangeListener {

    private final StoreManager storeManager;

    public BootstrapControllerImpl(StoreManager storeManager) {
      this.storeManager = storeManager;
    }

    @Override
    public void onPartitionBecomeBootstrapFromOffline(@Nonnull String partitionName) {
      Map<StateModelListenerType, PartitionStateChangeListener> partitionStateChangeListeners =
          this.storeManager.getPartitionStateChangeListeners();

      PartitionStateChangeListener storageManagerListener =
          partitionStateChangeListeners.get(StateModelListenerType.StorageManagerListener);

      PartitionStateChangeListener fileCopyManagerListener =
          partitionStateChangeListeners.get(StateModelListenerType.FileCopyManagerListener);

      ReplicaId replica = this.storeManager.getReplica(partitionName);
      PartitionStateChangeListener listenerToInvoke = null;

      if (null == replica) {
        // there can be two scenarios:
        // 1. this is the first time to add new replica onto current node;
        // 2. last replica addition failed at some point before updating InstanceConfig in Helix
        if (isFileCopyFeatureEnabled()) {
          // "New partition -> FC"
          listenerToInvoke = fileCopyManagerListener;
        } else {
          // "New partition -> R"
          listenerToInvoke = storageManagerListener;
        }
      } else {
        if (isFileCopyFeatureEnabled()) {
          if (isFileExists(replica.getReplicaPath(), BOOTSTRAP_IN_PROGRESS_FILE_NAME)) {
            // R.Incomplete -> FC
            listenerToInvoke = storageManagerListener;
          } else if (isAnyLogSegmentExists()) {
            if (isFileExists(replica.getReplicaPath(), FILECOPY_IN_PROGRESS_FILE_NAME)) {
              // FC.Incomplete -> FC
              listenerToInvoke = fileCopyManagerListener;
            } else {
              // R.complete -> FC or FC.complete -> FC
              listenerToInvoke = storageManagerListener;
            }
          }
        } else {
          if (isFileExists(replica.getReplicaPath(), BOOTSTRAP_IN_PROGRESS_FILE_NAME)) {
            // R.Incomplete -> R
            listenerToInvoke = storageManagerListener;
          } else if (isAnyLogSegmentExists()) {
            if (isFileExists(replica.getReplicaPath(), FILECOPY_IN_PROGRESS_FILE_NAME)) {
              // FC.Incomplete -> R
              try {
                deleteFileCopyData(replica.getReplicaPath());
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
              listenerToInvoke = storageManagerListener;
            } else {
              // R.complete -> R or FC.complete -> R
              listenerToInvoke = storageManagerListener;
            }
          }
        }
      }

      assert listenerToInvoke != null;
      listenerToInvoke.onPartitionBecomeBootstrapFromOffline(partitionName);
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
        @Nonnull String replicaPath, @Nonnull String fileName) {
      return new File(replicaPath, fileName).exists();
    }

    private boolean isAnyLogSegmentExists() {
      // TODO: Implement this method
      return false;
    }

    private void deleteFileCopyData(@Nonnull String replicaPath) throws IOException {
      File fileCopyInProgressFile = new File(replicaPath, FILECOPY_IN_PROGRESS_FILE_NAME);
      Utils.deleteFileOrDirectory(fileCopyInProgressFile);

      File fileCopyMetaDataFile = new File(replicaPath, FILECOPY_METADATA_FILE_NAME);
      Utils.deleteFileOrDirectory(fileCopyMetaDataFile);

      // TODO: Delete log segments
    }
  }
}
