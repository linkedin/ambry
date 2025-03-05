package com.github.ambry.filetransfer;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.ConnectionPoolTimeoutException;
import com.github.ambry.server.StoreManager;
import com.github.ambry.store.FileStoreException;
import com.github.ambry.store.PartitionFileStore;
import com.github.ambry.store.StoreException;
import java.io.IOException;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StoreFileCopyHandler implements FileCopyHandler {
  private final ConnectionPool connectionPool;
  private final StoreManager storeManager;
  private final ClusterMap clusterMap;
  private boolean isRunning = false;

  // TODO: From cfg2
  private static final int CHUNK_SIZE = 10 * 1024 * 1024; // 10MB

  private static final Logger logger = LoggerFactory.getLogger(StoreFileCopyHandler.class);

  public StoreFileCopyHandler(ConnectionPool connectionPool, StoreManager storeManager, ClusterMap clusterMap) {
    Objects.requireNonNull(connectionPool, "ConnectionPool cannot be null");
    Objects.requireNonNull(storeManager, "StoreManager cannot be null");
    Objects.requireNonNull(clusterMap, "ClusterMap cannot be null");

    this.connectionPool = connectionPool;
    this.storeManager = storeManager;
    this.clusterMap = clusterMap;
  }

  public void start() throws StoreException {
    isRunning = true;
  }

  public boolean isRunning() {
    return isRunning;
  }

  public void stop() {
    isRunning = false;
  }

  private void validateIfStoreFileCopyHandlerIsRunning() {
    if (!isRunning) {
      throw new FileStoreException("FileStore is not running", FileStoreException.FileStoreErrorCode.FileStoreRunningFailure);
    }
  }

  public void shutdown() {
    isRunning = false;
  }

  @Override
  public void copy(ReplicaId sourceReplicaId, ReplicaId targetReplicaId)
      throws IOException, ConnectionPoolTimeoutException, InterruptedException {
    validateIfStoreFileCopyHandlerIsRunning();

    PartitionFileStore fileStore = this.storeManager.getFileStore(sourceReplicaId.getPartitionId());
  }
}