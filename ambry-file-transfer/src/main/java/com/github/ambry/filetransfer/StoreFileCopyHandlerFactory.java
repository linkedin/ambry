package com.github.ambry.filetransfer;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.server.StoreManager;


public class StoreFileCopyHandlerFactory implements FileCopyHandlerFactory {
  private final ConnectionPool connectionPool;
  private final StoreManager storeManager;
  private final ClusterMap clusterMap;

  public StoreFileCopyHandlerFactory(ConnectionPool connectionPool, StoreManager storeManager, ClusterMap clusterMap)
      throws Exception {
    this.connectionPool = connectionPool;
    this.clusterMap = clusterMap;
    this.storeManager = storeManager;
  }

  @Override
  public FileCopyHandler getFileCopyHandler() {
    return new StoreFileCopyHandler(connectionPool, storeManager, clusterMap);
  }
}
