package com.github.ambry.cloud;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.ServerConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.CompositeNetworkClientFactory;
import com.github.ambry.replication.ReplicationEngine;
import com.github.ambry.replication.ReplicationException;
import com.github.ambry.store.StorageManager;
import com.github.ambry.store.StoreKeyConverterFactory;
import com.github.ambry.store.StoreKeyFactory;
import java.util.Collections;


public class BackupIntegrityMonitor extends ReplicationEngine {


  public BackupIntegrityMonitor(VerifiableProperties properties, ClusterMap clusterMap,
      CompositeNetworkClientFactory clientFactory, StorageManager localStorageManager,
      StoreKeyFactory keyFactory, StoreKeyConverterFactory keyConverterFactory) throws ReplicationException {
    super(new ReplicationConfig(properties), new ClusterMapConfig(properties), new StoreConfig(properties), keyFactory,
        clusterMap, null, clusterMap.getDataNodeId(new NetworkConfig(properties).hostName, new NetworkConfig(properties).port),
        Collections.emptyList(), clientFactory, clusterMap.getMetricRegistry(), null,
        keyConverterFactory, new ServerConfig(properties).serverMessageTransformer, null,
        localStorageManager, null, false);
  }

  @Override
  public void start() {

  }

  @Override
  protected boolean shouldReplicateFromDc(String datacenterName) {
    return false;
  }
}
