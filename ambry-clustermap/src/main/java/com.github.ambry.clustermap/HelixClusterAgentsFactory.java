/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.clustermap;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.HelixPropertyStoreConfig;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.store.zk.ZkHelixPropertyStore;


/**
 * A factory class to construct {@link HelixClusterManager} and {@link HelixParticipant}. Only one instance of each
 * type of objects will ever be created by this factory.
 */
public class HelixClusterAgentsFactory implements ClusterAgentsFactory {
  private final ClusterMapConfig clusterMapConfig;
  private final HelixPropertyStoreConfig helixPropertyStoreConfig;
  private final String instanceName;
  private final HelixFactory helixFactory;
  private final MetricRegistry metricRegistry;
  private HelixClusterManager helixClusterManager;
  private HelixPropertyStore<ZNRecord> helixPropertyStore;
  private HelixParticipant helixParticipant;

  /**
   * Construct an object of this factory.
   * @param clusterMapConfig the {@link ClusterMapConfig} associated with this factory.
   * @param helixPropertyStoreConfig the {@link HelixPropertyStoreConfig} associated with this factory.
   * @param hardwareLayoutFilePath unused.
   * @param partitionLayoutFilePath unused.
   */
  public HelixClusterAgentsFactory(ClusterMapConfig clusterMapConfig, HelixPropertyStoreConfig helixPropertyStoreConfig, String hardwareLayoutFilePath,
      String partitionLayoutFilePath) {
    this(clusterMapConfig, helixPropertyStoreConfig, new MetricRegistry());
  }

  HelixClusterAgentsFactory(ClusterMapConfig clusterMapConfig, HelixPropertyStoreConfig helixPropertyStoreConfig, MetricRegistry metricRegistry) {
    this.clusterMapConfig = clusterMapConfig;
    this.helixPropertyStoreConfig = helixPropertyStoreConfig;
    this.instanceName =
        ClusterMapUtils.getInstanceName(clusterMapConfig.clusterMapHostName, clusterMapConfig.clusterMapPort);
    helixFactory = new HelixFactory();
    this.metricRegistry = metricRegistry;
  }

  @Override
  public HelixClusterManager getClusterMap() throws IOException {
    if (helixClusterManager == null) {
      ZkClient zkClient =
          new ZkClient(helixPropertyStoreConfig.zkClientConnectString, helixPropertyStoreConfig.zkClientSessionTimeoutMs,
              helixPropertyStoreConfig.zkClientConnectionTimeoutMs, new ZNRecordSerializer());
      List<String> subscribedPaths = Collections.singletonList(helixPropertyStoreConfig.rootPath);
      helixPropertyStore =
          new ZkHelixPropertyStore<>(new ZkBaseDataAccessor<>(zkClient), helixPropertyStoreConfig.rootPath, subscribedPaths);
      helixClusterManager = new HelixClusterManager(clusterMapConfig, helixPropertyStore, instanceName, helixFactory, metricRegistry);
    }
    return helixClusterManager;
  }

  @Override
  public HelixParticipant getClusterParticipant() throws IOException {
    if (helixParticipant == null) {
      helixParticipant = new HelixParticipant(clusterMapConfig, helixFactory);
    }
    return helixParticipant;
  }
}

