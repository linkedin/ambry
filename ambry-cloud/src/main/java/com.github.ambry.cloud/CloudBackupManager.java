/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.VirtualReplicatorCluster;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.replication.ReplicationEngine;
import com.github.ambry.replication.ReplicationException;
import com.github.ambry.store.StoreKeyConverterFactory;
import com.github.ambry.store.StoreKeyFactory;
import java.util.concurrent.ScheduledExecutorService;


/**
 * {@link CloudBackupManager} is used to backup partitions to Cloud. Partitions assignment is handled by Helix.
 */
public class CloudBackupManager extends ReplicationEngine {

  public CloudBackupManager(CloudConfig cloudConfig, ReplicationConfig replicationConfig,
      ClusterMapConfig clusterMapConfig, StoreConfig storeConfig, StoreKeyFactory storeKeyFactory,
      ClusterMap clusterMap, VirtualReplicatorCluster virtualReplicatorCluster, ScheduledExecutorService scheduler,
      ConnectionPool connectionPool, MetricRegistry metricRegistry, NotificationSystem requestNotification,
      StoreKeyConverterFactory storeKeyConverterFactory, String transformerClassName) throws ReplicationException {

    super(replicationConfig, clusterMapConfig, storeKeyFactory, clusterMap, scheduler,
        virtualReplicatorCluster.getCurrentDataNodeId(), connectionPool, metricRegistry, requestNotification,
        storeKeyConverterFactory, transformerClassName);
  }
}
