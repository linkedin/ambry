/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.utils.Scheduler;
import com.github.ambry.utils.Time;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The store manager that handles all the stores
 */
public class StoreManager {

  private StoreConfig config;
  private Scheduler scheduler;
  private MetricRegistry registry;
  private List<ReplicaId> replicas;
  private ConcurrentMap<PartitionId, Store> stores;
  private Logger logger = LoggerFactory.getLogger(getClass());
  private StoreKeyFactory factory;
  private MessageStoreRecovery recovery;
  private MessageStoreHardDelete hardDelete;
  private Time time;

  public StoreManager(StoreConfig config, Scheduler scheduler, MetricRegistry registry, List<ReplicaId> replicas,
      StoreKeyFactory factory, MessageStoreRecovery recovery, MessageStoreHardDelete hardDelete, Time time)
      throws StoreException {
    this.config = config;
    this.scheduler = scheduler;
    this.registry = registry;
    this.replicas = replicas;
    this.stores = new ConcurrentHashMap<PartitionId, Store>();
    this.factory = factory;
    this.recovery = recovery;
    this.hardDelete = hardDelete;
    this.time = time;
    verifyConfigs();
  }

  private void verifyConfigs()
      throws StoreException {
    /* NOTE: We must ensure that the store never performs hard deletes on the part of the log that is not yet flushed.
       We do this by making sure that the retention period for deleted messages (which determines the end point for hard
       deletes) is always greater than the log flush period. */
    if (config.storeDeletedMessageRetentionDays < config.storeDataFlushIntervalSeconds / Time.SecsPerDay + 1) {
      throw new StoreException("Message retention days must be greater than the store flush interval period",
          StoreErrorCodes.Initialization_Error);
    }
  }

  public void start()
      throws StoreException {
    logger.info("Starting store manager");
    // iterate through the replicas for this node and create the stores
    for (ReplicaId replica : replicas) {
      // check if mount path exist
      File file = new File(replica.getMountPath());
      if (!file.exists()) {
        throw new IllegalStateException("Mount path does not exist " + replica.getMountPath());
      }
      Store store =
          new BlobStore(config, scheduler, registry, replica.getReplicaPath(), replica.getCapacityInBytes(), factory,
              recovery, hardDelete, time);
      store.start();
      stores.put(replica.getPartitionId(), store);
    }
    logger.info("Starting store manager complete");
  }

  public Store getStore(PartitionId id) {
    return stores.get(id);
  }

  public void shutdown()
      throws StoreException {
    logger.info("Shutting down store manager");
    for (Map.Entry<PartitionId, Store> entry : stores.entrySet()) {
      entry.getValue().shutdown();
    }
    logger.info("Shutting down store manager complete");
  }
}
