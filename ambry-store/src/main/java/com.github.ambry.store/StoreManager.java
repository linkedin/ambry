package com.github.ambry.store;

import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.metrics.ReadableMetricsRegistry;
import com.github.ambry.utils.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The store manager that handles all the stores
 */
public class StoreManager {

  private StoreConfig config;
  private Scheduler scheduler;
  private ReadableMetricsRegistry registry;
  private List<ReplicaId> replicas;
  private ConcurrentMap<PartitionId, Store> stores;
  private Logger logger = LoggerFactory.getLogger(getClass());
  private StoreKeyFactory factory;
  private MessageRecovery recovery;

  public StoreManager(StoreConfig config,
                      Scheduler scheduler,
                      ReadableMetricsRegistry registry,
                      List<ReplicaId> replicas,
                      StoreKeyFactory factory,
                      MessageRecovery recovery) {
    this.config = config;
    this.scheduler = scheduler;
    this.registry = registry;
    this.replicas = replicas;
    this.stores = new ConcurrentHashMap<PartitionId, Store>();
    this.factory = factory;
    this.recovery = recovery;
  }

  public void start() throws StoreException {
    logger.info("Starting store manager");
    // iterate through the replicas for this node and create the stores
    for (ReplicaId replica : replicas) {
      // check if mount path exist
      File file = new File(replica.getMountPath());
      if (!file.exists()) {
        throw new IllegalStateException("Mount path does not exist " + replica.getMountPath());
      }
      Store store = new BlobStore(config, scheduler, registry, replica.getReplicaPath(), replica.getCapacityGB(), factory, recovery);
      store.start();
      stores.put(replica.getPartitionId(), store);
    }
    logger.info("Starting store manager complete");
  }

  public Store getStore(PartitionId id) {
    return stores.get(id);
  }

  public void shutdown() throws StoreException {
    logger.info("Shutting down store manager");
    for (Map.Entry<PartitionId, Store> entry : stores.entrySet()) {
      entry.getValue().shutdown();
    }
    logger.info("Shutting down store manager complete");
  }
}
