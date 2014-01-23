package com.github.ambry.store;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

/**
 * Metrics for the store
 */
public class StoreMetrics {

  public final Timer getResponse;
  public final Timer putResponse;
  public final Timer deleteResponse;
  public final Timer ttlResponse;
  public final Timer storeStartTime;
  public final Counter overflowWriteError;
  public final Counter overflowReadError;
  public final Timer recoveryTime;
  public final Timer findTime;
  public final Timer indexFlushTime;
  public final Counter nonzeroMessageRecovery;
  public final Counter bloomPositiveCount;
  public final Counter bloomFalsePositiveCount;

  public StoreMetrics(String name, MetricRegistry registry) {
    getResponse = registry.timer(MetricRegistry.name(BlobStore.class, name + "storeGetResponse"));
    putResponse = registry.timer(MetricRegistry.name(BlobStore.class, name + "storePutResponse"));
    deleteResponse = registry.timer(MetricRegistry.name(BlobStore.class, name + "storeDeleteResponse"));
    ttlResponse = registry.timer(MetricRegistry.name(BlobStore.class, name + "storeTTLResponse"));
    storeStartTime = registry.timer(MetricRegistry.name(BlobStore.class, name + "storeStartTime"));
    overflowWriteError = registry.counter(MetricRegistry.name(Log.class, name + "overflowWriteError"));
    overflowReadError = registry.counter(MetricRegistry.name(Log.class, name + "overflowReadError"));
    recoveryTime = registry.timer(MetricRegistry.name(BlobPersistentIndex.class, name + "indexRecoveryTime"));
    findTime = registry.timer(MetricRegistry.name(BlobPersistentIndex.class, name + "indexFindTime"));
    indexFlushTime = registry.timer(MetricRegistry.name(BlobPersistentIndex.class, name + "indexFlushTime"));
    nonzeroMessageRecovery = registry.counter(MetricRegistry.name(BlobPersistentIndex.class, name + "nonZeroMessageRecovery"));
    bloomPositiveCount = registry.counter(MetricRegistry.name(IndexSegmentInfo.class, name + "bloomPositiveCount"));
    bloomFalsePositiveCount = registry.counter(MetricRegistry.name(IndexSegmentInfo.class, name + "bloomFalsePositiveCount"));
  }
}
