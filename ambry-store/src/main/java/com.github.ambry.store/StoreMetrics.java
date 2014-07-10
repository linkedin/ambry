package com.github.ambry.store;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;


/**
 * Metrics for the store
 */
public class StoreMetrics {

  public final Timer getResponse;
  public final Timer putResponse;
  public final Timer deleteResponse;
  public final Timer findEntriesSinceResponse;
  public final Timer findMissingKeysResponse;
  public final Timer isKeyDeletedResponse;
  public final Timer storeStartTime;
  public final Counter overflowWriteError;
  public final Counter overflowReadError;
  public final Timer recoveryTime;
  public final Timer findTime;
  public final Timer indexFlushTime;
  public final Counter nonzeroMessageRecovery;
  public final Counter bloomPositiveCount;
  public final Counter bloomFalsePositiveCount;
  public Gauge<Long> currentCapacityUsed;
  public Gauge<Double> ratioOfCurrentToTotalCapacity;
  private final MetricRegistry registry;
  private final String name;

  public StoreMetrics(String name, MetricRegistry registry) {
    this.registry = registry;
    this.name = name;
    getResponse = registry.timer(MetricRegistry.name(BlobStore.class, name + "-storeGetResponse"));
    putResponse = registry.timer(MetricRegistry.name(BlobStore.class, name + "-storePutResponse"));
    deleteResponse = registry.timer(MetricRegistry.name(BlobStore.class, name + "-storeDeleteResponse"));
    findEntriesSinceResponse =
        registry.timer(MetricRegistry.name(BlobStore.class, name + "-storeFindEntriesSinceResponse"));
    findMissingKeysResponse =
        registry.timer(MetricRegistry.name(BlobStore.class, name + "-storeFindMissingKeyResponse"));
    isKeyDeletedResponse = registry.timer(MetricRegistry.name(BlobStore.class, name + "-isKeyDeletedResponse"));
    storeStartTime = registry.timer(MetricRegistry.name(BlobStore.class, name + "-storeStartTime"));
    overflowWriteError = registry.counter(MetricRegistry.name(Log.class, name + "-overflowWriteError"));
    overflowReadError = registry.counter(MetricRegistry.name(Log.class, name + "-overflowReadError"));
    recoveryTime = registry.timer(MetricRegistry.name(PersistentIndex.class, name + "-indexRecoveryTime"));
    findTime = registry.timer(MetricRegistry.name(PersistentIndex.class, name + "-indexFindTime"));
    indexFlushTime = registry.timer(MetricRegistry.name(PersistentIndex.class, name + "-indexFlushTime"));
    nonzeroMessageRecovery =
        registry.counter(MetricRegistry.name(PersistentIndex.class, name + "-nonZeroMessageRecovery"));
    bloomPositiveCount = registry.counter(MetricRegistry.name(IndexSegment.class, name + "-bloomPositiveCount"));
    bloomFalsePositiveCount =
        registry.counter(MetricRegistry.name(IndexSegment.class, name + "-bloomFalsePositiveCount"));
  }

  public void initializeCapacityUsedMetric(final Log log, final long capacityInBytes) {
    currentCapacityUsed = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return log.getLogEndOffset();
      }
    };
    registry.register(MetricRegistry.name(Log.class, name + "-currentCapacityUsed"), currentCapacityUsed);
    ratioOfCurrentToTotalCapacity = new Gauge<Double>() {
      @Override
      public Double getValue() {
        return ((double)log.getLogEndOffset()/capacityInBytes);
      }
    };
    registry.register(MetricRegistry.name(Log.class, name + "-ratioOfCurrentToTotalCapacity"), currentCapacityUsed);
  }
}
