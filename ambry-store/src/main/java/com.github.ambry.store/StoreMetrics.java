package com.github.ambry.store;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
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
  public final Timer cleanupTokenFlushTime;
  public final Timer hardDeleteTime;
  public final Counter nonzeroMessageRecovery;
  public final Counter bloomPositiveCount;
  public final Counter bloomFalsePositiveCount;
  public final Counter keySizeMismatchCount;
  public final Counter cleanupDoneCount;
  public final Counter cleanupFailedCount;
  public Gauge<Long> currentCapacityUsed;
  public Gauge<Long> currentHardDeleteProgress;
  public final Histogram segmentSizeForExists;
  public Gauge<Double> percentageUsedCapacity;
  public Gauge<Double> percentageHardDeleteCompleted;
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
    cleanupTokenFlushTime = registry.timer(MetricRegistry.name(PersistentIndex.class, name + "-cleanupTokenFlushTime"));
    hardDeleteTime = registry.timer(MetricRegistry.name(PersistentIndex.class, name + "-hardDeleteTime"));
    nonzeroMessageRecovery =
        registry.counter(MetricRegistry.name(PersistentIndex.class, name + "-nonZeroMessageRecovery"));
    bloomPositiveCount = registry.counter(MetricRegistry.name(IndexSegment.class, name + "-bloomPositiveCount"));
    bloomFalsePositiveCount =
        registry.counter(MetricRegistry.name(IndexSegment.class, name + "-bloomFalsePositiveCount"));
    keySizeMismatchCount = registry.counter(MetricRegistry.name(IndexSegment.class, name + "-keySizeMismatchCount"));
    cleanupDoneCount = registry.counter(MetricRegistry.name(PersistentIndex.class, name + "-cleanupDoneCount"));
    cleanupFailedCount = registry.counter(MetricRegistry.name(PersistentIndex.class, name + "-cleanupFailedCount"));
    segmentSizeForExists = registry.histogram(MetricRegistry.name(IndexSegment.class, name + "-segmentSizeForExists"));
  }

  public void initializeCapacityUsedMetric(final Log log, final long capacityInBytes) {
    currentCapacityUsed = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return log.getLogEndOffset();
      }
    };
    registry.register(MetricRegistry.name(Log.class, name + "-currentCapacityUsed"), currentCapacityUsed);
    percentageUsedCapacity = new Gauge<Double>() {
      @Override
      public Double getValue() {
        return ((double) log.getLogEndOffset() / capacityInBytes) * 100;
      }
    };
    registry.register(MetricRegistry.name(Log.class, name + "-percentageUsedCapacity"), percentageUsedCapacity);
  }

  public void initializeHardDeleteProgressMetric(final PersistentIndex index, final Log log) {
    currentHardDeleteProgress = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return index.getHardDeleteProgress();
      }
    };
    registry.register(MetricRegistry.name(PersistentIndex.class, name + "-currentHardDeleteProgress"),
        currentHardDeleteProgress);
    percentageHardDeleteCompleted = new Gauge<Double>() {
      @Override
      public Double getValue() {
        return ((double) index.getHardDeleteProgress() / log.getLogEndOffset()) * 100;
      }
    };
    registry.register(MetricRegistry.name(Log.class, name + "-percentageHardDeleteCompleted"), percentageHardDeleteCompleted);
  }
}
