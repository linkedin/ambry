package com.github.ambry.cloud;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;


public class RecoveryMetrics {
  public final Counter recoveryTokenError;
  public final Counter listBlobsError;
  public final Counter metadataError;
  public final Counter recoveryRequestError;
  public final Meter listBlobsSuccessRate;

  public RecoveryMetrics(MetricRegistry registry) {
    recoveryTokenError = registry.counter(MetricRegistry.name(RecoveryMetrics.class, "RecoveryTokenError"));
    listBlobsError = registry.counter(MetricRegistry.name(RecoveryMetrics.class, "ListBlobsError"));
    metadataError = registry.counter(MetricRegistry.name(RecoveryMetrics.class, "MetadataError"));
    recoveryRequestError = registry.counter(MetricRegistry.name(RecoveryMetrics.class, "RecoveryRequestError"));
    listBlobsSuccessRate = registry.meter(MetricRegistry.name(RecoveryMetrics.class, "ListBlobsSuccessRate"));
  }
}
