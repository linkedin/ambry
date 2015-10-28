package com.github.ambry.router;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;


/**
 * {@link CoordinatorBackedRouter} specific metrics tracking.
 * <p/>
 * Exports metrics that are triggered by the admin to the provided {@link MetricRegistry}.
 */
public class CoordinatorBackedRouterMetrics {

  // Rates
  public final Meter deleteBlobRate;
  public final Meter getBlobInfoRate;
  public final Meter getBlobRate;
  public final Meter putBlobRate;

  // Latencies
  public final Histogram operationQueueingTimeInMs;
  // GetBlobInfo
  public final Histogram getBlobInfoResultConstructionTimeInMs;
  public final Histogram getBlobInfoTotalTimeInMs;
  public final Histogram getBlobPropertiesTimeInMs;
  public final Histogram getUserMetadataTimeInMs;
  // GetBlob
  public final Histogram getBlobResultConstructionTimeInMs;
  public final Histogram getBlobTimeInMs;
  public final Histogram getBlobTotalTimeInMs;
  // PutBlob
  public final Histogram putBlobTotalTimeInMs;
  // DeleteBlob
  public final Histogram deleteBlobTotalTimeInMs;

  // Errors
  public final Counter operationError;
  public final Counter closeError;
  public final Counter unavailableError;

  public CoordinatorBackedRouterMetrics(MetricRegistry metricRegistry) {
    // Rates
    deleteBlobRate = metricRegistry.meter(MetricRegistry.name(CoordinatorBackedRouter.class, "DeleteBlobRate"));
    getBlobInfoRate = metricRegistry.meter(MetricRegistry.name(CoordinatorBackedRouter.class, "GetBlobInfoRate"));
    getBlobRate = metricRegistry.meter(MetricRegistry.name(CoordinatorBackedRouter.class, "GetBlobRate"));
    putBlobRate = metricRegistry.meter(MetricRegistry.name(CoordinatorBackedRouter.class, "PutBlobRate"));

    // Latencies
    operationQueueingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(CoordinatorOperation.class, "OperationQueueingTimeInMs"));
    // GetBlobInfo
    getBlobInfoResultConstructionTimeInMs = metricRegistry
        .histogram(MetricRegistry.name(CoordinatorOperation.class, "GetBlobInfoResultConstructionTimeInMs"));
    getBlobInfoTotalTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(CoordinatorOperation.class, "GetBlobInfoTotalTimeInMs"));
    getBlobPropertiesTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(CoordinatorOperation.class, "GetBlobPropertiesTimeInMs"));
    getUserMetadataTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(CoordinatorOperation.class, "GetUserMetadataTimeInMs"));
    // GetBlob
    getBlobResultConstructionTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(CoordinatorOperation.class, "GetBlobResultConstructionTimeInMs"));
    getBlobTimeInMs = metricRegistry.histogram(MetricRegistry.name(CoordinatorOperation.class, "GetBlobTimeInMs"));
    getBlobTotalTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(CoordinatorOperation.class, "GetBlobTotalTimeInMs"));
    // PutBlob
    putBlobTotalTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(CoordinatorOperation.class, "PutBlobTotalTimeInMs"));
    // DeleteBlob
    deleteBlobTotalTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(CoordinatorOperation.class, "DeleteBlobTotalTimeInMs"));

    // Errors
    operationError = metricRegistry.counter(MetricRegistry.name(CoordinatorOperation.class, "OperationError"));
    closeError = metricRegistry.counter(MetricRegistry.name(CoordinatorBackedRouter.class, "CloseError"));
    unavailableError = metricRegistry.counter(MetricRegistry.name(CoordinatorBackedRouter.class, "UnavailableError"));
  }
}
