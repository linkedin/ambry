package com.github.ambry.admin;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;


/**
 * Admin specific metrics tracking.
 * <p/>
 * Exports metrics that are triggered by the admin to the provided {@link MetricRegistry}.
 */
class AdminMetrics {

  // Rates
  // AdminBlobStorageService
  public final Meter getOperationRate;
  public final Meter postOperationRate;
  public final Meter deleteOperationRate;
  public final Meter headOperationRate;
  // EchoHandler
  public final Meter echoRate;
  // GetReplicasForBlobIdHandler
  public final Meter getReplicasForBlobIdRate;

  // Latencies
  // EchoHandler
  public final Histogram echoProcessingTimeInMs;
  // GetReplicasForBlobIdHandler
  public final Histogram getReplicasForBlobIdProcessingTimeInMs;

  // Errors
  // AdminBlobStorageService
  public final Counter unsupportedGetOperationError;
  public final Counter unsupportedPostOperationError;
  public final Counter unsupportedDeleteOperationError;
  public final Counter unsupportedHeadOperationError;
  // EchoHandler
  public final Counter echoGetMissingParameterError;
  public final Counter echoGetResponseBuildingError;
  // GetReplicasForBlobIdHandler
  public final Counter getReplicasForBlobIdInvalidBlobIdError;
  public final Counter getReplicasForBlobIdObjectCreationError;
  public final Counter getReplicasForBlobIdMissingParameterError;
  public final Counter getReplicasForBlobIdPartitionNullError;
  public final Counter getReplicasForBlobIdResponseBuildingError;

  public AdminMetrics(MetricRegistry metricRegistry) {

    getOperationRate = metricRegistry.meter(MetricRegistry.name(AdminBlobStorageService.class, "GetOperationRate"));
    postOperationRate = metricRegistry.meter(MetricRegistry.name(AdminBlobStorageService.class, "PostOperationRate"));
    deleteOperationRate =
        metricRegistry.meter(MetricRegistry.name(AdminBlobStorageService.class, "DeleteOperationRate"));
    headOperationRate = metricRegistry.meter(MetricRegistry.name(AdminBlobStorageService.class, "HeadOperationRate"));
    echoRate = metricRegistry.meter(MetricRegistry.name(EchoHandler.class, "EchoRate"));
    getReplicasForBlobIdRate =
        metricRegistry.meter(MetricRegistry.name(GetReplicasForBlobIdHandler.class, "GetReplicasForBlobIdRate"));

    echoProcessingTimeInMs = metricRegistry.histogram(MetricRegistry.name(EchoHandler.class, "ProcessingTimeInMs"));
    getReplicasForBlobIdProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(GetReplicasForBlobIdHandler.class, "ProcessingTimeInMs"));

    unsupportedGetOperationError =
        metricRegistry.counter(MetricRegistry.name(AdminBlobStorageService.class, "UnsupportedGetOperationError"));
    unsupportedPostOperationError =
        metricRegistry.counter(MetricRegistry.name(AdminBlobStorageService.class, "UnsupportedPostOperationError"));
    unsupportedDeleteOperationError =
        metricRegistry.counter(MetricRegistry.name(AdminBlobStorageService.class, "UnsupportedDeleteOperationError"));
    unsupportedHeadOperationError =
        metricRegistry.counter(MetricRegistry.name(AdminBlobStorageService.class, "UnsupportedHeadOperationError"));
    echoGetMissingParameterError =
        metricRegistry.counter(MetricRegistry.name(EchoHandler.class, "MissingParameterError"));
    echoGetResponseBuildingError =
        metricRegistry.counter(MetricRegistry.name(EchoHandler.class, "ResponseBuildingError"));
    getReplicasForBlobIdInvalidBlobIdError =
        metricRegistry.counter(MetricRegistry.name(GetReplicasForBlobIdHandler.class, "InvalidBlobIdError"));
    getReplicasForBlobIdObjectCreationError =
        metricRegistry.counter(MetricRegistry.name(GetReplicasForBlobIdHandler.class, "ObjectCreationError"));
    getReplicasForBlobIdMissingParameterError =
        metricRegistry.counter(MetricRegistry.name(GetReplicasForBlobIdHandler.class, "MissingParameterError"));
    getReplicasForBlobIdPartitionNullError =
        metricRegistry.counter(MetricRegistry.name(GetReplicasForBlobIdHandler.class, "PartitionNullError"));
    getReplicasForBlobIdResponseBuildingError =
        metricRegistry.counter(MetricRegistry.name(GetReplicasForBlobIdHandler.class, "ResponseBuildingError"));
  }
}
