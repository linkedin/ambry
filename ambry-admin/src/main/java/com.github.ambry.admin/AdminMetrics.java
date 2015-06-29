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
  // GetReplicasForBlobIdHandler
  public final Histogram getReplicasForBlobIdTimeInMs;

  // Errors
  // AdminBlobStorageService
  public final Counter unsupportedGetOperation;
  public final Counter unsupportedPostOperation;
  public final Counter unsupportedDeleteOperation;
  public final Counter unsupportedHeadOperation;
  // EchoHandler
  public final Counter echoGetMissingParameter;
  public final Counter echoGetResponseBuildingError;
  // GetReplicasForBlobIdHandler
  public final Counter getReplicasForBlobIdInvalidBlobId;
  public final Counter getReplicasForBlobIdObjectCreationError;
  public final Counter getReplicasForBlobIdMissingParameter;
  public final Counter getReplicasForBlobIdResponseBuildingError;

  public AdminMetrics(MetricRegistry metricRegistry) {

    getOperationRate =
        metricRegistry.meter(MetricRegistry.name(AdminBlobStorageService.class, "AdminGetOperationRate"));
    postOperationRate =
        metricRegistry.meter(MetricRegistry.name(AdminBlobStorageService.class, "AdminPostOperationRate"));
    deleteOperationRate =
        metricRegistry.meter(MetricRegistry.name(AdminBlobStorageService.class, "AdminDeleteOperationRate"));
    headOperationRate =
        metricRegistry.meter(MetricRegistry.name(AdminBlobStorageService.class, "AdminHeadOperationRate"));
    echoRate = metricRegistry.meter(MetricRegistry.name(EchoHandler.class, "AdminEchoRate"));
    getReplicasForBlobIdRate =
        metricRegistry.meter(MetricRegistry.name(GetReplicasForBlobIdHandler.class, "AdminGetReplicasForBlobIdRate"));

    getReplicasForBlobIdTimeInMs = metricRegistry
        .histogram(MetricRegistry.name(GetReplicasForBlobIdHandler.class, "AdminGetReplicasForBlobIdTimeInMs"));

    unsupportedGetOperation =
        metricRegistry.counter(MetricRegistry.name(AdminBlobStorageService.class, "AdminUnsupportedGetOperation"));
    unsupportedPostOperation =
        metricRegistry.counter(MetricRegistry.name(AdminBlobStorageService.class, "AdminUnsupportedPostOperation"));
    unsupportedDeleteOperation =
        metricRegistry.counter(MetricRegistry.name(AdminBlobStorageService.class, "AdminUnsupportedDeleteOperation"));
    unsupportedHeadOperation =
        metricRegistry.counter(MetricRegistry.name(AdminBlobStorageService.class, "AdminUnsupportedHeadOperation"));
    echoGetMissingParameter =
        metricRegistry.counter(MetricRegistry.name(EchoHandler.class, "AdminEchoGetMissingParameter"));
    echoGetResponseBuildingError =
        metricRegistry.counter(MetricRegistry.name(EchoHandler.class, "AdminEchoGetResponseBuildingError"));
    getReplicasForBlobIdInvalidBlobId = metricRegistry
        .counter(MetricRegistry.name(GetReplicasForBlobIdHandler.class, "AdminGetReplicasForBlobIdInvalidBlobId"));
    getReplicasForBlobIdObjectCreationError = metricRegistry.counter(
        MetricRegistry.name(GetReplicasForBlobIdHandler.class, "AdminGetReplicasForBlobIdObjectCreationError"));
    getReplicasForBlobIdMissingParameter = metricRegistry
        .counter(MetricRegistry.name(GetReplicasForBlobIdHandler.class, "AdminGetReplicasForBlobIdMissingParameter"));
    getReplicasForBlobIdResponseBuildingError = metricRegistry.counter(
        MetricRegistry.name(GetReplicasForBlobIdHandler.class, "AdminGetReplicasForBlobIdResponseBuildingError"));
  }
}
