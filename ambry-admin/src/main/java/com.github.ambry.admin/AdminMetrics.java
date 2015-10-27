package com.github.ambry.admin;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.rest.RestRequestMetrics;


/**
 * Admin specific metrics tracking.
 * <p/>
 * Exports metrics that are triggered by the admin to the provided {@link MetricRegistry}.
 */
class AdminMetrics {

  // RequestMetricsTracker instances
  // GET
  public final RestRequestMetrics.RequestMetricsTracker echoTracker;
  public final RestRequestMetrics.RequestMetricsTracker getBlobTracker;
  public final RestRequestMetrics.RequestMetricsTracker getReplicasForBlobIdTracker;
  // POST
  public final RestRequestMetrics.RequestMetricsTracker postBlobTracker;
  // DELETE
  public final RestRequestMetrics.RequestMetricsTracker deleteBlobTracker;
  // HEAD
  public final RestRequestMetrics.RequestMetricsTracker headBlobTracker;

  // Rates
  // AdminBlobStorageService
  // GET
  public final Meter echoRate;
  public final Meter getBlobRate;
  public final Meter getReplicasForBlobIdRate;
  // POST
  public final Meter postBlobRate;
  // DELETE
  public final Meter deleteBlobRate;
  // HEAD
  public final Meter headBlobRate;

  // Latencies
  // AdminBlobStorageService
  public final Histogram blobPropsBuildTimeInMs;
  // HeadForGetCallback
  public final Histogram headForGetCallbackProcessingTimeInMs;
  public final Histogram headForGetTimeInMs;
  // GetCallback
  public final Histogram getCallbackProcessingTimeInMs;
  public final Histogram getTimeInMs;
  // PostCallback
  public final Histogram postCallbackProcessingTimeInMs;
  public final Histogram postTimeInMs;
  // DeleteCallback
  public final Histogram deleteCallbackProcessingTimeInMs;
  public final Histogram deleteTimeInMs;
  // HeadCallback
  public final Histogram headCallbackProcessingTimeInMs;
  public final Histogram headTimeInMs;
  // EchoHandler
  public final Histogram echoProcessingTimeInMs;
  // GetReplicasForBlobIdHandler
  public final Histogram getReplicasForBlobIdProcessingTimeInMs;

  // Errors
  // AdminBlobStorageService
  public final Counter callbackProcessingError;
  public final Counter operationError;
  public final Counter responseSubmissionError;
  public final Counter resourceReleaseError;
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
    // RequestMetricsTracker instances
    // GET
    echoTracker = new RestRequestMetrics.RequestMetricsTracker(AdminBlobStorageService.class, "Echo", metricRegistry);
    getBlobTracker =
        new RestRequestMetrics.RequestMetricsTracker(AdminBlobStorageService.class, "GetBlob", metricRegistry);
    getReplicasForBlobIdTracker =
        new RestRequestMetrics.RequestMetricsTracker(AdminBlobStorageService.class, "GetReplicasForBlobId",
            metricRegistry);
    // POST
    postBlobTracker =
        new RestRequestMetrics.RequestMetricsTracker(AdminBlobStorageService.class, "PostBlob", metricRegistry);
    // DELETE
    deleteBlobTracker =
        new RestRequestMetrics.RequestMetricsTracker(AdminBlobStorageService.class, "DeleteBlob", metricRegistry);
    // HEAD
    headBlobTracker =
        new RestRequestMetrics.RequestMetricsTracker(AdminBlobStorageService.class, "HeadBlob", metricRegistry);

    // Rates
    // AdminBlobStorageService
    // GET
    echoRate = metricRegistry.meter(MetricRegistry.name(AdminBlobStorageService.class, "EchoRate"));
    getBlobRate = metricRegistry.meter(MetricRegistry.name(AdminBlobStorageService.class, "GetBlobRate"));
    getReplicasForBlobIdRate =
        metricRegistry.meter(MetricRegistry.name(AdminBlobStorageService.class, "GetReplicasForBlobIdRate"));
    // POST
    postBlobRate = metricRegistry.meter(MetricRegistry.name(AdminBlobStorageService.class, "PostBlobRate"));
    // DELETE
    deleteBlobRate = metricRegistry.meter(MetricRegistry.name(AdminBlobStorageService.class, "DeleteBlobRate"));
    // HEAD
    headBlobRate = metricRegistry.meter(MetricRegistry.name(AdminBlobStorageService.class, "HeadBlobRate"));

    // Latencies
    // AdminBlobStorageService
    blobPropsBuildTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AdminBlobStorageService.class, "BlobPropsBuildTimeInMs"));
    // HeadForGetCallback
    headForGetCallbackProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(HeadForGetCallback.class, "ProcessingTimeInMs"));
    headForGetTimeInMs = metricRegistry.histogram(MetricRegistry.name(HeadForGetCallback.class, "ResultTimeInMs"));
    // GetCallback
    getCallbackProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(GetCallback.class, "ProcessingTimeInMs"));
    getTimeInMs = metricRegistry.histogram(MetricRegistry.name(GetCallback.class, "ResultTimeInMs"));
    // PostCallback
    postCallbackProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(PostCallback.class, "ProcessingTimeInMs"));
    postTimeInMs = metricRegistry.histogram(MetricRegistry.name(PostCallback.class, "ResultTimeInMs"));
    // DeleteCallback
    deleteCallbackProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(DeleteCallback.class, "ProcessingTimeInMs"));
    deleteTimeInMs = metricRegistry.histogram(MetricRegistry.name(DeleteCallback.class, "ResultTimeInMs"));
    // HeadCallback
    headCallbackProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(HeadCallback.class, "ProcessingTimeInMs"));
    headTimeInMs = metricRegistry.histogram(MetricRegistry.name(HeadCallback.class, "ResultTimeInMs"));
    // EchoHandler
    echoProcessingTimeInMs = metricRegistry.histogram(MetricRegistry.name(EchoHandler.class, "ProcessingTimeInMs"));
    // GetReplicasForBlobIdHandler
    getReplicasForBlobIdProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(GetReplicasForBlobIdHandler.class, "ProcessingTimeInMs"));

    // Errors
    // AdminBlobStorageService
    callbackProcessingError = metricRegistry.counter(MetricRegistry.name(EchoHandler.class, "CallbackProcessingError"));
    operationError = metricRegistry.counter(MetricRegistry.name(EchoHandler.class, "OperationError"));
    responseSubmissionError = metricRegistry.counter(MetricRegistry.name(EchoHandler.class, "ResponseSubmissionError"));
    resourceReleaseError = metricRegistry.counter(MetricRegistry.name(EchoHandler.class, "ResourceReleaseError"));
    // EchoHandler
    echoGetMissingParameterError =
        metricRegistry.counter(MetricRegistry.name(EchoHandler.class, "MissingParameterError"));
    echoGetResponseBuildingError =
        metricRegistry.counter(MetricRegistry.name(EchoHandler.class, "ResponseBuildingError"));
    // GetReplicasForBlobIdHandler
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
