package com.github.ambry.frontend;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.rest.RestRequestMetrics;


/**
 * Ambry frontend specific metrics tracking.
 * <p/>
 * Exports metrics that are triggered by the Ambry frontend to the provided {@link MetricRegistry}.
 */
class FrontendMetrics {

  // RestRequestMetrics instances
  // DELETE
  public final RestRequestMetrics deleteBlobMetrics;
  // HEAD
  public final RestRequestMetrics headBlobMetrics;
  // GET
  public final RestRequestMetrics getBlobInfoMetrics;
  public final RestRequestMetrics getBlobMetrics;
  public final RestRequestMetrics getUserMetadataMetrics;
  // POST
  public final RestRequestMetrics postBlobMetrics;

  // Rates
  // AmbryBlobStorageService
  // DELETE
  public final Meter deleteBlobRate;
  // HEAD
  public final Meter headBlobRate;
  // GET
  public final Meter getBlobRate;
  // POST
  public final Meter postBlobRate;

  // Latencies
  // AmbryBlobStorageService
  // DELETE
  public final Histogram deletePreProcessingTimeInMs;
  // HEAD
  public final Histogram headPreProcessingTimeInMs;
  // GET
  public final Histogram getPreProcessingTimeInMs;
  // POST
  public final Histogram blobPropsBuildTimeInMs;
  public final Histogram postPreProcessingTimeInMs;
  // DeleteCallback
  public final Histogram deleteCallbackProcessingTimeInMs;
  public final Histogram deleteTimeInMs;
  // HeadCallback
  public final Histogram headCallbackProcessingTimeInMs;
  public final Histogram headTimeInMs;
  // HeadForGetCallback
  public final Histogram headForGetCallbackProcessingTimeInMs;
  public final Histogram headForGetTimeInMs;
  // GetCallback
  public final Histogram getCallbackProcessingTimeInMs;
  public final Histogram getTimeInMs;
  // PostCallback
  public final Histogram postCallbackProcessingTimeInMs;
  public final Histogram postTimeInMs;

  // Errors
  // AmbryBlobStorageService
  public final Counter callbackProcessingError;
  public final Counter operationError;
  public final Counter responseSubmissionError;
  public final Counter resourceReleaseError;

  /**
   * Creates an instance of FrontendMetrics using the given {@code metricRegistry}.
   * @param metricRegistry the {@link MetricRegistry} to use for the metrics.
   */
  public FrontendMetrics(MetricRegistry metricRegistry) {
    // RestRequestMetrics instances
    // DELETE
    deleteBlobMetrics = new RestRequestMetrics(AmbryBlobStorageService.class, "DeleteBlob", metricRegistry);
    // HEAD
    headBlobMetrics = new RestRequestMetrics(AmbryBlobStorageService.class, "HeadBlob", metricRegistry);
    // GET
    getBlobInfoMetrics = new RestRequestMetrics(AmbryBlobStorageService.class, "GetBlobInfo", metricRegistry);
    getBlobMetrics = new RestRequestMetrics(AmbryBlobStorageService.class, "GetBlob", metricRegistry);
    getUserMetadataMetrics = new RestRequestMetrics(AmbryBlobStorageService.class, "GetUserMetadata", metricRegistry);
    // POST
    postBlobMetrics = new RestRequestMetrics(AmbryBlobStorageService.class, "PostBlob", metricRegistry);

    // Rates
    // AmbryBlobStorageService
    // DELETE
    deleteBlobRate = metricRegistry.meter(MetricRegistry.name(AmbryBlobStorageService.class, "DeleteBlobRate"));
    // HEAD
    headBlobRate = metricRegistry.meter(MetricRegistry.name(AmbryBlobStorageService.class, "HeadBlobRate"));
    // GET
    getBlobRate = metricRegistry.meter(MetricRegistry.name(AmbryBlobStorageService.class, "GetBlobRate"));
    // POST
    postBlobRate = metricRegistry.meter(MetricRegistry.name(AmbryBlobStorageService.class, "PostBlobRate"));

    // Latencies
    // AmbryBlobStorageService
    // DELETE
    deletePreProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "DeletePreProcessingTimeInMs"));
    // HEAD
    headPreProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "HeadPreProcessingTimeInMs"));
    // GET
    getPreProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "GetPreProcessingTimeInMs"));
    // POST
    blobPropsBuildTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "BlobPropsBuildTimeInMs"));
    postPreProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AmbryBlobStorageService.class, "PostPreProcessingTimeInMs"));
    // DeleteCallback
    deleteCallbackProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(DeleteCallback.class, "ProcessingTimeInMs"));
    deleteTimeInMs = metricRegistry.histogram(MetricRegistry.name(DeleteCallback.class, "ResultTimeInMs"));
    // HeadCallback
    headCallbackProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(HeadCallback.class, "ProcessingTimeInMs"));
    headTimeInMs = metricRegistry.histogram(MetricRegistry.name(HeadCallback.class, "ResultTimeInMs"));
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

    // Errors
    // AmbryBlobStorageService
    callbackProcessingError =
        metricRegistry.counter(MetricRegistry.name(AmbryBlobStorageService.class, "CallbackProcessingError"));
    operationError = metricRegistry.counter(MetricRegistry.name(AmbryBlobStorageService.class, "OperationError"));
    responseSubmissionError =
        metricRegistry.counter(MetricRegistry.name(AmbryBlobStorageService.class, "ResponseSubmissionError"));
    resourceReleaseError =
        metricRegistry.counter(MetricRegistry.name(AmbryBlobStorageService.class, "ResourceReleaseError"));
  }
}
