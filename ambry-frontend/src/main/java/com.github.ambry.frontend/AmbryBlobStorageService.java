package com.github.ambry.frontend;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.rest.BlobStorageService;
import com.github.ambry.rest.ResponseStatus;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestResponseHandler;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.Callback;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.router.Router;
import com.github.ambry.router.RouterException;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is an Ambry frontend specific implementation of {@link BlobStorageService}.
 * <p/>
 * All the operations that need to be performed by the Ambry frontend are supported here.
 */
class AmbryBlobStorageService implements BlobStorageService {
  protected final FrontendMetrics frontendMetrics;

  private final ClusterMap clusterMap;
  private final RestResponseHandler responseHandler;
  private final Router router;
  private final long cacheValidityInSecs;
  private final Logger logger = LoggerFactory.getLogger(AmbryBlobStorageService.class);

  /**
   * Create a new instance of AmbryBlobStorageService by supplying it with config, metrics, cluster map, a
   * response handler controller and a router.
   * @param frontendConfig the configuration to use in the form of {@link FrontendConfig}.
   * @param frontendMetrics the metrics instance to use in the form of {@link FrontendMetrics}.
   * @param clusterMap the {@link ClusterMap} to be used for operations.
   * @param responseHandler the {@link RestResponseHandler} that can be used to submit responses that need to be sent
   *                        out.
   * @param router the {@link Router} instance to use to perform blob operations.
   */
  public AmbryBlobStorageService(FrontendConfig frontendConfig, FrontendMetrics frontendMetrics, ClusterMap clusterMap,
      RestResponseHandler responseHandler, Router router) {
    this.frontendMetrics = frontendMetrics;
    this.clusterMap = clusterMap;
    this.responseHandler = responseHandler;
    this.router = router;
    cacheValidityInSecs = frontendConfig.frontendCacheValiditySeconds;
    logger.trace("Instantiated AmbryBlobStorageService");
  }

  @Override
  public void start()
      throws InstantiationException {
    logger.info("AmbryBlobStorageService has started");
  }

  @Override
  public void shutdown() {
    logger.info("AmbryBlobStorageService shutdown complete");
  }

  @Override
  public void handleGet(RestRequest restRequest, RestResponseChannel restResponseChannel) {
    long processingStartTime = System.currentTimeMillis();
    long preProcessingTime = 0;
    handlePrechecks(restRequest, restResponseChannel);
    try {
      frontendMetrics.getBlobRate.mark();
      logger.trace("Handling GET request - {}", restRequest.getUri());
      String operationOrBlobId = getOperationOrBlobIdFromUri(restRequest);
      restRequest.getMetricsTracker().injectMetrics(frontendMetrics.getBlobMetrics);
      logger.trace("Forwarding GET of {} to the router", operationOrBlobId);
      preProcessingTime = System.currentTimeMillis() - processingStartTime;
      HeadForGetCallback callback =
          new HeadForGetCallback(this, restRequest, restResponseChannel, router, cacheValidityInSecs);
      router.getBlobInfo(operationOrBlobId, callback);
    } catch (Exception e) {
      frontendMetrics.operationError.inc();
      submitResponse(restRequest, restResponseChannel, null, e);
    } finally {
      frontendMetrics.getPreProcessingTimeInMs.update(preProcessingTime);
      restRequest.getMetricsTracker().addToTotalCpuTime(preProcessingTime);
    }
  }

  @Override
  public void handlePost(RestRequest restRequest, RestResponseChannel restResponseChannel) {
    long processingStartTime = System.currentTimeMillis();
    long preProcessingTime = 0;
    handlePrechecks(restRequest, restResponseChannel);
    frontendMetrics.postBlobRate.mark();
    restRequest.getMetricsTracker().injectMetrics(frontendMetrics.postBlobMetrics);
    try {
      logger.trace("Handling POST request - {}", restRequest.getUri());
      long propsBuildStartTime = System.currentTimeMillis();
      BlobProperties blobProperties = RestUtils.buildBlobProperties(restRequest);
      byte[] usermetadata = RestUtils.buildUsermetadata(restRequest);
      frontendMetrics.blobPropsBuildTimeInMs.update(System.currentTimeMillis() - propsBuildStartTime);
      logger.trace("Blob properties of blob being POSTed - {}", blobProperties);
      logger.trace("Forwarding POST to the router");
      preProcessingTime = System.currentTimeMillis() - processingStartTime;
      PostCallback callback = new PostCallback(this, restRequest, restResponseChannel, blobProperties);
      router.putBlob(blobProperties, usermetadata, restRequest, callback);
    } catch (Exception e) {
      frontendMetrics.operationError.inc();
      submitResponse(restRequest, restResponseChannel, null, e);
    } finally {
      frontendMetrics.postPreProcessingTimeInMs.update(preProcessingTime);
      restRequest.getMetricsTracker().addToTotalCpuTime(preProcessingTime);
    }
  }

  @Override
  public void handleDelete(RestRequest restRequest, RestResponseChannel restResponseChannel) {
    long processingStartTime = System.currentTimeMillis();
    long preProcessingTime = 0;
    handlePrechecks(restRequest, restResponseChannel);
    frontendMetrics.deleteBlobRate.mark();
    restRequest.getMetricsTracker().injectMetrics(frontendMetrics.deleteBlobMetrics);
    try {
      logger.trace("Handling DELETE request - {}", restRequest.getUri());
      String blobId = getOperationOrBlobIdFromUri(restRequest);
      logger.trace("Forwarding DELETE of {} to the router", blobId);
      preProcessingTime = System.currentTimeMillis() - processingStartTime;
      DeleteCallback callback = new DeleteCallback(this, restRequest, restResponseChannel);
      router.deleteBlob(blobId, callback);
    } catch (Exception e) {
      frontendMetrics.operationError.inc();
      submitResponse(restRequest, restResponseChannel, null, e);
    } finally {
      frontendMetrics.deletePreProcessingTimeInMs.update(preProcessingTime);
      restRequest.getMetricsTracker().addToTotalCpuTime(preProcessingTime);
    }
  }

  @Override
  public void handleHead(RestRequest restRequest, RestResponseChannel restResponseChannel) {
    long processingStartTime = System.currentTimeMillis();
    long preProcessingTime = 0;
    handlePrechecks(restRequest, restResponseChannel);
    restRequest.getMetricsTracker().injectMetrics(frontendMetrics.headBlobMetrics);
    try {
      logger.trace("Handling HEAD request - {}", restRequest.getUri());
      String blobId = getOperationOrBlobIdFromUri(restRequest);
      logger.trace("Forwarding HEAD of {} to the router", blobId);
      preProcessingTime = System.currentTimeMillis() - processingStartTime;
      HeadCallback callback = new HeadCallback(this, restRequest, restResponseChannel);
      router.getBlobInfo(blobId, callback);
    } catch (Exception e) {
      frontendMetrics.operationError.inc();
      submitResponse(restRequest, restResponseChannel, null, e);
    } finally {
      frontendMetrics.headPreProcessingTimeInMs.update(preProcessingTime);
      restRequest.getMetricsTracker().addToTotalCpuTime(preProcessingTime);
    }
  }

  /**
   * Submits the  {@code response} (and any {@code exception})for the {@code restRequest} to the
   * {@code responseHandler}.
   * @param restRequest the {@link RestRequest} for which a a {@code response} is ready.
   * @param restResponseChannel the {@link RestResponseChannel} over which the response can be sent.
   * @param response the response in the form of a {@link ReadableStreamChannel}.
   * @param exception any {@link Exception} that occurred during the handling of {@code restRequest}.
   */
  protected void submitResponse(RestRequest restRequest, RestResponseChannel restResponseChannel,
      ReadableStreamChannel response, Exception exception) {
    try {
      if (exception != null && exception instanceof RouterException) {
        exception = new RestServiceException(exception,
            RestServiceErrorCode.getRestServiceErrorCode(((RouterException) exception).getErrorCode()));
      }
      responseHandler.handleResponse(restRequest, restResponseChannel, response, exception);
    } catch (RestServiceException e) {
      frontendMetrics.responseSubmissionError.inc();
      if (exception != null) {
        logger.error("Error submitting response to response handler", e);
      } else {
        exception = e;
      }
      logger.error("Handling of request {} failed", restRequest.getUri(), exception);
      restResponseChannel.onResponseComplete(exception);

      if (response != null) {
        try {
          response.close();
        } catch (IOException ioe) {
          frontendMetrics.resourceReleaseError.inc();
          logger.error("Error closing ReadableStreamChannel", e);
        }
      }
    }
  }

  /**
   * Checks for bad arguments or states.
   * @param restRequest the {@link RestRequest} to use. Cannot be null.
   * @param restResponseChannel the {@link RestResponseChannel} to use. Cannot be null.
   */
  private void handlePrechecks(RestRequest restRequest, RestResponseChannel restResponseChannel) {
    if (restRequest == null || restResponseChannel == null) {
      StringBuilder errorMessage = new StringBuilder("Null arg(s) received -");
      if (restRequest == null) {
        errorMessage.append(" [RestRequest] ");
      }
      if (restResponseChannel == null) {
        errorMessage.append(" [RestResponseChannel] ");
      }
      throw new IllegalArgumentException(errorMessage.toString());
    }
  }

  /**
   * Looks at the URI to determine the type of operation required or the blob ID that an operation needs to be
   * performed on.
   * @param restRequest {@link RestRequest} containing metadata about the request.
   * @return extracted operation type or blob ID from the uri.
   */
  protected static String getOperationOrBlobIdFromUri(RestRequest restRequest) {
    String path = restRequest.getPath();
    return (path.startsWith("/") ? path.substring(1, path.length()) : path);
  }
}

/**
 * Callback for HEAD that precedes GET operations. Updates headers and invokes GET with a new callback.
 */
class HeadForGetCallback implements Callback<BlobInfo> {
  private final AmbryBlobStorageService ambryBlobStorageService;
  private final RestRequest restRequest;
  private final RestResponseChannel restResponseChannel;
  private final Router router;
  private final long cacheValidityInSecs;
  private final long operationStartTime = System.currentTimeMillis();
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Create a HEAD before GET callback.
   * @param ambryBlobStorageService the {@link AmbryBlobStorageService} instance to submit responses to.
   * @param restRequest the {@link RestRequest} for whose response this is a callback.
   * @param restResponseChannel the {@link RestResponseChannel} to set headers on.
   * @param router the {@link Router} instance to use to make the GET call.
   * @param cacheValidityInSecs the period of validity of cache that needs to be sent to the client (in case of non
   *                            private blobs).
   */
  public HeadForGetCallback(AmbryBlobStorageService ambryBlobStorageService, RestRequest restRequest,
      RestResponseChannel restResponseChannel, Router router, long cacheValidityInSecs) {
    this.ambryBlobStorageService = ambryBlobStorageService;
    this.restRequest = restRequest;
    this.restResponseChannel = restResponseChannel;
    this.router = router;
    this.cacheValidityInSecs = cacheValidityInSecs;
  }

  /**
   * Sets headers and makes a GET call if the result was not null. Otherwise bails out.
   * @param result The result of the request i.e a {@link BlobInfo} object with the properties of the blob that is going
   *               to be scheduled for GET. This is non null if the request executed successfully.
   * @param exception The exception that was reported on execution of the request (if any).
   */
  @Override
  public void onCompletion(BlobInfo result, Exception exception) {
    long processingStartTime = System.currentTimeMillis();
    try {
      long routerTime = processingStartTime - operationStartTime;
      ambryBlobStorageService.frontendMetrics.headForGetTimeInMs.update(routerTime);
      restRequest.getMetricsTracker().addToTotalCpuTime(routerTime);

      String blobId = AmbryBlobStorageService.getOperationOrBlobIdFromUri(restRequest);
      logger.trace("Callback received for HEAD before GET of {}", blobId);
      restResponseChannel.setDate(new GregorianCalendar().getTime());
      if (exception == null && result != null) {
        logger.trace("Setting response headers for {}", blobId);
        setResponseHeaders(result);
        logger.trace("Forwarding GET after HEAD for {} to the router", blobId);
        router.getBlob(blobId, new GetCallback(ambryBlobStorageService, restRequest, restResponseChannel));
      } else if (exception == null) {
        exception = new IllegalStateException("Both response and exception are null for HeadForGetCallback");
      }
    } catch (Exception e) {
      ambryBlobStorageService.frontendMetrics.callbackProcessingError.inc();
      if (exception != null) {
        logger.error("Error while processing callback", e);
      } else {
        exception = e;
      }
    } finally {
      long processingTime = System.currentTimeMillis() - processingStartTime;
      ambryBlobStorageService.frontendMetrics.headForGetCallbackProcessingTimeInMs.update(processingTime);
      restRequest.getMetricsTracker().addToTotalCpuTime(processingTime);
      if (exception != null) {
        ambryBlobStorageService.frontendMetrics.operationError.inc();
        ambryBlobStorageService.submitResponse(restRequest, restResponseChannel, null, exception);
      }
    }
  }

  /**
   * Sets the required headers in the response.
   * @param blobInfo the {@link BlobInfo} to refer to while setting headers.
   * @throws RestServiceException if there was any problem setting the headers.
   */
  private void setResponseHeaders(BlobInfo blobInfo)
      throws RestServiceException {
    BlobProperties blobProperties = blobInfo.getBlobProperties();
    restResponseChannel.setLastModified(new Date(blobProperties.getCreationTimeInMs()));
    restResponseChannel.setHeader(RestUtils.Headers.Blob_Size, blobProperties.getBlobSize());
    if (blobProperties.getContentType() != null) {
      restResponseChannel.setContentType(blobProperties.getContentType());
      // Ensure browsers do not execute html with embedded exploits.
      if (blobProperties.getContentType().equals("text/html")) {
        restResponseChannel.setHeader("Content-Disposition", "attachment");
      }
    }
    if (blobProperties.isPrivate()) {
      restResponseChannel.setExpires(new Date(0));
      restResponseChannel.setCacheControl("private, no-cache, no-store, proxy-revalidate");
      restResponseChannel.setPragma("no-cache");
    } else {
      restResponseChannel.setExpires(new Date(System.currentTimeMillis() + cacheValidityInSecs * Time.MsPerSec));
      restResponseChannel.setCacheControl("max-age=" + cacheValidityInSecs);
    }
  }
}

/**
 * Callback for GET operations. Submits the response received to an instance of {@link RestResponseHandler}.
 */
class GetCallback implements Callback<ReadableStreamChannel> {
  private final AmbryBlobStorageService ambryBlobStorageService;
  private final RestRequest restRequest;
  private final RestResponseChannel restResponseChannel;
  private final long operationStartTime = System.currentTimeMillis();
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Create a GET callback.
   * @param ambryBlobStorageService the {@link AmbryBlobStorageService} instance to submit responses to.
   * @param restRequest the {@link RestRequest} for whose response this is a callback.
   * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be sent.
   */
  public GetCallback(AmbryBlobStorageService ambryBlobStorageService, RestRequest restRequest,
      RestResponseChannel restResponseChannel) {
    this.ambryBlobStorageService = ambryBlobStorageService;
    this.restRequest = restRequest;
    this.restResponseChannel = restResponseChannel;
  }

  /**
   * Submits the GET response to {@link RestResponseHandler} so that it can be sent (or the exception handled).
   * @param result The result of the request. This is the actual blob data as a {@link ReadableStreamChannel}.
   *               This is non null if the request executed successfully.
   * @param exception The exception that was reported on execution of the request (if any).
   */
  @Override
  public void onCompletion(ReadableStreamChannel result, Exception exception) {
    long processingStartTime = System.currentTimeMillis();
    try {
      long routerTime = processingStartTime - operationStartTime;
      ambryBlobStorageService.frontendMetrics.getTimeInMs.update(routerTime);
      restRequest.getMetricsTracker().addToTotalCpuTime(routerTime);

      String blobId = AmbryBlobStorageService.getOperationOrBlobIdFromUri(restRequest);
      logger.trace("Callback received for GET of {}", blobId);
      if (exception == null && result != null) {
        logger.trace("Successful GET of {}", blobId);
        restResponseChannel.setStatus(ResponseStatus.Ok);
      } else if (exception == null) {
        exception = new IllegalStateException("Both response and exception are null for GetCallback");
      }
    } catch (Exception e) {
      ambryBlobStorageService.frontendMetrics.callbackProcessingError.inc();
      if (exception != null) {
        logger.error("Error while processing callback", e);
      } else {
        exception = e;
      }
    } finally {
      long processingTime = System.currentTimeMillis() - processingStartTime;
      ambryBlobStorageService.frontendMetrics.getCallbackProcessingTimeInMs.update(processingTime);
      restRequest.getMetricsTracker().addToTotalCpuTime(processingTime);
      if (exception != null) {
        ambryBlobStorageService.frontendMetrics.operationError.inc();
      }
      ambryBlobStorageService.submitResponse(restRequest, restResponseChannel, result, exception);
    }
  }
}

/**
 * Callback for POST operations. Sends the response received to the client. Submits response either to handle exceptions
 * or to clean up after a response.
 */
class PostCallback implements Callback<String> {
  private final AmbryBlobStorageService ambryBlobStorageService;
  private final RestRequest restRequest;
  private final RestResponseChannel restResponseChannel;
  private final BlobProperties blobProperties;
  private final long operationStartTime = System.currentTimeMillis();
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Create a POST callback.
   * @param ambryBlobStorageService the {@link AmbryBlobStorageService} instance to submit responses to.
   * @param restRequest the {@link RestRequest} for whose response this is a callback.
   * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be sent.
   * @param createdBlobProperties the {@link BlobProperties} of the blob that was asked to be POSTed.
   */
  public PostCallback(AmbryBlobStorageService ambryBlobStorageService, RestRequest restRequest,
      RestResponseChannel restResponseChannel, BlobProperties createdBlobProperties) {
    this.ambryBlobStorageService = ambryBlobStorageService;
    this.restRequest = restRequest;
    this.restResponseChannel = restResponseChannel;
    this.blobProperties = createdBlobProperties;
  }

  /**
   * If there was no exception, updates the header with the location of the object. Submits the response either for
   * exception handling or for cleanup.
   * @param result The result of the request. This is the blob ID of the blob. This is non null if the request executed
   *               successfully.
   * @param exception The exception that was reported on execution of the request (if any).
   */
  @Override
  public void onCompletion(String result, Exception exception) {
    long processingStartTime = System.currentTimeMillis();
    try {
      long routerTime = processingStartTime - operationStartTime;
      ambryBlobStorageService.frontendMetrics.postTimeInMs.update(routerTime);
      restRequest.getMetricsTracker().addToTotalCpuTime(routerTime);

      logger.trace("Callback received for POST");
      restResponseChannel.setDate(new GregorianCalendar().getTime());
      if (exception == null && result != null) {
        logger.trace("Successful POST of {}", result);
        setResponseHeaders(result);
      } else if (exception == null) {
        exception = new IllegalStateException("Both response and exception are null for PostCallback");
      }
    } catch (Exception e) {
      ambryBlobStorageService.frontendMetrics.callbackProcessingError.inc();
      if (exception != null) {
        logger.error("Error while processing callback", e);
      } else {
        exception = e;
      }
    } finally {
      long processingTime = System.currentTimeMillis() - processingStartTime;
      ambryBlobStorageService.frontendMetrics.postCallbackProcessingTimeInMs.update(processingTime);
      restRequest.getMetricsTracker().addToTotalCpuTime(processingTime);
      if (exception != null) {
        ambryBlobStorageService.frontendMetrics.operationError.inc();
      }
      ambryBlobStorageService.submitResponse(restRequest, restResponseChannel, null, exception);
    }
  }

  /**
   * Sets the required headers in the response.
   * @param location the location of the created resource.
   * @throws RestServiceException if there was any problem setting the headers.
   */
  private void setResponseHeaders(String location)
      throws RestServiceException {
    restResponseChannel.setStatus(ResponseStatus.Created);
    restResponseChannel.setLocation(location);
    restResponseChannel.setContentLength(0);
    restResponseChannel.setHeader(RestUtils.Headers.Creation_Time, new Date(blobProperties.getCreationTimeInMs()));
  }
}

/**
 * Callback for DELETE operations. Sends an ACCEPTED response to the client if operation is successful. Submits response
 * either to handle exceptions or to clean up after a response.
 */
class DeleteCallback implements Callback<Void> {
  private final AmbryBlobStorageService ambryBlobStorageService;
  private final RestRequest restRequest;
  private final RestResponseChannel restResponseChannel;
  private final long operationStartTime = System.currentTimeMillis();
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Create a DELETE callback.
   * @param ambryBlobStorageService the {@link AmbryBlobStorageService} instance to submit responses to.
   * @param restRequest the {@link RestRequest} for whose response this is a callback.
   * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be sent.
   */
  public DeleteCallback(AmbryBlobStorageService ambryBlobStorageService, RestRequest restRequest,
      RestResponseChannel restResponseChannel) {
    this.ambryBlobStorageService = ambryBlobStorageService;
    this.restRequest = restRequest;
    this.restResponseChannel = restResponseChannel;
  }

  /**
   * If there was no exception, updates the header with the acceptance of the request. Submits the response either for
   * exception handling or for cleanup.
   * @param result The result of the request. This is always null.
   * @param exception The exception that was reported on execution of the request (if any).
   */
  @Override
  public void onCompletion(Void result, Exception exception) {
    long processingStartTime = System.currentTimeMillis();
    try {
      long routerTime = processingStartTime - operationStartTime;
      ambryBlobStorageService.frontendMetrics.deleteTimeInMs.update(routerTime);
      restRequest.getMetricsTracker().addToTotalCpuTime(routerTime);

      String blobId = AmbryBlobStorageService.getOperationOrBlobIdFromUri(restRequest);
      logger.trace("Callback received for DELETE of {}", blobId);
      restResponseChannel.setDate(new GregorianCalendar().getTime());
      if (exception == null) {
        logger.trace("Successful DELETE of {}", blobId);
        restResponseChannel.setStatus(ResponseStatus.Accepted);
        restResponseChannel.setContentLength(0);
      }
    } catch (Exception e) {
      ambryBlobStorageService.frontendMetrics.callbackProcessingError.inc();
      if (exception != null) {
        logger.error("Error while processing callback", e);
      } else {
        exception = e;
      }
    } finally {
      long processingTime = System.currentTimeMillis() - processingStartTime;
      ambryBlobStorageService.frontendMetrics.deleteCallbackProcessingTimeInMs.update(processingTime);
      restRequest.getMetricsTracker().addToTotalCpuTime(processingTime);
      if (exception != null) {
        ambryBlobStorageService.frontendMetrics.operationError.inc();
      }
      ambryBlobStorageService.submitResponse(restRequest, restResponseChannel, null, exception);
    }
  }
}

/**
 * Callback for HEAD operations. Sends the headers to the client if operation is successful. Submits response either to
 * handle exceptions or to clean up after a response.
 */
class HeadCallback implements Callback<BlobInfo> {
  private final AmbryBlobStorageService ambryBlobStorageService;
  private final RestRequest restRequest;
  private final RestResponseChannel restResponseChannel;
  private final long operationStartTime = System.currentTimeMillis();
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Create a HEAD callback.
   * @param ambryBlobStorageService the {@link AmbryBlobStorageService} instance to submit responses to.
   * @param restRequest the {@link RestRequest} for whose response this is a callback.
   * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be sent.
   */
  public HeadCallback(AmbryBlobStorageService ambryBlobStorageService, RestRequest restRequest,
      RestResponseChannel restResponseChannel) {
    this.ambryBlobStorageService = ambryBlobStorageService;
    this.restRequest = restRequest;
    this.restResponseChannel = restResponseChannel;
  }

  /**
   * If there was no exception, updates the header with the properties. Exceptions, if any, will be handled upon
   * submission.
   * @param result The result of the request i.e a {@link BlobInfo} object with the properties of the blob. This is
   *               non null if the request executed successfully.
   * @param exception The exception that was reported on execution of the request (if any).
   */
  @Override
  public void onCompletion(BlobInfo result, Exception exception) {
    long processingStartTime = System.currentTimeMillis();
    try {
      long routerTime = processingStartTime - operationStartTime;
      ambryBlobStorageService.frontendMetrics.headTimeInMs.update(routerTime);
      restRequest.getMetricsTracker().addToTotalCpuTime(routerTime);

      String blobId = AmbryBlobStorageService.getOperationOrBlobIdFromUri(restRequest);
      logger.trace("Callback received for HEAD of {}", blobId);
      restResponseChannel.setDate(new GregorianCalendar().getTime());
      if (exception == null && result != null) {
        logger.trace("Successful HEAD of {}", blobId);
        setResponseHeaders(result);
      } else if (exception == null) {
        exception = new IllegalStateException("Both response and exception are null for HeadCallback");
      }
    } catch (Exception e) {
      ambryBlobStorageService.frontendMetrics.callbackProcessingError.inc();
      if (exception != null) {
        logger.error("Error while processing callback", e);
      } else {
        exception = e;
      }
    } finally {
      long processingTime = System.currentTimeMillis() - processingStartTime;
      ambryBlobStorageService.frontendMetrics.headCallbackProcessingTimeInMs.update(processingTime);
      restRequest.getMetricsTracker().addToTotalCpuTime(processingTime);
      if (exception != null) {
        ambryBlobStorageService.frontendMetrics.operationError.inc();
      }
      ambryBlobStorageService.submitResponse(restRequest, restResponseChannel, null, exception);
    }
  }

  /**
   * Sets the required headers in the response.
   * @param blobInfo the {@link BlobInfo} to refer to while setting headers.
   * @throws RestServiceException if there was any problem setting the headers.
   */
  private void setResponseHeaders(BlobInfo blobInfo)
      throws RestServiceException {
    BlobProperties blobProperties = blobInfo.getBlobProperties();
    restResponseChannel.setStatus(ResponseStatus.Ok);
    restResponseChannel.setLastModified(new Date(blobProperties.getCreationTimeInMs()));
    restResponseChannel.setContentLength(blobProperties.getBlobSize());

    // Blob props
    restResponseChannel.setHeader(RestUtils.Headers.Blob_Size, blobProperties.getBlobSize());
    restResponseChannel.setHeader(RestUtils.Headers.Service_Id, blobProperties.getServiceId());
    restResponseChannel.setHeader(RestUtils.Headers.Creation_Time, new Date(blobProperties.getCreationTimeInMs()));
    restResponseChannel.setHeader(RestUtils.Headers.Private, blobProperties.isPrivate());
    if (blobProperties.getTimeToLiveInSeconds() != Utils.Infinite_Time) {
      restResponseChannel.setHeader(RestUtils.Headers.TTL, Long.toString(blobProperties.getTimeToLiveInSeconds()));
    }
    if (blobProperties.getContentType() != null) {
      restResponseChannel.setHeader(RestUtils.Headers.Content_Type, blobProperties.getContentType());
      restResponseChannel.setContentType(blobProperties.getContentType());
    }
    if (blobProperties.getOwnerId() != null) {
      restResponseChannel.setHeader(RestUtils.Headers.Owner_Id, blobProperties.getOwnerId());
    }
    byte[] userMetadataArray = blobInfo.getUserMetadata();
    Map<String, String> userMetadata = RestUtils.buildUserMetadata(userMetadataArray);
    for (Map.Entry<String, String> entry : userMetadata.entrySet()) {
      restResponseChannel.setHeader(entry.getKey(), entry.getValue());
    }
  }
}
