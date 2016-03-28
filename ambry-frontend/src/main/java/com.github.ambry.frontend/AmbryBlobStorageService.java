package com.github.ambry.frontend;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.rest.BlobStorageService;
import com.github.ambry.rest.IdConverter;
import com.github.ambry.rest.IdConverterFactory;
import com.github.ambry.rest.ResponseStatus;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestRequestMetrics;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestResponseHandler;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.rest.SecurityService;
import com.github.ambry.rest.SecurityServiceFactory;
import com.github.ambry.router.Callback;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.router.Router;
import com.github.ambry.router.RouterException;
import java.io.IOException;
import java.nio.ByteBuffer;
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
  protected static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);
  protected final FrontendMetrics frontendMetrics;

  private final ClusterMap clusterMap;
  private final RestResponseHandler responseHandler;
  private final Router router;
  private final IdConverterFactory idConverterFactory;
  private final SecurityServiceFactory securityServiceFactory;
  private final Logger logger = LoggerFactory.getLogger(AmbryBlobStorageService.class);

  private IdConverter idConverter = null;
  private SecurityService securityService = null;
  private boolean isUp = false;

  /**
   * Create a new instance of AmbryBlobStorageService by supplying it with config, metrics, cluster map, a
   * response handler controller and a router.
   * @param frontendMetrics the metrics instance to use in the form of {@link FrontendMetrics}.
   * @param clusterMap the {@link ClusterMap} to be used for operations.
   * @param responseHandler the {@link RestResponseHandler} that can be used to submit responses that need to be sent
   *                        out.
   * @param router the {@link Router} instance to use to perform blob operations.
   * @param idConverterFactory the {@link IdConverterFactory} to use to get an {@link IdConverter}.
   * @param securityServiceFactory the {@link SecurityServiceFactory} to use to get an {@link SecurityService}.
   */
  public AmbryBlobStorageService(FrontendMetrics frontendMetrics, ClusterMap clusterMap,
      RestResponseHandler responseHandler, Router router, IdConverterFactory idConverterFactory,
      SecurityServiceFactory securityServiceFactory) {
    this.frontendMetrics = frontendMetrics;
    this.clusterMap = clusterMap;
    this.responseHandler = responseHandler;
    this.router = router;
    this.idConverterFactory = idConverterFactory;
    this.securityServiceFactory = securityServiceFactory;
    logger.trace("Instantiated AmbryBlobStorageService");
  }

  @Override
  public void start()
      throws InstantiationException {
    idConverter = idConverterFactory.getIdConverter();
    securityService = securityServiceFactory.getSecurityService();
    isUp = true;
    logger.info("AmbryBlobStorageService has started");
  }

  @Override
  public void shutdown() {
    isUp = false;
    try {
      if (securityService != null) {
        securityService.close();
        securityService = null;
      }
      if (idConverter != null) {
        idConverter.close();
        idConverter = null;
      }
      logger.info("AmbryBlobStorageService shutdown complete");
    } catch (IOException e) {
      logger.error("Downstream service close failed", e);
    }
  }

  @Override
  public void handleGet(RestRequest restRequest, RestResponseChannel restResponseChannel) {
    long processingStartTime = System.currentTimeMillis();
    long preProcessingTime = 0;
    handlePrechecks(restRequest, restResponseChannel);
    try {
      frontendMetrics.getBlobRate.mark();
      logger.trace("Handling GET request - {}", restRequest.getUri());
      checkAvailable();
      RestUtils.SubResource subresource = RestUtils.getBlobSubResource(restRequest);
      RestRequestMetrics requestMetrics = frontendMetrics.getBlobMetrics;
      if (subresource != null) {
        logger.trace("Sub-resource requested: {}", subresource);
        switch (subresource) {
          case BlobInfo:
            requestMetrics = frontendMetrics.getBlobInfoMetrics;
            break;
          case UserMetadata:
            requestMetrics = frontendMetrics.getUserMetadataMetrics;
            break;
        }
      }
      restRequest.getMetricsTracker().injectMetrics(requestMetrics);
      preProcessingTime = System.currentTimeMillis() - processingStartTime;
      HeadForGetCallback routerCallback =
          new HeadForGetCallback(this, restRequest, restResponseChannel, router, securityService, subresource);
      SecurityProcessRequestCallback securityCallback =
          new SecurityProcessRequestCallback(restRequest, restResponseChannel, routerCallback);
      securityService.processRequest(restRequest, securityCallback);
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
      checkAvailable();
      long propsBuildStartTime = System.currentTimeMillis();
      BlobProperties blobProperties = RestUtils.buildBlobProperties(restRequest);
      byte[] usermetadata = RestUtils.buildUsermetadata(restRequest);
      frontendMetrics.blobPropsBuildTimeInMs.update(System.currentTimeMillis() - propsBuildStartTime);
      logger.trace("Blob properties of blob being POSTed - {}", blobProperties);
      logger.trace("Forwarding POST to the router");
      preProcessingTime = System.currentTimeMillis() - processingStartTime;
      PostCallback routerCallback =
          new PostCallback(this, restRequest, restResponseChannel, blobProperties, idConverter);
      SecurityProcessRequestCallback securityCallback =
          new SecurityProcessRequestCallback(restRequest, restResponseChannel, blobProperties, usermetadata,
              routerCallback);
      securityService.processRequest(restRequest, securityCallback);
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
      checkAvailable();
      preProcessingTime = System.currentTimeMillis() - processingStartTime;
      DeleteCallback routerCallback = new DeleteCallback(this, restRequest, restResponseChannel);
      SecurityProcessRequestCallback securityCallback =
          new SecurityProcessRequestCallback(restRequest, restResponseChannel, routerCallback);
      securityService.processRequest(restRequest, securityCallback);
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
      checkAvailable();
      preProcessingTime = System.currentTimeMillis() - processingStartTime;
      HeadCallback routerCallback = new HeadCallback(this, restRequest, restResponseChannel, securityService);
      SecurityProcessRequestCallback securityCallback =
          new SecurityProcessRequestCallback(restRequest, restResponseChannel, routerCallback);
      securityService.processRequest(restRequest, securityCallback);
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
   * Checks if {@link AmbryBlobStorageService} is available to serve requests.
   * @throws RestServiceException if {@link AmbryBlobStorageService} is not available to serve requests.
   */
  private void checkAvailable()
      throws RestServiceException {
    if (!isUp) {
      throw new RestServiceException("AmbryBlobStorageService unavailable", RestServiceErrorCode.ServiceUnavailable);
    }
  }

  /**
   * Callback for {@link IdConverter} that is used when inbound IDs are converted.
   */
  private class InboundIdConverterCallback implements Callback<String> {
    private final RestRequest restRequest;
    private final RestResponseChannel restResponseChannel;
    private final HeadForGetCallback headForGetCallback;
    private final HeadCallback headCallback;
    private final DeleteCallback deleteCallback;

    private InboundIdConverterCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
        HeadForGetCallback callback) {
      this(restRequest, restResponseChannel, callback, null, null);
    }

    private InboundIdConverterCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
        HeadCallback callback) {
      this(restRequest, restResponseChannel, null, callback, null);
    }

    private InboundIdConverterCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
        DeleteCallback callback) {
      this(restRequest, restResponseChannel, null, null, callback);
    }

    private InboundIdConverterCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
        HeadForGetCallback headForGetCallback, HeadCallback headCallback, DeleteCallback deleteCallback) {
      this.restRequest = restRequest;
      this.restResponseChannel = restResponseChannel;
      this.headForGetCallback = headForGetCallback;
      this.headCallback = headCallback;
      this.deleteCallback = deleteCallback;
    }

    /**
     * Forwards request to the {@link Router} once ID conversion is complete.
     * @param result The converted ID. This would be non null when the request executed successfully
     * @param exception The exception that was reported on execution of the request
     */
    @Override
    public void onCompletion(String result, Exception exception) {
      if (result == null && exception == null) {
        throw new IllegalStateException("Both result and exception cannot be null");
      } else if (exception != null) {
        submitResponse(restRequest, restResponseChannel, null, exception);
      } else {
        RestMethod restMethod = restRequest.getRestMethod();
        logger.trace("Forwarding {} of {} to the router", restMethod, result);
        switch (restMethod) {
          case GET:
            headForGetCallback.markStartTime();
            router.getBlobInfo(result, headForGetCallback);
            break;
          case HEAD:
            headCallback.markStartTime();
            router.getBlobInfo(result, headCallback);
            break;
          case DELETE:
            deleteCallback.markStartTime();
            router.deleteBlob(result, deleteCallback);
            break;
          default:
            throw new IllegalStateException("Unrecognized RestMethod: " + restMethod);
        }
      }
    }
  }

  /**
   * Callback for {@link SecurityService#processRequest(RestRequest, Callback)}.
   */
  private class SecurityProcessRequestCallback implements Callback<Void> {
    private final RestRequest restRequest;
    private final RestResponseChannel restResponseChannel;

    private HeadForGetCallback headForGetCallback;
    private HeadCallback headCallback;
    private PostCallback postCallback;
    private DeleteCallback deleteCallback;

    private BlobProperties blobProperties;
    private byte[] userMetadata;

    private SecurityProcessRequestCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
        HeadForGetCallback callback) {
      this(restRequest, restResponseChannel);
      this.headForGetCallback = callback;
    }

    private SecurityProcessRequestCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
        HeadCallback callback) {
      this(restRequest, restResponseChannel);
      this.headCallback = callback;
    }

    private SecurityProcessRequestCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
        BlobProperties blobProperties, byte[] userMetadata, PostCallback callback) {
      this(restRequest, restResponseChannel);
      this.blobProperties = blobProperties;
      this.userMetadata = userMetadata;
      this.postCallback = callback;
    }

    private SecurityProcessRequestCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
        DeleteCallback callback) {
      this(restRequest, restResponseChannel);
      this.deleteCallback = callback;
    }

    private SecurityProcessRequestCallback(RestRequest restRequest, RestResponseChannel restResponseChannel) {
      this.restRequest = restRequest;
      this.restResponseChannel = restResponseChannel;
    }

    /**
     * Handles request once it has been vetted by the {@link SecurityService}.
     * In case of exception, response is immediately submitted to the {@link RestResponseHandler}.
     * In case of GET, HEAD and DELETE, ID conversion is triggered.
     * In case of POST, request is forwarded to the {@link Router}.
     * @param result The result of the request. This would be non null when the request executed successfully
     * @param exception The exception that was reported on execution of the request
     */
    @Override
    public void onCompletion(Void result, Exception exception) {
      if (exception != null) {
        submitResponse(restRequest, restResponseChannel, null, exception);
      } else {
        RestMethod restMethod = restRequest.getRestMethod();
        logger.trace("Forwarding {} to the IdConverter/Router", restMethod);
        switch (restMethod) {
          case GET:
            String receivedId = RestUtils.getOperationOrBlobIdFromUri(restRequest);
            InboundIdConverterCallback idConverterCallback =
                new InboundIdConverterCallback(restRequest, restResponseChannel, headForGetCallback);
            idConverter.convert(restRequest, receivedId, idConverterCallback);
            break;
          case HEAD:
            receivedId = RestUtils.getOperationOrBlobIdFromUri(restRequest);
            idConverterCallback = new InboundIdConverterCallback(restRequest, restResponseChannel, headCallback);
            idConverter.convert(restRequest, receivedId, idConverterCallback);
            break;
          case POST:
            postCallback.markStartTime();
            router.putBlob(blobProperties, userMetadata, restRequest, postCallback);
            break;
          case DELETE:
            receivedId = RestUtils.getOperationOrBlobIdFromUri(restRequest);
            idConverterCallback = new InboundIdConverterCallback(restRequest, restResponseChannel, deleteCallback);
            idConverter.convert(restRequest, receivedId, idConverterCallback);
            break;
          default:
            throw new IllegalStateException("Unrecognized RestMethod: " + restMethod);
        }
      }
    }
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
  private final SecurityService securityService;
  private final RestUtils.SubResource subResource;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  private long operationStartTime = 0;

  /**
   * Create a HEAD before GET callback.
   * @param ambryBlobStorageService the {@link AmbryBlobStorageService} instance to submit responses to.
   * @param restRequest the {@link RestRequest} for whose response this is a callback.
   * @param restResponseChannel the {@link RestResponseChannel} to set headers on.
   * @param router the {@link Router} instance to use to make the GET call.
   * @param securityService the {@link SecurityService} instance to use to verify the response.
   * @param subResource the sub-resource requested.
   */
  public HeadForGetCallback(AmbryBlobStorageService ambryBlobStorageService, RestRequest restRequest,
      RestResponseChannel restResponseChannel, Router router, SecurityService securityService,
      RestUtils.SubResource subResource) {
    this.ambryBlobStorageService = ambryBlobStorageService;
    this.restRequest = restRequest;
    this.restResponseChannel = restResponseChannel;
    this.router = router;
    this.securityService = securityService;
    this.subResource = subResource;
  }

  /**
   * If the request is not for a sub resource, makes a GET call to the router. If the request is for a sub resource,
   * responds immediately. If there was no {@code routerResult} or if there was an exception, bails out.
   * @param routerResult The result of the request i.e a {@link BlobInfo} object with the properties of the blob that is going
   *               to be scheduled for GET. This is non null if the request executed successfully.
   * @param exception The exception that was reported on execution of the request (if any).
   */
  @Override
  public void onCompletion(final BlobInfo routerResult, Exception exception) {
    long processingStartTime = System.currentTimeMillis();
    try {
      long routerTime = processingStartTime - operationStartTime;
      ambryBlobStorageService.frontendMetrics.headForGetTimeInMs.update(routerTime);
      restRequest.getMetricsTracker().addToTotalCpuTime(routerTime);

      final String blobId = RestUtils.getOperationOrBlobIdFromUri(restRequest);
      logger.trace("Callback received for HEAD before GET of {}", blobId);
      if (exception == null && routerResult != null) {
        securityService.processResponse(restRequest, restResponseChannel, routerResult, new Callback<Void>() {
          @Override
          public void onCompletion(Void antivirusResult, Exception exception) {
            ReadableStreamChannel response = null;
            try {
              if (exception == null) {
                if (subResource == null) {
                  logger.trace("Forwarding GET after HEAD for {} to the router", blobId);
                  router.getBlob(blobId, new GetCallback(ambryBlobStorageService, restRequest, restResponseChannel));
                } else {
                  // TODO: if old style, make RestUtils.getUserMetadata() just return null.
                  Map<String, String> userMetadata = RestUtils.buildUserMetadata(routerResult.getUserMetadata());
                  if (shouldSendMetadataAsContent(userMetadata)) {
                    restResponseChannel.setHeader(RestUtils.Headers.CONTENT_TYPE, "application/octet-stream");
                    restResponseChannel
                        .setHeader(RestUtils.Headers.CONTENT_LENGTH, routerResult.getUserMetadata().length);
                    response = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(routerResult.getUserMetadata()));
                  } else {
                    setUserMetadataHeaders(userMetadata, restResponseChannel);
                    restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, 0);
                    response = new ByteBufferReadableStreamChannel(AmbryBlobStorageService.EMPTY_BUFFER);
                  }
                }
              }
            } catch (Exception e) {
              exception = e;
            } finally {
              if (response != null || exception != null) {
                if (exception != null) {
                  ambryBlobStorageService.frontendMetrics.operationError.inc();
                }
                ambryBlobStorageService.submitResponse(restRequest, restResponseChannel, response, exception);
              }
            }
          }
        });
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
   * Marks the start time of the operation.
   */
  protected void markStartTime() {
    operationStartTime = System.currentTimeMillis();
  }

  /**
   * Determines if user metadata should be sent as content by looking for any keys that are prefixed with
   * {@link RestUtils.Headers#USER_META_DATA_OLD_STYLE_PREFIX}.
   * @param userMetadata the user metadata that was constructed from the byte stream.
   * @return {@code true} if any key is prefixed with {@link RestUtils.Headers#USER_META_DATA_OLD_STYLE_PREFIX}.
   *         {@code false} otherwise.
   */
  private boolean shouldSendMetadataAsContent(Map<String, String> userMetadata) {
    boolean shouldSendAsContent = false;
    for (Map.Entry<String, String> entry : userMetadata.entrySet()) {
      if (entry.getKey().startsWith(RestUtils.Headers.USER_META_DATA_OLD_STYLE_PREFIX)) {
        shouldSendAsContent = true;
        break;
      }
    }
    return shouldSendAsContent;
  }

  /**
   * Sets the user metadata in the headers of the response.
   * @param userMetadata the user metadata that need to be set in the headers.
   * @param restResponseChannel the {@link RestResponseChannel} that is used for sending the response.
   * @throws RestServiceException if there are any problems setting the header.
   */
  private void setUserMetadataHeaders(Map<String, String> userMetadata, RestResponseChannel restResponseChannel)
      throws RestServiceException {
    for (Map.Entry<String, String> entry : userMetadata.entrySet()) {
      restResponseChannel.setHeader(entry.getKey(), entry.getValue());
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

      String blobId = RestUtils.getOperationOrBlobIdFromUri(restRequest);
      logger.trace("Callback received for GET of {}", blobId);
      if (exception == null && result != null) {
        logger.trace("Successful GET of {}", blobId);
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
  private final IdConverter idConverter;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  private long operationStartTime = 0;

  /**
   * Create a POST callback.
   * @param ambryBlobStorageService the {@link AmbryBlobStorageService} instance to submit responses to.
   * @param restRequest the {@link RestRequest} for whose response this is a callback.
   * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be sent.
   * @param createdBlobProperties the {@link BlobProperties} of the blob that was asked to be POSTed.
   * @param idConverter the {@link IdConverter} to use to convert IDs returned from the {@link Router}.
   */
  public PostCallback(AmbryBlobStorageService ambryBlobStorageService, RestRequest restRequest,
      RestResponseChannel restResponseChannel, BlobProperties createdBlobProperties, IdConverter idConverter) {
    this.ambryBlobStorageService = ambryBlobStorageService;
    this.restRequest = restRequest;
    this.restResponseChannel = restResponseChannel;
    this.blobProperties = createdBlobProperties;
    this.idConverter = idConverter;
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
      restResponseChannel.setHeader(RestUtils.Headers.DATE, new GregorianCalendar().getTime());
      if (exception == null && result != null) {
        logger.trace("Successful POST of {}", result);
        idConverter.convert(restRequest, result, new Callback<String>() {
          @Override
          public void onCompletion(String result, Exception exception) {
            if (exception == null) {
              try {
                setResponseHeaders(result);
              } catch (RestServiceException e) {
                exception = e;
              }
            } else {
              ambryBlobStorageService.frontendMetrics.operationError.inc();
            }
            ambryBlobStorageService.submitResponse(restRequest, restResponseChannel, null, exception);
          }
        });
      } else if (exception == null) {
        exception = new IllegalStateException("Both response and exception are null for PostCallback");
      }
    } catch (Exception e) {
      ambryBlobStorageService.frontendMetrics.callbackProcessingError.inc();
      if (exception != null) {
        logger.error("Error while processing Router callback", e);
      } else {
        exception = e;
      }
    } finally {
      long processingTime = System.currentTimeMillis() - processingStartTime;
      ambryBlobStorageService.frontendMetrics.postCallbackProcessingTimeInMs.update(processingTime);
      restRequest.getMetricsTracker().addToTotalCpuTime(processingTime);
      if (exception != null) {
        ambryBlobStorageService.frontendMetrics.operationError.inc();
        ambryBlobStorageService.submitResponse(restRequest, restResponseChannel, null, exception);
      }
    }
  }

  /**
   * Marks the start time of the operation.
   */
  protected void markStartTime() {
    operationStartTime = System.currentTimeMillis();
  }

  /**
   * Sets the required headers in the response.
   * @param location the location of the created resource.
   * @throws RestServiceException if there was any problem setting the headers.
   */
  private void setResponseHeaders(String location)
      throws RestServiceException {
    restResponseChannel.setStatus(ResponseStatus.Created);
    restResponseChannel.setHeader(RestUtils.Headers.LOCATION, location);
    restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, 0);
    restResponseChannel.setHeader(RestUtils.Headers.CREATION_TIME, new Date(blobProperties.getCreationTimeInMs()));
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
  private final Logger logger = LoggerFactory.getLogger(getClass());

  private long operationStartTime = 0;

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

      String blobId = RestUtils.getOperationOrBlobIdFromUri(restRequest);
      logger.trace("Callback received for DELETE of {}", blobId);
      restResponseChannel.setHeader(RestUtils.Headers.DATE, new GregorianCalendar().getTime());
      if (exception == null) {
        logger.trace("Successful DELETE of {}", blobId);
        restResponseChannel.setStatus(ResponseStatus.Accepted);
        restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, 0);
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

  /**
   * Marks the start time of the operation.
   */
  protected void markStartTime() {
    operationStartTime = System.currentTimeMillis();
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
  private final SecurityService securityService;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  private long operationStartTime = 0;

  /**
   * Create a HEAD callback.
   * @param ambryBlobStorageService the {@link AmbryBlobStorageService} instance to submit responses to.
   * @param restRequest the {@link RestRequest} for whose response this is a callback.
   * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be sent.
   * @param securityService the {@link SecurityService} instance to use to verify the response.
   */
  public HeadCallback(AmbryBlobStorageService ambryBlobStorageService, RestRequest restRequest,
      RestResponseChannel restResponseChannel, SecurityService securityService) {
    this.ambryBlobStorageService = ambryBlobStorageService;
    this.restRequest = restRequest;
    this.restResponseChannel = restResponseChannel;
    this.securityService = securityService;
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

      String blobId = RestUtils.getOperationOrBlobIdFromUri(restRequest);
      logger.trace("Callback received for HEAD of {}", blobId);
      if (exception == null && result != null) {
        logger.trace("Successful HEAD of {}", blobId);
        securityService.processResponse(restRequest, restResponseChannel, result, new Callback<Void>() {
          @Override
          public void onCompletion(Void result, Exception exception) {
            if (exception != null) {
              ambryBlobStorageService.frontendMetrics.operationError.inc();
            }
            ambryBlobStorageService.submitResponse(restRequest, restResponseChannel, null, exception);
          }
        });
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
        ambryBlobStorageService.submitResponse(restRequest, restResponseChannel, null, exception);
      }
    }
  }

  /**
   * Marks the start time of the operation.
   */
  protected void markStartTime() {
    operationStartTime = System.currentTimeMillis();
  }
}
