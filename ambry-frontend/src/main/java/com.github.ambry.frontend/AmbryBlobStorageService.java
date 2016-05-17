/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.frontend;

import com.codahale.metrics.Histogram;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.config.FrontendConfig;
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

  private static final String OPERATION_TYPE_INBOUND_ID_CONVERSION = "Inbound Id Conversion";
  private static final String OPERATION_TYPE_OUTBOUND_ID_CONVERSION = "Outbound Id Conversion";
  private static final String OPERATION_TYPE_GET_RESPONSE_SECURITY = "GET Response Security";
  private static final String OPERATION_TYPE_HEAD_RESPONSE_SECURITY = "HEAD Response Security";
  private static final String OPERATION_TYPE_HEAD_BEFORE_GET = "HEAD Before GET";
  private static final String OPERATION_TYPE_GET = "GET";
  private static final String OPERATION_TYPE_HEAD = "HEAD";
  private static final String OPERATION_TYPE_DELETE = "DELETE";
  private static final String OPERATION_TYPE_POST = "POST";

  private final RestResponseHandler responseHandler;
  private final Router router;
  private final IdConverterFactory idConverterFactory;
  private final SecurityServiceFactory securityServiceFactory;
  private final FrontendConfig frontendConfig;
  private final Logger logger = LoggerFactory.getLogger(AmbryBlobStorageService.class);

  private IdConverter idConverter = null;
  private SecurityService securityService = null;
  private boolean isUp = false;

  /**
   * Create a new instance of AmbryBlobStorageService by supplying it with config, metrics, cluster map, a
   * response handler controller and a router.
   * @param frontendConfig the {@link FrontendConfig} with configuration parameters.
   * @param frontendMetrics the metrics instance to use in the form of {@link FrontendMetrics}.
   * @param responseHandler the {@link RestResponseHandler} that can be used to submit responses that need to be sent
   *                        out.
   * @param router the {@link Router} instance to use to perform blob operations.
   * @param idConverterFactory the {@link IdConverterFactory} to use to get an {@link IdConverter}.
   * @param securityServiceFactory the {@link SecurityServiceFactory} to use to get an {@link SecurityService}.
   */
  public AmbryBlobStorageService(FrontendConfig frontendConfig, FrontendMetrics frontendMetrics,
      RestResponseHandler responseHandler, Router router, IdConverterFactory idConverterFactory,
      SecurityServiceFactory securityServiceFactory) {
    this.frontendConfig = frontendConfig;
    this.frontendMetrics = frontendMetrics;
    this.responseHandler = responseHandler;
    this.router = router;
    this.idConverterFactory = idConverterFactory;
    this.securityServiceFactory = securityServiceFactory;
    logger.trace("Instantiated AmbryBlobStorageService");
  }

  @Override
  public void start()
      throws InstantiationException {
    long startupBeginTime = System.currentTimeMillis();
    idConverter = idConverterFactory.getIdConverter();
    securityService = securityServiceFactory.getSecurityService();
    isUp = true;
    logger.info("AmbryBlobStorageService has started");
    frontendMetrics.blobStorageServiceStartupTimeInMs.update(System.currentTimeMillis() - startupBeginTime);
  }

  @Override
  public void shutdown() {
    long shutdownBeginTime = System.currentTimeMillis();
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
    } finally {
      frontendMetrics.blobStorageServiceShutdownTimeInMs.update(System.currentTimeMillis() - shutdownBeginTime);
    }
  }

  @Override
  public void handleGet(RestRequest restRequest, RestResponseChannel restResponseChannel) {
    long processingStartTime = System.currentTimeMillis();
    long preProcessingTime = 0;
    handlePrechecks(restRequest, restResponseChannel);
    try {
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
      HeadForGetCallback routerCallback = new HeadForGetCallback(restRequest, restResponseChannel, subresource);
      preProcessingTime = System.currentTimeMillis() - processingStartTime;
      SecurityProcessRequestCallback securityCallback =
          new SecurityProcessRequestCallback(restRequest, restResponseChannel, routerCallback);
      securityService.processRequest(restRequest, securityCallback);
    } catch (Exception e) {
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
    restRequest.getMetricsTracker().injectMetrics(frontendMetrics.postBlobMetrics);
    try {
      logger.trace("Handling POST request - {}", restRequest.getUri());
      checkAvailable();
      long propsBuildStartTime = System.currentTimeMillis();
      BlobProperties blobProperties = RestUtils.buildBlobProperties(restRequest);
      byte[] usermetadata = RestUtils.buildUsermetadata(restRequest);
      frontendMetrics.blobPropsBuildTimeInMs.update(System.currentTimeMillis() - propsBuildStartTime);
      logger.trace("Blob properties of blob being POSTed - {}", blobProperties);
      PostCallback routerCallback = new PostCallback(restRequest, restResponseChannel, blobProperties);
      preProcessingTime = System.currentTimeMillis() - processingStartTime;
      SecurityProcessRequestCallback securityCallback =
          new SecurityProcessRequestCallback(restRequest, restResponseChannel, blobProperties, usermetadata,
              routerCallback);
      securityService.processRequest(restRequest, securityCallback);
    } catch (Exception e) {
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
    restRequest.getMetricsTracker().injectMetrics(frontendMetrics.deleteBlobMetrics);
    try {
      logger.trace("Handling DELETE request - {}", restRequest.getUri());
      checkAvailable();
      DeleteCallback routerCallback = new DeleteCallback(restRequest, restResponseChannel);
      preProcessingTime = System.currentTimeMillis() - processingStartTime;
      SecurityProcessRequestCallback securityCallback =
          new SecurityProcessRequestCallback(restRequest, restResponseChannel, routerCallback);
      securityService.processRequest(restRequest, securityCallback);
    } catch (Exception e) {
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
      HeadCallback routerCallback = new HeadCallback(restRequest, restResponseChannel);
      preProcessingTime = System.currentTimeMillis() - processingStartTime;
      SecurityProcessRequestCallback securityCallback =
          new SecurityProcessRequestCallback(restRequest, restResponseChannel, routerCallback);
      securityService.processRequest(restRequest, securityCallback);
    } catch (Exception e) {
      submitResponse(restRequest, restResponseChannel, null, e);
    } finally {
      frontendMetrics.headPreProcessingTimeInMs.update(preProcessingTime);
      restRequest.getMetricsTracker().addToTotalCpuTime(preProcessingTime);
    }
  }

  /**
   * Submits the response and {@code responseBody} (and any {@code exception})for the {@code restRequest} to the
   * {@code responseHandler}.
   * @param restRequest the {@link RestRequest} for which a response is ready.
   * @param restResponseChannel the {@link RestResponseChannel} over which the response can be sent.
   * @param responseBody the body of the response in the form of a {@link ReadableStreamChannel}.
   * @param exception any {@link Exception} that occurred during the handling of {@code restRequest}.
   */
  protected void submitResponse(RestRequest restRequest, RestResponseChannel restResponseChannel,
      ReadableStreamChannel responseBody, Exception exception) {
    try {
      if (exception != null && exception instanceof RouterException) {
        exception = new RestServiceException(exception,
            RestServiceErrorCode.getRestServiceErrorCode(((RouterException) exception).getErrorCode()));
      }
      responseHandler.handleResponse(restRequest, restResponseChannel, responseBody, exception);
    } catch (RestServiceException e) {
      frontendMetrics.responseSubmissionError.inc();
      if (exception != null) {
        logger.error("Error submitting response to response handler", e);
      } else {
        exception = e;
      }
      logger.error("Handling of request {} failed", restRequest.getUri(), exception);
      restResponseChannel.onResponseComplete(exception);

      if (responseBody != null) {
        try {
          responseBody.close();
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
    private final CallbackTracker callbackTracker;

    InboundIdConverterCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
        HeadForGetCallback callback) {
      this(restRequest, restResponseChannel, callback, null, null);
    }

    InboundIdConverterCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
        HeadCallback callback) {
      this(restRequest, restResponseChannel, null, callback, null);
    }

    InboundIdConverterCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
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
      callbackTracker = new CallbackTracker(restRequest, OPERATION_TYPE_INBOUND_ID_CONVERSION,
          frontendMetrics.inboundIdConversionTimeInMs, frontendMetrics.inboundIdConversionCallbackProcessingTimeInMs);
      callbackTracker.markOperationStart();
    }

    /**
     * Forwards request to the {@link Router} once ID conversion is complete.
     * @param result The converted ID. This would be non null when the request executed successfully
     * @param exception The exception that was reported on execution of the request
     * @throws IllegalStateException if both {@code result} and {@code exception} are null.
     */
    @Override
    public void onCompletion(String result, Exception exception) {
      callbackTracker.markOperationEnd();
      if (result == null && exception == null) {
        throw new IllegalStateException("Both result and exception cannot be null");
      } else if (exception == null) {
        try {
          RestMethod restMethod = restRequest.getRestMethod();
          logger.trace("Forwarding {} of {} to the router", restMethod, result);
          switch (restMethod) {
            case GET:
              headForGetCallback.setBlobId(result);
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
              exception = new IllegalStateException("Unrecognized RestMethod: " + restMethod);
          }
        } catch (Exception e) {
          exception = e;
        }
      }

      if (exception != null) {
        submitResponse(restRequest, restResponseChannel, null, exception);
      }
      callbackTracker.markCallbackProcessingEnd();
    }
  }

  /**
   * Callback for {@link SecurityService#processRequest(RestRequest, Callback)}.
   */
  private class SecurityProcessRequestCallback implements Callback<Void> {
    private static final String PROCESS_GET = "GET Request Security";
    private static final String PROCESS_HEAD = "HEAD Request Security";
    private static final String PROCESS_POST = "POST Request Security";
    private static final String PROCESS_DELETE = "DELETE Request Security";

    private final RestRequest restRequest;
    private final RestResponseChannel restResponseChannel;
    private final CallbackTracker callbackTracker;

    private HeadForGetCallback headForGetCallback;
    private HeadCallback headCallback;
    private PostCallback postCallback;
    private DeleteCallback deleteCallback;

    private BlobProperties blobProperties;
    private byte[] userMetadata;

    SecurityProcessRequestCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
        HeadForGetCallback callback) {
      this(restRequest, restResponseChannel, PROCESS_GET, frontendMetrics.getSecurityRequestTimeInMs,
          frontendMetrics.getSecurityRequestCallbackProcessingTimeInMs);
      this.headForGetCallback = callback;
    }

    SecurityProcessRequestCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
        HeadCallback callback) {
      this(restRequest, restResponseChannel, PROCESS_HEAD, frontendMetrics.headSecurityRequestTimeInMs,
          frontendMetrics.headSecurityRequestCallbackProcessingTimeInMs);
      this.headCallback = callback;
    }

    SecurityProcessRequestCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
        BlobProperties blobProperties, byte[] userMetadata, PostCallback callback) {
      this(restRequest, restResponseChannel, PROCESS_POST, frontendMetrics.postSecurityRequestTimeInMs,
          frontendMetrics.postSecurityRequestCallbackProcessingTimeInMs);
      this.blobProperties = blobProperties;
      this.userMetadata = userMetadata;
      this.postCallback = callback;
    }

    SecurityProcessRequestCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
        DeleteCallback callback) {
      this(restRequest, restResponseChannel, PROCESS_DELETE, frontendMetrics.deleteSecurityRequestTimeInMs,
          frontendMetrics.deleteSecurityRequestCallbackProcessingTimeInMs);
      this.deleteCallback = callback;
    }

    private SecurityProcessRequestCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
        String operationType, Histogram operationTimeTracker, Histogram callbackProcessingTimeTracker) {
      this.restRequest = restRequest;
      this.restResponseChannel = restResponseChannel;
      callbackTracker =
          new CallbackTracker(restRequest, operationType, operationTimeTracker, callbackProcessingTimeTracker);
      callbackTracker.markOperationStart();
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
      callbackTracker.markOperationEnd();
      if (exception == null) {
        try {
          RestMethod restMethod = restRequest.getRestMethod();
          logger.trace("Forwarding {} to the IdConverter/Router", restMethod);
          switch (restMethod) {
            case GET:
              String receivedId = RestUtils
                  .getOperationOrBlobIdFromUri(restRequest, RestUtils.getBlobSubResource(restRequest),
                      frontendConfig.frontendPathPrefixesToRemove);
              InboundIdConverterCallback idConverterCallback =
                  new InboundIdConverterCallback(restRequest, restResponseChannel, headForGetCallback);
              idConverter.convert(restRequest, receivedId, idConverterCallback);
              break;
            case HEAD:
              receivedId = RestUtils.getOperationOrBlobIdFromUri(restRequest, RestUtils.getBlobSubResource(restRequest),
                  frontendConfig.frontendPathPrefixesToRemove);
              idConverterCallback = new InboundIdConverterCallback(restRequest, restResponseChannel, headCallback);
              idConverter.convert(restRequest, receivedId, idConverterCallback);
              break;
            case POST:
              postCallback.markStartTime();
              router.putBlob(blobProperties, userMetadata, restRequest, postCallback);
              break;
            case DELETE:
              receivedId = RestUtils.getOperationOrBlobIdFromUri(restRequest, RestUtils.getBlobSubResource(restRequest),
                  frontendConfig.frontendPathPrefixesToRemove);
              idConverterCallback = new InboundIdConverterCallback(restRequest, restResponseChannel, deleteCallback);
              idConverter.convert(restRequest, receivedId, idConverterCallback);
              break;
            default:
              exception = new IllegalStateException("Unrecognized RestMethod: " + restMethod);
          }
        } catch (Exception e) {
          exception = e;
        }
      }

      if (exception != null) {
        submitResponse(restRequest, restResponseChannel, null, exception);
      }
      callbackTracker.markCallbackProcessingEnd();
    }
  }

  /**
   * Tracks metrics and logs progress of operations that accept callbacks.
   */
  private class CallbackTracker {
    private long operationStartTime = 0;
    private long processingStartTime = 0;

    private final RestRequest restRequest;
    private final String operationType;
    private final Histogram operationTimeTracker;
    private final Histogram callbackProcessingTimeTracker;
    private final String blobId;

    /**
     * Create a CallbackTracker that tracks a particular operation.
     * @param restRequest the {@link RestRequest} for the operation.
     * @param operationType the type of operation.
     * @param operationTimeTracker the {@link Histogram} of the time taken by the operation.
     * @param callbackProcessingTimeTracker the {@link Histogram} of the time taken by the callback of the operation.
     */
    CallbackTracker(RestRequest restRequest, String operationType, Histogram operationTimeTracker,
        Histogram callbackProcessingTimeTracker) {
      this.restRequest = restRequest;
      this.operationType = operationType;
      this.operationTimeTracker = operationTimeTracker;
      this.callbackProcessingTimeTracker = callbackProcessingTimeTracker;
      blobId = RestUtils.getOperationOrBlobIdFromUri(restRequest, RestUtils.getBlobSubResource(restRequest),
          frontendConfig.frontendPathPrefixesToRemove);
    }

    /**
     * Marks that the operation being tracked has started.
     */
    void markOperationStart() {
      logger.trace("{} started for {}", operationType, blobId);
      operationStartTime = System.currentTimeMillis();
    }

    /**
     * Marks that the operation being tracked has ended and callback processing has started.
     */
    void markOperationEnd() {
      logger.trace("{} finished for {}", operationType, blobId);
      processingStartTime = System.currentTimeMillis();
      long operationTime = processingStartTime - operationStartTime;
      operationTimeTracker.update(operationTime);
      restRequest.getMetricsTracker().addToTotalCpuTime(operationTime);
    }

    /**
     * Marks that the  callback processing has ended.
     */
    void markCallbackProcessingEnd() {
      logger.trace("Callback for {} of {} finished", operationType, blobId);
      long processingTime = System.currentTimeMillis() - processingStartTime;
      callbackProcessingTimeTracker.update(processingTime);
      restRequest.getMetricsTracker().addToTotalCpuTime(processingTime);
    }
  }

  /**
   * Callback for HEAD that precedes GET operations. Updates headers and invokes GET with a new callback.
   */
  private class HeadForGetCallback implements Callback<BlobInfo> {
    private final RestRequest restRequest;
    private final RestResponseChannel restResponseChannel;
    private final RestUtils.SubResource subResource;
    private final CallbackTracker callbackTracker;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private String blobId;

    /**
     * Create a HEAD before GET callback.
     * @param restRequest the {@link RestRequest} for whose response this is a callback.
     * @param restResponseChannel the {@link RestResponseChannel} to set headers on.
     * @param subResource the sub-resource requested.
     */
    HeadForGetCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
        RestUtils.SubResource subResource) {
      this.restRequest = restRequest;
      this.restResponseChannel = restResponseChannel;
      this.subResource = subResource;
      callbackTracker =
          new CallbackTracker(restRequest, OPERATION_TYPE_HEAD_BEFORE_GET, frontendMetrics.headForGetTimeInMs,
              frontendMetrics.headForGetCallbackProcessingTimeInMs);
      blobId =
          RestUtils.getOperationOrBlobIdFromUri(restRequest, subResource, frontendConfig.frontendPathPrefixesToRemove);
    }

    /**
     * If the request is not for a sub resource, makes a GET call to the router. If the request is for a sub resource,
     * responds immediately. If there was no {@code routerResult} or if there was an exception, bails out.
     * @param routerResult The result of the request i.e a {@link BlobInfo} object with the properties of the blob that
     *                     is going to be scheduled for GET. This is non null if the request executed successfully.
     * @param routerException The exception that was reported on execution of the request (if any).
     */
    @Override
    public void onCompletion(final BlobInfo routerResult, Exception routerException) {
      callbackTracker.markOperationEnd();
      if (routerResult == null && routerException == null) {
        frontendMetrics.routerCallbackError.inc();
        callbackTracker.markCallbackProcessingEnd();
        throw new IllegalStateException("Both response and exception are null");
      }
      try {
        if (routerException == null) {
          final CallbackTracker securityCallbackTracker =
              new CallbackTracker(restRequest, OPERATION_TYPE_GET_RESPONSE_SECURITY,
                  frontendMetrics.getSecurityResponseTimeInMs,
                  frontendMetrics.getSecurityResponseCallbackProcessingTimeInMs);
          securityCallbackTracker.markOperationStart();
          securityService.processResponse(restRequest, restResponseChannel, routerResult, new Callback<Void>() {
            @Override
            public void onCompletion(Void securityResult, Exception securityException) {
              securityCallbackTracker.markOperationEnd();
              ReadableStreamChannel response = null;
              boolean blobNotModified = false;
              try {
                if (securityException == null) {
                  if (subResource == null) {
                    Long ifModifiedSinceMs = getIfModifiedSinceMs(restRequest);
                    if (ifModifiedSinceMs != null
                        && RestUtils.toSecondsPrecisionInMs(routerResult.getBlobProperties().getCreationTimeInMs())
                        <= ifModifiedSinceMs) {
                      blobNotModified = true;
                    } else {
                      logger.trace("Forwarding GET after HEAD for {} to the router", blobId);
                      router.getBlob(blobId, new GetCallback(restRequest, restResponseChannel));
                    }
                  } else {
                    Map<String, String> userMetadata = RestUtils.buildUserMetadata(routerResult.getUserMetadata());
                    if (userMetadata == null) {
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
              } catch (RestServiceException e) {
                frontendMetrics.getSecurityResponseCallbackProcessingError.inc();
                securityException = e;
              } finally {
                securityCallbackTracker.markCallbackProcessingEnd();
                if (response != null || securityException != null || blobNotModified) {
                  submitResponse(restRequest, restResponseChannel, response, securityException);
                }
              }
            }
          });
        }
      } catch (Exception e) {
        frontendMetrics.headForGetCallbackProcessingError.inc();
        routerException = e;
      } finally {
        callbackTracker.markCallbackProcessingEnd();
        if (routerException != null) {
          submitResponse(restRequest, restResponseChannel, null, routerException);
        }
      }
    }

    /**
     * Fetches the {@link RestUtils.Headers#IF_MODIFIED_SINCE} value in epoch time if present
     * @param restRequest the {@link RestRequest} that needs to be parsed
     * @return the {@link RestUtils.Headers#IF_MODIFIED_SINCE} value in epoch time if present
     */
    private Long getIfModifiedSinceMs(RestRequest restRequest) {
      if (restRequest.getArgs().get(RestUtils.Headers.IF_MODIFIED_SINCE) != null) {
        return RestUtils.getTimeFromDateString((String) restRequest.getArgs().get(RestUtils.Headers.IF_MODIFIED_SINCE));
      }
      return null;
    }

    /**
     * Sets the blob ID that should be used for {@link Router#getBlob(String, Callback)}.
     * @param blobId the blob ID that should be used for {@link Router#getBlob(String, Callback)}.
     */
    void setBlobId(String blobId) {
      this.blobId = blobId;
    }

    /**
     * Marks the start time of the operation.
     */
    void markStartTime() {
      callbackTracker.markOperationStart();
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
  private class GetCallback implements Callback<ReadableStreamChannel> {
    private final RestRequest restRequest;
    private final RestResponseChannel restResponseChannel;
    private final CallbackTracker callbackTracker;

    /**
     * Create a GET callback.
     * @param restRequest the {@link RestRequest} for whose response this is a callback.
     * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be
     *                            sent.
     */
    GetCallback(RestRequest restRequest, RestResponseChannel restResponseChannel) {
      this.restRequest = restRequest;
      this.restResponseChannel = restResponseChannel;
      callbackTracker = new CallbackTracker(restRequest, OPERATION_TYPE_GET, frontendMetrics.getTimeInMs,
          frontendMetrics.getCallbackProcessingTimeInMs);
      callbackTracker.markOperationStart();
    }

    /**
     * Submits the GET response to {@link RestResponseHandler} so that it can be sent (or the exception handled).
     * @param routerResult The result of the request. This is the actual blob data as a {@link ReadableStreamChannel}.
     *               This is non null if the request executed successfully.
     * @param routerException The exception that was reported on execution of the request (if any).
     */
    @Override
    public void onCompletion(ReadableStreamChannel routerResult, Exception routerException) {
      callbackTracker.markOperationEnd();
      if (routerResult == null && routerException == null) {
        frontendMetrics.routerCallbackError.inc();
        callbackTracker.markCallbackProcessingEnd();
        throw new IllegalStateException("Both response and exception are null");
      } else {
        callbackTracker.markCallbackProcessingEnd();
        submitResponse(restRequest, restResponseChannel, routerResult, routerException);
      }
    }
  }

  /**
   * Callback for POST operations. Sends the response received to the client. Submits response either to handle
   * exceptions or to clean up after a response.
   */
  private class PostCallback implements Callback<String> {
    private final RestRequest restRequest;
    private final RestResponseChannel restResponseChannel;
    private final BlobProperties blobProperties;
    private final CallbackTracker callbackTracker;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * Create a POST callback.
     * @param restRequest the {@link RestRequest} for whose response this is a callback.
     * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be
     *                            sent.
     * @param createdBlobProperties the {@link BlobProperties} of the blob that was asked to be POSTed.
     */
    PostCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
        BlobProperties createdBlobProperties) {
      this.restRequest = restRequest;
      this.restResponseChannel = restResponseChannel;
      this.blobProperties = createdBlobProperties;
      callbackTracker = new CallbackTracker(restRequest, OPERATION_TYPE_POST, frontendMetrics.postTimeInMs,
          frontendMetrics.postCallbackProcessingTimeInMs);
    }

    /**
     * If there was no exception, updates the header with the location of the object. Submits the response either for
     * exception handling or for cleanup.
     * @param routerResult The result of the request. This is the blob ID of the blob. This is non null if the request
     *               executed successfully.
     * @param routerException The exception that was reported on execution of the request (if any).
     */
    @Override
    public void onCompletion(String routerResult, Exception routerException) {
      callbackTracker.markOperationEnd();
      if (routerResult == null && routerException == null) {
        frontendMetrics.routerCallbackError.inc();
        callbackTracker.markCallbackProcessingEnd();
        throw new IllegalStateException("Both response and exception are null");
      }
      try {
        if (routerException == null) {
          logger.trace("Successful POST of {}", routerResult);
          final CallbackTracker idConversionCallbackTracker =
              new CallbackTracker(restRequest, OPERATION_TYPE_OUTBOUND_ID_CONVERSION,
                  frontendMetrics.outboundIdConversionTimeInMs,
                  frontendMetrics.outboundIdConversionCallbackProcessingTimeInMs);
          idConversionCallbackTracker.markOperationStart();
          idConverter.convert(restRequest, routerResult, new Callback<String>() {
            @Override
            public void onCompletion(String idConversionResult, Exception idConversionException) {
              idConversionCallbackTracker.markOperationEnd();
              if (idConversionException == null) {
                try {
                  setResponseHeaders(idConversionResult);
                } catch (RestServiceException e) {
                  frontendMetrics.outboundIdConversionCallbackProcessingError.inc();
                  idConversionException = e;
                }
              }
              idConversionCallbackTracker.markCallbackProcessingEnd();
              submitResponse(restRequest, restResponseChannel, null, idConversionException);
            }
          });
        }
      } catch (Exception e) {
        frontendMetrics.postCallbackProcessingError.inc();
        routerException = e;
      } finally {
        callbackTracker.markCallbackProcessingEnd();
        if (routerException != null) {
          submitResponse(restRequest, restResponseChannel, null, routerException);
        }
      }
    }

    /**
     * Marks the start time of the operation.
     */
    void markStartTime() {
      callbackTracker.markOperationStart();
    }

    /**
     * Sets the required headers in the response.
     * @param location the location of the created resource.
     * @throws RestServiceException if there was any problem setting the headers.
     */
    private void setResponseHeaders(String location)
        throws RestServiceException {
      restResponseChannel.setHeader(RestUtils.Headers.DATE, new GregorianCalendar().getTime());
      restResponseChannel.setStatus(ResponseStatus.Created);
      restResponseChannel.setHeader(RestUtils.Headers.LOCATION, location);
      restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, 0);
      restResponseChannel.setHeader(RestUtils.Headers.CREATION_TIME, new Date(blobProperties.getCreationTimeInMs()));
    }
  }

  /**
   * Callback for DELETE operations. Sends an ACCEPTED response to the client if operation is successful. Submits
   * response either to handle exceptions or to clean up after a response.
   */
  private class DeleteCallback implements Callback<Void> {
    private final RestRequest restRequest;
    private final RestResponseChannel restResponseChannel;
    private final CallbackTracker callbackTracker;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * Create a DELETE callback.
     * @param restRequest the {@link RestRequest} for whose response this is a callback.
     * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be
     *                            sent.
     */
    DeleteCallback(RestRequest restRequest, RestResponseChannel restResponseChannel) {
      this.restRequest = restRequest;
      this.restResponseChannel = restResponseChannel;
      callbackTracker = new CallbackTracker(restRequest, OPERATION_TYPE_DELETE, frontendMetrics.deleteTimeInMs,
          frontendMetrics.deleteCallbackProcessingTimeInMs);
    }

    /**
     * If there was no exception, updates the header with the acceptance of the request. Submits the response either for
     * exception handling or for cleanup.
     * @param routerResult The result of the request. This is always null.
     * @param routerException The exception that was reported on execution of the request (if any).
     */
    @Override
    public void onCompletion(Void routerResult, Exception routerException) {
      callbackTracker.markOperationEnd();
      try {
        if (routerException == null) {
          restResponseChannel.setHeader(RestUtils.Headers.DATE, new GregorianCalendar().getTime());
          restResponseChannel.setStatus(ResponseStatus.Accepted);
          restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, 0);
        }
      } catch (RestServiceException e) {
        frontendMetrics.deleteCallbackProcessingError.inc();
        routerException = e;
      } finally {
        callbackTracker.markCallbackProcessingEnd();
        submitResponse(restRequest, restResponseChannel, null, routerException);
      }
    }

    /**
     * Marks the start time of the operation.
     */
    void markStartTime() {
      callbackTracker.markOperationStart();
    }
  }

  /**
   * Callback for HEAD operations. Sends the headers to the client if operation is successful. Submits response either
   * to handle exceptions or to clean up after a response.
   */
  private class HeadCallback implements Callback<BlobInfo> {
    private final RestRequest restRequest;
    private final RestResponseChannel restResponseChannel;
    private final CallbackTracker callbackTracker;

    /**
     * Create a HEAD callback.
     * @param restRequest the {@link RestRequest} for whose response this is a callback.
     * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be
     *                            sent.
     */
    HeadCallback(RestRequest restRequest, RestResponseChannel restResponseChannel) {
      this.restRequest = restRequest;
      this.restResponseChannel = restResponseChannel;
      callbackTracker = new CallbackTracker(restRequest, OPERATION_TYPE_HEAD, frontendMetrics.headTimeInMs,
          frontendMetrics.headCallbackProcessingTimeInMs);
    }

    /**
     * If there was no exception, updates the header with the properties. Exceptions, if any, will be handled upon
     * submission.
     * @param routerResult The result of the request i.e a {@link BlobInfo} object with the properties of the blob. This is
     *               non null if the request executed successfully.
     * @param routerException The exception that was reported on execution of the request (if any).
     */
    @Override
    public void onCompletion(BlobInfo routerResult, Exception routerException) {
      callbackTracker.markOperationEnd();
      if (routerResult == null && routerException == null) {
        frontendMetrics.routerCallbackError.inc();
        callbackTracker.markCallbackProcessingEnd();
        throw new IllegalStateException("Both response and exception are null");
      }
      try {
        if (routerException == null) {
          final CallbackTracker securityCallbackTracker =
              new CallbackTracker(restRequest, OPERATION_TYPE_HEAD_RESPONSE_SECURITY,
                  frontendMetrics.headSecurityResponseTimeInMs,
                  frontendMetrics.headSecurityResponseCallbackProcessingTimeInMs);
          securityCallbackTracker.markOperationStart();
          securityService.processResponse(restRequest, restResponseChannel, routerResult, new Callback<Void>() {
            @Override
            public void onCompletion(Void securityResult, Exception securityException) {
              callbackTracker.markOperationEnd();
              callbackTracker.markCallbackProcessingEnd();
              submitResponse(restRequest, restResponseChannel, null, securityException);
            }
          });
        }
      } catch (Exception e) {
        frontendMetrics.headCallbackProcessingError.inc();
        routerException = e;
      } finally {
        callbackTracker.markCallbackProcessingEnd();
        if (routerException != null) {
          submitResponse(restRequest, restResponseChannel, null, routerException);
        }
      }
    }

    /**
     * Marks the start time of the operation.
     */
    void markStartTime() {
      callbackTracker.markOperationStart();
    }
  }
}
