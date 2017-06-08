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
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.protocol.GetOption;
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
import com.github.ambry.router.GetBlobOptions;
import com.github.ambry.router.GetBlobOptionsBuilder;
import com.github.ambry.router.GetBlobResult;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.router.Router;
import com.github.ambry.router.RouterException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.GregorianCalendar;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is an Ambry frontend specific implementation of {@link BlobStorageService}.
 * <p/>
 * All the operations that need to be performed by the Ambry frontend are supported here.
 */
class AmbryBlobStorageService implements BlobStorageService {
  private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);
  static final String OPERATION_GET_PEERS = "peers";

  private static final String OPERATION_TYPE_INBOUND_ID_CONVERSION = "Inbound Id Conversion";
  private static final String OPERATION_TYPE_OUTBOUND_ID_CONVERSION = "Outbound Id Conversion";
  private static final String OPERATION_TYPE_GET_RESPONSE_SECURITY = "GET Response Security";
  private static final String OPERATION_TYPE_HEAD_RESPONSE_SECURITY = "HEAD Response Security";
  private static final String OPERATION_TYPE_POST_RESPONSE_SECURITY = "POST Response Security";
  private static final String OPERATION_TYPE_GET = "GET";
  private static final String OPERATION_TYPE_HEAD = "HEAD";
  private static final String OPERATION_TYPE_DELETE = "DELETE";
  private static final String OPERATION_TYPE_POST = "POST";

  private final RestResponseHandler responseHandler;
  private final Router router;
  private final IdConverterFactory idConverterFactory;
  private final SecurityServiceFactory securityServiceFactory;
  private final ClusterMap clusterMap;
  private final FrontendConfig frontendConfig;
  private final FrontendMetrics frontendMetrics;
  private final Logger logger = LoggerFactory.getLogger(AmbryBlobStorageService.class);

  private IdConverter idConverter = null;
  private SecurityService securityService = null;
  private GetPeersHandler getPeersHandler;
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
   * @param clusterMap the {@link ClusterMap} in use.
   */
  AmbryBlobStorageService(FrontendConfig frontendConfig, FrontendMetrics frontendMetrics,
      RestResponseHandler responseHandler, Router router, IdConverterFactory idConverterFactory,
      SecurityServiceFactory securityServiceFactory, ClusterMap clusterMap) {
    this.frontendConfig = frontendConfig;
    this.frontendMetrics = frontendMetrics;
    this.responseHandler = responseHandler;
    this.router = router;
    this.idConverterFactory = idConverterFactory;
    this.securityServiceFactory = securityServiceFactory;
    this.clusterMap = clusterMap;
    logger.trace("Instantiated AmbryBlobStorageService");
  }

  @Override
  public void start() throws InstantiationException {
    long startupBeginTime = System.currentTimeMillis();
    idConverter = idConverterFactory.getIdConverter();
    securityService = securityServiceFactory.getSecurityService();
    getPeersHandler = new GetPeersHandler(clusterMap, securityService, frontendMetrics);
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
  public void handleGet(final RestRequest restRequest, final RestResponseChannel restResponseChannel) {
    long processingStartTime = System.currentTimeMillis();
    long preProcessingTime = 0;
    handlePrechecks(restRequest, restResponseChannel);
    try {
      logger.trace("Handling GET request - {}", restRequest.getUri());
      checkAvailable();
      RestUtils.SubResource subresource = RestUtils.getBlobSubResource(restRequest);
      String operationOrBlobId =
          RestUtils.getOperationOrBlobIdFromUri(restRequest, subresource, frontendConfig.frontendPathPrefixesToRemove);
      if (operationOrBlobId.startsWith("/")) {
        operationOrBlobId = operationOrBlobId.substring(1);
      }
      if (operationOrBlobId.equalsIgnoreCase(OPERATION_GET_PEERS)) {
        getPeersHandler.handle(restRequest, restResponseChannel,
            (result, exception) -> submitResponse(restRequest, restResponseChannel, result, exception));
      } else {
        RestRequestMetrics requestMetrics =
            restRequest.getSSLSession() != null ? frontendMetrics.getBlobSSLMetrics : frontendMetrics.getBlobMetrics;
        if (subresource != null) {
          logger.trace("Sub-resource requested: {}", subresource);
          switch (subresource) {
            case BlobInfo:
              requestMetrics = restRequest.getSSLSession() != null ? frontendMetrics.getBlobInfoSSLMetrics
                  : frontendMetrics.getBlobInfoMetrics;
              break;
            case UserMetadata:
              requestMetrics = restRequest.getSSLSession() != null ? frontendMetrics.getUserMetadataSSLMetrics
                  : frontendMetrics.getUserMetadataMetrics;
              break;
          }
        }
        restRequest.getMetricsTracker().injectMetrics(requestMetrics);
        GetBlobOptions options = RestUtils.buildGetBlobOptions(restRequest.getArgs(), subresource, GetOption.None);
        GetCallback routerCallback = new GetCallback(restRequest, restResponseChannel, subresource, options);
        SecurityProcessRequestCallback securityCallback =
            new SecurityProcessRequestCallback(restRequest, restResponseChannel, routerCallback);
        securityService.processRequest(restRequest, securityCallback);
      }
      preProcessingTime = System.currentTimeMillis() - processingStartTime;
    } catch (Exception e) {
      submitResponse(restRequest, restResponseChannel, null, e);
    } finally {
      frontendMetrics.getPreProcessingTimeInMs.update(preProcessingTime);
    }
  }

  @Override
  public void handlePost(RestRequest restRequest, RestResponseChannel restResponseChannel) {
    long processingStartTime = System.currentTimeMillis();
    long preProcessingTime = 0;
    handlePrechecks(restRequest, restResponseChannel);
    restRequest.getMetricsTracker().injectMetrics(frontendMetrics.postBlobMetrics);
    RestRequestMetrics requestMetrics =
        restRequest.getSSLSession() != null ? frontendMetrics.postBlobSSLMetrics : frontendMetrics.postBlobMetrics;
    restRequest.getMetricsTracker().injectMetrics(requestMetrics);
    try {
      logger.trace("Handling POST request - {}", restRequest.getUri());
      checkAvailable();
      long propsBuildStartTime = System.currentTimeMillis();
      BlobProperties blobProperties = RestUtils.buildBlobProperties(restRequest.getArgs());
      if (blobProperties.getTimeToLiveInSeconds() + TimeUnit.MILLISECONDS.toSeconds(
          blobProperties.getCreationTimeInMs()) > Integer.MAX_VALUE) {
        logger.debug("TTL set to very large value in POST request with BlobProperties {}", blobProperties);
        frontendMetrics.ttlTooLargeError.inc();
      }
      byte[] usermetadata = RestUtils.buildUsermetadata(restRequest.getArgs());
      frontendMetrics.blobPropsBuildTimeInMs.update(System.currentTimeMillis() - propsBuildStartTime);
      logger.trace("Blob properties of blob being POSTed - {}", blobProperties);
      PostCallback routerCallback =
          new PostCallback(restRequest, restResponseChannel, new BlobInfo(blobProperties, usermetadata));
      preProcessingTime = System.currentTimeMillis() - processingStartTime;
      SecurityProcessRequestCallback securityCallback =
          new SecurityProcessRequestCallback(restRequest, restResponseChannel, blobProperties, usermetadata,
              routerCallback);
      securityService.processRequest(restRequest, securityCallback);
    } catch (Exception e) {
      submitResponse(restRequest, restResponseChannel, null, e);
    } finally {
      frontendMetrics.postPreProcessingTimeInMs.update(preProcessingTime);
    }
  }

  @Override
  public void handlePut(RestRequest restRequest, RestResponseChannel restResponseChannel) {
    handlePrechecks(restRequest, restResponseChannel);
    Exception exception =
        isUp ? new RestServiceException("PUT is not supported", RestServiceErrorCode.UnsupportedHttpMethod)
            : new RestServiceException("AmbryBlobStorageService unavailable", RestServiceErrorCode.ServiceUnavailable);
    submitResponse(restRequest, restResponseChannel, null, exception);
  }

  @Override
  public void handleDelete(RestRequest restRequest, RestResponseChannel restResponseChannel) {
    long processingStartTime = System.currentTimeMillis();
    long preProcessingTime = 0;
    handlePrechecks(restRequest, restResponseChannel);
    RestRequestMetrics requestMetrics =
        restRequest.getSSLSession() != null ? frontendMetrics.deleteBlobSSLMetrics : frontendMetrics.deleteBlobMetrics;
    restRequest.getMetricsTracker().injectMetrics(requestMetrics);
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
    }
  }

  @Override
  public void handleHead(RestRequest restRequest, RestResponseChannel restResponseChannel) {
    long processingStartTime = System.currentTimeMillis();
    long preProcessingTime = 0;
    handlePrechecks(restRequest, restResponseChannel);
    RestRequestMetrics requestMetrics =
        restRequest.getSSLSession() != null ? frontendMetrics.headBlobSSLMetrics : frontendMetrics.headBlobMetrics;
    restRequest.getMetricsTracker().injectMetrics(requestMetrics);
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
  void submitResponse(RestRequest restRequest, RestResponseChannel restResponseChannel,
      ReadableStreamChannel responseBody, Exception exception) {
    try {
      if (exception != null && exception instanceof RouterException) {
        exception = new RestServiceException(exception,
            RestServiceErrorCode.getRestServiceErrorCode(((RouterException) exception).getErrorCode()));
      }
      responseHandler.handleResponse(restRequest, restResponseChannel, responseBody, exception);
    } catch (Exception e) {
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
  private void checkAvailable() throws RestServiceException {
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
    private final GetCallback getCallback;
    private final HeadCallback headCallback;
    private final DeleteCallback deleteCallback;
    private final CallbackTracker callbackTracker;

    InboundIdConverterCallback(RestRequest restRequest, RestResponseChannel restResponseChannel, GetCallback callback) {
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
        GetCallback getCallback, HeadCallback headCallback, DeleteCallback deleteCallback) {
      this.restRequest = restRequest;
      this.restResponseChannel = restResponseChannel;
      this.getCallback = getCallback;
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
              getCallback.markStartTime();
              router.getBlob(result, getCallback.options, getCallback);
              break;
            case HEAD:
              headCallback.markStartTime();
              router.getBlob(result,
                  new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.BlobInfo).build(),
                  headCallback);
              break;
            case DELETE:
              deleteCallback.markStartTime();
              router.deleteBlob(result, RestUtils.getServiceId(restRequest), deleteCallback);
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

    private GetCallback getCallback;
    private HeadCallback headCallback;
    private PostCallback postCallback;
    private DeleteCallback deleteCallback;

    private BlobProperties blobProperties;
    private byte[] userMetadata;

    SecurityProcessRequestCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
        GetCallback callback) {
      this(restRequest, restResponseChannel, PROCESS_GET, frontendMetrics.getSecurityRequestTimeInMs,
          frontendMetrics.getSecurityRequestCallbackProcessingTimeInMs);
      this.getCallback = callback;
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
              String receivedId =
                  RestUtils.getOperationOrBlobIdFromUri(restRequest, RestUtils.getBlobSubResource(restRequest),
                      frontendConfig.frontendPathPrefixesToRemove);
              InboundIdConverterCallback idConverterCallback =
                  new InboundIdConverterCallback(restRequest, restResponseChannel, getCallback);
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
    }

    /**
     * Marks that the  callback processing has ended.
     */
    void markCallbackProcessingEnd() {
      logger.trace("Callback for {} of {} finished", operationType, blobId);
      long processingTime = System.currentTimeMillis() - processingStartTime;
      callbackProcessingTimeTracker.update(processingTime);
    }
  }

  /**
   * Callback for GET operations. Updates headers and submits the response body if there is no security exception.
   */
  private class GetCallback implements Callback<GetBlobResult> {
    private final RestRequest restRequest;
    private final RestResponseChannel restResponseChannel;
    private final RestUtils.SubResource subResource;
    private final GetBlobOptions options;
    private final CallbackTracker callbackTracker;

    /**
     * Create a GET callback.
     * @param restRequest the {@link RestRequest} for whose response this is a callback.
     * @param restResponseChannel the {@link RestResponseChannel} to set headers on.
     * @param subResource the sub-resource requested.
     * @param options the {@link GetBlobOptions} associated with the
     *                {@link Router#getBlob(String, GetBlobOptions, Callback)} call.
     */
    GetCallback(RestRequest restRequest, RestResponseChannel restResponseChannel, RestUtils.SubResource subResource,
        GetBlobOptions options) {
      this.restRequest = restRequest;
      this.restResponseChannel = restResponseChannel;
      this.subResource = subResource;
      this.options = options;
      callbackTracker = new CallbackTracker(restRequest, OPERATION_TYPE_GET, frontendMetrics.getTimeInMs,
          frontendMetrics.getCallbackProcessingTimeInMs);
    }

    /**
     * If the request is not for a sub resource, makes a GET call to the router. If the request is for a sub resource,
     * responds immediately. If there was no {@code routerResult} or if there was an exception, bails out.
     * Submits the GET response to {@link RestResponseHandler} so that it can be sent (or the exception handled).
     * @param routerResult The result of the request i.e a {@link GetBlobResult} object with the properties of the blob
     *                     (and a channel for blob data, if the request did not have a subresource) that is going to be
     *                     returned if no exception occured. This is non null if the request executed successfully.
     * @param routerException The exception that was reported on execution of the request (if any).
     */
    @Override
    public void onCompletion(final GetBlobResult routerResult, Exception routerException) {
      callbackTracker.markOperationEnd();
      if (routerResult == null && routerException == null) {
        throw new IllegalStateException("Both response and exception are null");
      }
      try {
        if (routerException == null) {
          final CallbackTracker securityCallbackTracker =
              new CallbackTracker(restRequest, OPERATION_TYPE_GET_RESPONSE_SECURITY,
                  frontendMetrics.getSecurityResponseTimeInMs,
                  frontendMetrics.getSecurityResponseCallbackProcessingTimeInMs);
          securityCallbackTracker.markOperationStart();
          securityService.processResponse(restRequest, restResponseChannel, routerResult.getBlobInfo(),
              new Callback<Void>() {
                @Override
                public void onCompletion(Void securityResult, Exception securityException) {
                  securityCallbackTracker.markOperationEnd();
                  ReadableStreamChannel response = null;
                  boolean blobNotModified = restResponseChannel.getStatus() == ResponseStatus.NotModified;
                  try {
                    if (securityException == null) {
                      if (subResource != null) {
                        BlobInfo blobInfo = routerResult.getBlobInfo();
                        Map<String, String> userMetadata = RestUtils.buildUserMetadata(blobInfo.getUserMetadata());
                        if (userMetadata == null) {
                          restResponseChannel.setHeader(RestUtils.Headers.CONTENT_TYPE, "application/octet-stream");
                          restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH,
                              blobInfo.getUserMetadata().length);
                          response = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(blobInfo.getUserMetadata()));
                        } else {
                          setUserMetadataHeaders(userMetadata, restResponseChannel);
                          restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, 0);
                          response = new ByteBufferReadableStreamChannel(AmbryBlobStorageService.EMPTY_BUFFER);
                        }
                      } else if (!blobNotModified) {
                        response = routerResult.getBlobDataChannel();
                      } else {
                        // If the blob was not modified, we need to close the channel, as it will not be submitted to
                        // the RestResponseHandler
                        routerResult.getBlobDataChannel().close();
                      }
                    }
                  } catch (Exception e) {
                    frontendMetrics.getSecurityResponseCallbackProcessingError.inc();
                    securityException = e;
                  } finally {
                    submitResponse(restRequest, restResponseChannel, response, securityException);
                    securityCallbackTracker.markCallbackProcessingEnd();
                  }
                }
              });
        }
      } catch (Exception e) {
        frontendMetrics.getCallbackProcessingError.inc();
        routerException = e;
      } finally {
        if (routerException != null) {
          submitResponse(restRequest, restResponseChannel,
              routerResult != null ? routerResult.getBlobDataChannel() : null, routerException);
        }
        callbackTracker.markCallbackProcessingEnd();
      }
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
   * Callback for POST operations. Sends the response received to the client. Submits response either to handle
   * exceptions or to clean up after a response.
   */
  private class PostCallback implements Callback<String> {
    private final RestRequest restRequest;
    private final RestResponseChannel restResponseChannel;
    private final BlobInfo blobInfo;
    private final CallbackTracker callbackTracker;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * Create a POST callback.
     * @param restRequest the {@link RestRequest} for whose response this is a callback.
     * @param restResponseChannel the {@link RestResponseChannel} over which response to {@code restRequest} can be
     *                            sent.
     * @param blobInfo the {@link BlobInfo} of the blob that was asked to be POSTed.
     */
    PostCallback(RestRequest restRequest, RestResponseChannel restResponseChannel, BlobInfo blobInfo) {
      this.restRequest = restRequest;
      this.restResponseChannel = restResponseChannel;
      this.blobInfo = blobInfo;
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
        throw new IllegalStateException("Both response and exception are null");
      }
      try {
        if (routerException == null) {
          logger.trace("Successful POST of {}", routerResult);
          final CallbackTracker idConversionCallbackTracker =
              new CallbackTracker(restRequest, OPERATION_TYPE_OUTBOUND_ID_CONVERSION,
                  frontendMetrics.outboundIdConversionTimeInMs,
                  frontendMetrics.outboundIdConversionCallbackProcessingTimeInMs);
          final CallbackTracker securityCallbackTracker =
              new CallbackTracker(restRequest, OPERATION_TYPE_POST_RESPONSE_SECURITY,
                  frontendMetrics.postSecurityResponseTimeInMs,
                  frontendMetrics.postSecurityResponseCallbackProcessingTimeInMs);
          idConversionCallbackTracker.markOperationStart();
          idConverter.convert(restRequest, routerResult, new Callback<String>() {
            @Override
            public void onCompletion(String idConversionResult, Exception idConversionException) {
              idConversionCallbackTracker.markOperationEnd();
              if (idConversionException != null) {
                submitResponse(restRequest, restResponseChannel, null, idConversionException);
              } else {
                try {
                  restResponseChannel.setHeader(RestUtils.Headers.LOCATION, idConversionResult);
                  securityCallbackTracker.markOperationStart();
                  securityService.processResponse(restRequest, restResponseChannel, blobInfo, new Callback<Void>() {
                    @Override
                    public void onCompletion(Void securityResult, Exception securityException) {
                      securityCallbackTracker.markOperationEnd();
                      submitResponse(restRequest, restResponseChannel, null, securityException);
                      securityCallbackTracker.markCallbackProcessingEnd();
                    }
                  });
                } catch (Exception e) {
                  frontendMetrics.outboundIdConversionCallbackProcessingError.inc();
                  submitResponse(restRequest, restResponseChannel, null, e);
                } finally {
                  idConversionCallbackTracker.markCallbackProcessingEnd();
                }
              }
            }
          });
        }
      } catch (Exception e) {
        frontendMetrics.postCallbackProcessingError.inc();
        routerException = e;
      } finally {
        if (routerException != null) {
          submitResponse(restRequest, restResponseChannel, null, routerException);
        }
        callbackTracker.markCallbackProcessingEnd();
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
      } catch (Exception e) {
        frontendMetrics.deleteCallbackProcessingError.inc();
        routerException = e;
      } finally {
        submitResponse(restRequest, restResponseChannel, null, routerException);
        callbackTracker.markCallbackProcessingEnd();
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
  private class HeadCallback implements Callback<GetBlobResult> {
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
     * @param routerResult The result of the request, which includes a {@link BlobInfo} object with the properties of
     *                     the blob. This is non null if the request executed successfully.
     * @param routerException The exception that was reported on execution of the request (if any).
     */
    @Override
    public void onCompletion(GetBlobResult routerResult, Exception routerException) {
      callbackTracker.markOperationEnd();
      if (routerResult == null && routerException == null) {
        throw new IllegalStateException("Both response and exception are null");
      }
      try {
        if (routerException == null) {
          final CallbackTracker securityCallbackTracker =
              new CallbackTracker(restRequest, OPERATION_TYPE_HEAD_RESPONSE_SECURITY,
                  frontendMetrics.headSecurityResponseTimeInMs,
                  frontendMetrics.headSecurityResponseCallbackProcessingTimeInMs);
          securityCallbackTracker.markOperationStart();
          securityService.processResponse(restRequest, restResponseChannel, routerResult.getBlobInfo(),
              new Callback<Void>() {
                @Override
                public void onCompletion(Void securityResult, Exception securityException) {
                  callbackTracker.markOperationEnd();
                  submitResponse(restRequest, restResponseChannel, null, securityException);
                  callbackTracker.markCallbackProcessingEnd();
                }
              });
        }
      } catch (Exception e) {
        frontendMetrics.headCallbackProcessingError.inc();
        routerException = e;
      } finally {
        if (routerException != null) {
          submitResponse(restRequest, restResponseChannel, null, routerException);
        }
        callbackTracker.markCallbackProcessingEnd();
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
