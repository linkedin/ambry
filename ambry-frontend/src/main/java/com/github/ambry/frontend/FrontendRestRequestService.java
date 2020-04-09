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
import com.github.ambry.account.AccountService;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.rest.RestRequestService;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.ResponseStatus;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestRequestMetrics;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestResponseHandler;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.router.Callback;
import com.github.ambry.router.GetBlobOptions;
import com.github.ambry.router.GetBlobOptionsBuilder;
import com.github.ambry.router.GetBlobResult;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.router.Router;
import com.github.ambry.router.RouterErrorCode;
import com.github.ambry.router.RouterException;
import com.github.ambry.utils.AsyncOperationTracker;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.ThrowingConsumer;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.GregorianCalendar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.rest.RestUtils.*;
import static com.github.ambry.rest.RestUtils.InternalKeys.*;


/**
 * This is an Ambry frontend specific implementation of {@link RestRequestService}.
 * All the operations that need to be performed by the Ambry frontend are supported here.
 */
class FrontendRestRequestService implements RestRequestService {
  static final String TTL_UPDATE_REJECTED_ALLOW_HEADER_VALUE = "GET,HEAD,DELETE";

  private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

  private static final String OPERATION_TYPE_INBOUND_ID_CONVERSION = "Inbound Id Conversion";
  private static final String OPERATION_TYPE_GET_RESPONSE_SECURITY = "GET Response Security";
  private static final String OPERATION_TYPE_HEAD_RESPONSE_SECURITY = "HEAD Response Security";
  private static final String OPERATION_TYPE_GET = "GET";
  private static final String OPERATION_TYPE_HEAD = "HEAD";
  private static final String OPERATION_TYPE_DELETE = "DELETE";
  private final Router router;
  private final IdConverterFactory idConverterFactory;
  private final SecurityServiceFactory securityServiceFactory;
  private final ClusterMap clusterMap;
  private final FrontendConfig frontendConfig;
  private final FrontendMetrics frontendMetrics;
  private final GetReplicasHandler getReplicasHandler;
  private final UrlSigningService urlSigningService;
  private final IdSigningService idSigningService;
  private final AccountService accountService;
  private final AccountAndContainerInjector accountAndContainerInjector;
  private final Logger logger = LoggerFactory.getLogger(FrontendRestRequestService.class);
  private final String datacenterName;
  private final String hostname;
  private final String clusterName;
  private RestResponseHandler responseHandler;
  private IdConverter idConverter = null;
  private SecurityService securityService = null;
  private GetPeersHandler getPeersHandler;
  private GetSignedUrlHandler getSignedUrlHandler;
  private PostBlobHandler postBlobHandler;
  private TtlUpdateHandler ttlUpdateHandler;
  private UndeleteHandler undeleteHandler;
  private GetClusterMapSnapshotHandler getClusterMapSnapshotHandler;
  private GetAccountsHandler getAccountsHandler;
  private PostAccountsHandler postAccountsHandler;
  private boolean isUp = false;

  /**
   * Create a new instance of FrontendRestRequestService by supplying it with config, metrics, cluster map, a
   * response handler controller and a router.
   * @param frontendConfig the {@link FrontendConfig} with configuration parameters.
   * @param frontendMetrics the metrics instance to use in the form of {@link FrontendMetrics}.
   * @param router the {@link Router} instance to use to perform blob operations.
   * @param clusterMap the {@link ClusterMap} in use.
   * @param idConverterFactory the {@link IdConverterFactory} to use to get an {@link IdConverter} instance.
   * @param securityServiceFactory the {@link SecurityServiceFactory} to use to get an {@link SecurityService} instance.
   * @param urlSigningService the {@link UrlSigningService} used to sign URLs.
   * @param idSigningService the {@link IdSigningService} used to sign and verify IDs.
   * @param accountService the {@link AccountService} to use.
   * @param accountAndContainerInjector the {@link AccountAndContainerInjector} to use.
   * @param datacenterName the local datacenter name for this frontend.
   * @param hostname the hostname for this frontend.
   * @param clusterName the name of the storage cluster that the router communicates with.
   */
  FrontendRestRequestService(FrontendConfig frontendConfig, FrontendMetrics frontendMetrics,
      Router router, ClusterMap clusterMap, IdConverterFactory idConverterFactory,
      SecurityServiceFactory securityServiceFactory, UrlSigningService urlSigningService,
      IdSigningService idSigningService, AccountService accountService,
      AccountAndContainerInjector accountAndContainerInjector, String datacenterName, String hostname,
      String clusterName) {
    this.frontendConfig = frontendConfig;
    this.frontendMetrics = frontendMetrics;
    this.router = router;
    this.clusterMap = clusterMap;
    this.idConverterFactory = idConverterFactory;
    this.securityServiceFactory = securityServiceFactory;
    this.urlSigningService = urlSigningService;
    this.idSigningService = idSigningService;
    this.accountService = accountService;
    this.accountAndContainerInjector = accountAndContainerInjector;
    this.datacenterName = datacenterName;
    this.hostname = hostname;
    this.clusterName = clusterName.toLowerCase();
    getReplicasHandler = new GetReplicasHandler(frontendMetrics, clusterMap);
    logger.trace("Instantiated FrontendRestRequestService");
  }

  /**
   * @param responseHandler the {@link RestResponseHandler} that can be used to submit responses that need to be sent out.
   */
  @Override
  public void setupResponseHandler(RestResponseHandler responseHandler) {
    this.responseHandler = responseHandler;
  }

  @Override
  public void start() throws InstantiationException {
    if (responseHandler == null) {
      throw new InstantiationException("ResponseHandler is not set.");
    }
    long startupBeginTime = System.currentTimeMillis();
    idConverter = idConverterFactory.getIdConverter();
    securityService = securityServiceFactory.getSecurityService();
    getPeersHandler = new GetPeersHandler(clusterMap, securityService, frontendMetrics);
    getSignedUrlHandler =
        new GetSignedUrlHandler(urlSigningService, securityService, idConverter, accountAndContainerInjector,
            frontendMetrics, clusterMap);
    postBlobHandler =
        new PostBlobHandler(securityService, idConverter, idSigningService, router, accountAndContainerInjector,
            SystemTime.getInstance(), frontendConfig, frontendMetrics, clusterName);
    ttlUpdateHandler =
        new TtlUpdateHandler(router, securityService, idConverter, accountAndContainerInjector, frontendMetrics,
            clusterMap);
    undeleteHandler =
        new UndeleteHandler(router, securityService, idConverter, accountAndContainerInjector, frontendMetrics,
            clusterMap);
    getClusterMapSnapshotHandler = new GetClusterMapSnapshotHandler(securityService, frontendMetrics, clusterMap);
    getAccountsHandler = new GetAccountsHandler(securityService, accountService, frontendMetrics);
    postAccountsHandler = new PostAccountsHandler(securityService, accountService, frontendConfig, frontendMetrics);
    isUp = true;
    logger.info("FrontendRestRequestService has started");
    frontendMetrics.restRequestServiceStartupTimeInMs.update(System.currentTimeMillis() - startupBeginTime);
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
      logger.info("FrontendRestRequestService shutdown complete");
    } catch (IOException e) {
      logger.error("Downstream service close failed", e);
    } finally {
      frontendMetrics.restRequestServiceShutdownTimeInMs.update(System.currentTimeMillis() - shutdownBeginTime);
    }
  }

  @Override
  public void handleGet(final RestRequest restRequest, final RestResponseChannel restResponseChannel) {
    ThrowingConsumer<RequestPath> routingAction = requestPath -> {
      if (requestPath.matchesOperation(Operations.GET_PEERS)) {
        getPeersHandler.handle(restRequest, restResponseChannel,
            (result, exception) -> submitResponse(restRequest, restResponseChannel, result, exception));
      } else if (requestPath.matchesOperation(Operations.GET_CLUSTER_MAP_SNAPSHOT)) {
        getClusterMapSnapshotHandler.handle(restRequest, restResponseChannel,
            (result, exception) -> submitResponse(restRequest, restResponseChannel, result, exception));
      } else if (requestPath.matchesOperation(Operations.GET_SIGNED_URL)) {
        getSignedUrlHandler.handle(restRequest, restResponseChannel,
            (result, exception) -> submitResponse(restRequest, restResponseChannel, result, exception));
      } else if (requestPath.matchesOperation(Operations.ACCOUNTS)) {
        getAccountsHandler.handle(restRequest, restResponseChannel,
            (result, exception) -> submitResponse(restRequest, restResponseChannel, result, exception));
      } else {
        SubResource subResource = requestPath.getSubResource();
        GetBlobOptions options = buildGetBlobOptions(restRequest.getArgs(), subResource,
            getGetOption(restRequest, frontendConfig.defaultRouterGetOption), requestPath.getBlobSegmentIdx());
        GetCallback routerCallback = new GetCallback(restRequest, restResponseChannel, subResource, options);
        SecurityProcessRequestCallback securityCallback =
            new SecurityProcessRequestCallback(restRequest, restResponseChannel, routerCallback);
        if (subResource == SubResource.Replicas) {
          securityCallback = new SecurityProcessRequestCallback(restRequest, restResponseChannel);
        }
        RestRequestMetrics restRequestMetrics =
            getMetricsGroupForGet(frontendMetrics, subResource).getRestRequestMetrics(restRequest.isSslUsed(), false);
        restRequest.getMetricsTracker().injectMetrics(restRequestMetrics);
        securityService.processRequest(restRequest, securityCallback);
      }
    };
    preProcessAndRouteRequest(restRequest, restResponseChannel, frontendMetrics.getPreProcessingMetrics, routingAction);
  }

  @Override
  public void handlePost(RestRequest restRequest, RestResponseChannel restResponseChannel) {
    ThrowingConsumer<RequestPath> routingAction = requestPath -> {
      if (requestPath.matchesOperation(Operations.ACCOUNTS)) {
        postAccountsHandler.handle(restRequest, restResponseChannel,
            (result, exception) -> submitResponse(restRequest, restResponseChannel, null, exception));
      } else {
        postBlobHandler.handle(restRequest, restResponseChannel,
            (result, exception) -> submitResponse(restRequest, restResponseChannel, null, exception));
      }
    };
    preProcessAndRouteRequest(restRequest, restResponseChannel, frontendMetrics.postPreProcessingMetrics,
        routingAction);
  }

  @Override
  public void handlePut(RestRequest restRequest, RestResponseChannel restResponseChannel) {
    ThrowingConsumer<RequestPath> routingAction = requestPath -> {
      if (requestPath.matchesOperation(Operations.UPDATE_TTL)) {
        ttlUpdateHandler.handle(restRequest, restResponseChannel, (r, e) -> {
          if (e instanceof RouterException
              && ((RouterException) e).getErrorCode() == RouterErrorCode.BlobUpdateNotAllowed) {
            restResponseChannel.setHeader(Headers.ALLOW, TTL_UPDATE_REJECTED_ALLOW_HEADER_VALUE);
          }
          submitResponse(restRequest, restResponseChannel, null, e);
        });
      } else if (requestPath.matchesOperation(Operations.UNDELETE) && frontendConfig.enableUndelete) {
        // If the undelete is not enabled, then treat it as unrecognized operation.
        undeleteHandler.handle(restRequest, restResponseChannel, (r, e) -> {
          submitResponse(restRequest, restResponseChannel, null, e);
        });
      } else {
        throw new RestServiceException("Unrecognized operation: " + requestPath.getOperationOrBlobId(false),
            RestServiceErrorCode.BadRequest);
      }
    };
    preProcessAndRouteRequest(restRequest, restResponseChannel, frontendMetrics.putPreProcessingMetrics, routingAction);
  }

  @Override
  public void handleDelete(RestRequest restRequest, RestResponseChannel restResponseChannel) {
    ThrowingConsumer<RequestPath> routingAction = requestPath -> {
      RestRequestMetrics requestMetrics =
          frontendMetrics.deleteBlobMetricsGroup.getRestRequestMetrics(restRequest.isSslUsed(), false);
      restRequest.getMetricsTracker().injectMetrics(requestMetrics);
      DeleteCallback routerCallback = new DeleteCallback(restRequest, restResponseChannel);
      SecurityProcessRequestCallback securityCallback =
          new SecurityProcessRequestCallback(restRequest, restResponseChannel, routerCallback);
      securityService.processRequest(restRequest, securityCallback);
    };
    preProcessAndRouteRequest(restRequest, restResponseChannel, frontendMetrics.deletePreProcessingMetrics,
        routingAction);
  }

  @Override
  public void handleHead(RestRequest restRequest, RestResponseChannel restResponseChannel) {
    ThrowingConsumer<RequestPath> routingAction = requestPath -> {
      RestRequestMetrics requestMetrics =
          frontendMetrics.headBlobMetricsGroup.getRestRequestMetrics(restRequest.isSslUsed(), false);
      restRequest.getMetricsTracker().injectMetrics(requestMetrics);
      HeadCallback routerCallback = new HeadCallback(restRequest, restResponseChannel);
      SecurityProcessRequestCallback securityCallback =
          new SecurityProcessRequestCallback(restRequest, restResponseChannel, routerCallback);
      securityService.processRequest(restRequest, securityCallback);
    };
    preProcessAndRouteRequest(restRequest, restResponseChannel, frontendMetrics.headPreProcessingMetrics,
        routingAction);
  }

  @Override
  public void handleOptions(RestRequest restRequest, RestResponseChannel restResponseChannel) {
    long processingStartTime = System.currentTimeMillis();
    handlePrechecks(restRequest, restResponseChannel);
    RestRequestMetrics requestMetrics =
        frontendMetrics.optionsMetricsGroup.getRestRequestMetrics(restRequest.isSslUsed(), false);
    restRequest.getMetricsTracker().injectMetrics(requestMetrics);
    Exception exception = null;
    try {
      logger.trace("Handling OPTIONS request - {}", restRequest.getUri());
      checkAvailable();
      // TODO: make this non blocking once all handling of indiviual methods is moved to their own classes
      securityService.preProcessRequest(restRequest).get();
      restRequest.setArg(REQUEST_PATH,
          RequestPath.parse(restRequest, frontendConfig.pathPrefixesToRemove, clusterName));
      long preProcessingEndTime = System.currentTimeMillis();
      frontendMetrics.optionsPreProcessingTimeInMs.update(preProcessingEndTime - processingStartTime);

      // making this blocking for now. TODO: convert to non blocking
      securityService.processRequest(restRequest).get();
      long securityRequestProcessingEndTime = System.currentTimeMillis();
      frontendMetrics.optionsSecurityRequestTimeInMs.update(securityRequestProcessingEndTime - preProcessingEndTime);

      restResponseChannel.setStatus(ResponseStatus.Ok);
      restResponseChannel.setHeader(Headers.DATE, new GregorianCalendar().getTime());
      restResponseChannel.setHeader(Headers.CONTENT_LENGTH, 0);
      restResponseChannel.setHeader(Headers.ACCESS_CONTROL_ALLOW_METHODS, frontendConfig.optionsAllowMethods);
      restResponseChannel.setHeader(Headers.ACCESS_CONTROL_MAX_AGE, frontendConfig.optionsValiditySeconds);
      securityService.processResponse(restRequest, restResponseChannel, null).get();
      long securityResponseProcessingEndTime = System.currentTimeMillis();
      frontendMetrics.optionsSecurityResponseTimeInMs.update(
          securityResponseProcessingEndTime - securityRequestProcessingEndTime);
    } catch (Exception e) {
      exception = Utils.extractExecutionExceptionCause(e);
    }
    submitResponse(restRequest, restResponseChannel, null, exception);
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
      if (restRequest.getArgs().containsKey(InternalKeys.SEND_TRACKING_INFO) && (Boolean) restRequest.getArgs()
          .get(InternalKeys.SEND_TRACKING_INFO)) {
        restResponseChannel.setHeader(TrackingHeaders.DATACENTER_NAME, datacenterName);
        restResponseChannel.setHeader(TrackingHeaders.FRONTEND_NAME, hostname);
      }
      if (exception instanceof RouterException) {
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
   * Handle any basic checks, call {@link SecurityService#preProcessRequest}, and parse the URI to find a blob ID or
   * operation, and maybe a sub-resource. {@code routingAction} is used to choose how to handle the request based on
   * parsed URI.
   * @param restRequest the {@link RestRequest}.
   * @param restResponseChannel the {@link RestResponseChannel}.
   * @param preProcessingMetrics metrics instance for recording pre-processing time.
   * @param routingAction called with the parsed {@link RequestPath} as an argument. Used to start request handling
   *                      based on operation type.
   */
  private void preProcessAndRouteRequest(RestRequest restRequest, RestResponseChannel restResponseChannel,
      AsyncOperationTracker.Metrics preProcessingMetrics, ThrowingConsumer<RequestPath> routingAction) {
    handlePrechecks(restRequest, restResponseChannel);
    Callback<Void> errorCallback = (r, e) -> submitResponse(restRequest, restResponseChannel, null, e);
    try {
      logger.trace("Handling {} request - {}", restRequest.getRestMethod(), restRequest.getUri());
      checkAvailable();
      securityService.preProcessRequest(restRequest, FrontendUtils.buildCallback(preProcessingMetrics, r -> {
        RequestPath requestPath = RequestPath.parse(restRequest, frontendConfig.pathPrefixesToRemove, clusterName);
        restRequest.setArg(REQUEST_PATH, requestPath);
        routingAction.accept(requestPath);
      }, restRequest.getUri(), logger, errorCallback));
    } catch (Exception e) {
      errorCallback.onCompletion(null, e);
    }
  }

  /**
   * Fetch {@link RestRequestMetricsGroup} for GetRequest based on the {@link SubResource}.
   * @param frontendMetrics instance of {@link FrontendMetrics} to use
   * @param subResource {@link SubResource} corresponding to the GetRequest
   * @return the appropriate {@link RestRequestMetricsGroup} based on the given params
   */
  private static RestRequestMetricsGroup getMetricsGroupForGet(FrontendMetrics frontendMetrics,
      SubResource subResource) {
    RestRequestMetricsGroup group = null;
    if (subResource == null || subResource.equals(SubResource.Segment)) {
      group = frontendMetrics.getBlobMetricsGroup;
    } else {
      switch (subResource) {
        case BlobInfo:
          group = frontendMetrics.getBlobInfoMetricsGroup;
          break;
        case UserMetadata:
          group = frontendMetrics.getUserMetadataMetricsGroup;
          break;
        case Replicas:
          group = frontendMetrics.getReplicasMetricsGroup;
          break;
      }
    }
    return group;
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
   * Checks if {@link FrontendRestRequestService} is available to serve requests.
   * @throws RestServiceException if {@link FrontendRestRequestService} is not available to serve requests.
   */
  private void checkAvailable() throws RestServiceException {
    if (!isUp) {
      throw new RestServiceException("FrontendRestRequestService unavailable", RestServiceErrorCode.ServiceUnavailable);
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
     * Calls {@link SecurityService#postProcessRequest} once ID conversion is completed.
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
          logger.trace("Handling {} of {}", restMethod, result);
          BlobId blobId = FrontendUtils.getBlobIdFromString(result, clusterMap);
          RestRequestMetricsGroup metricsGroup = null;
          if (getCallback != null) {
            metricsGroup = getMetricsGroupForGet(frontendMetrics, getRequestPath(restRequest).getSubResource());
          } else if (headCallback != null) {
            metricsGroup = frontendMetrics.headBlobMetricsGroup;
          } else if (deleteCallback != null) {
            metricsGroup = frontendMetrics.deleteBlobMetricsGroup;
          }
          accountAndContainerInjector.injectTargetAccountAndContainerFromBlobId(blobId, restRequest, metricsGroup);
          securityService.postProcessRequest(restRequest,
              securityPostProcessRequestCallback(result, restRequest, restResponseChannel, getCallback, headCallback,
                  deleteCallback));
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
    private static final String PROCESS_DELETE = "DELETE Request Security";

    private final RestRequest restRequest;
    private final RestResponseChannel restResponseChannel;
    private final CallbackTracker callbackTracker;

    private GetCallback getCallback;
    private HeadCallback headCallback;
    private DeleteCallback deleteCallback;

    /**
     * Constructor for GETs that will eventually reach the {@link Router}.
     */
    SecurityProcessRequestCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
        GetCallback callback) {
      this(restRequest, restResponseChannel, PROCESS_GET, frontendMetrics.getSecurityRequestTimeInMs,
          frontendMetrics.getSecurityRequestCallbackProcessingTimeInMs);
      this.getCallback = callback;
    }

    /**
     * Constructor for GETs that will not be sent to the {@link Router}.
     */
    SecurityProcessRequestCallback(RestRequest restRequest, RestResponseChannel restResponseChannel) {
      this(restRequest, restResponseChannel, PROCESS_GET, frontendMetrics.getSecurityRequestTimeInMs,
          frontendMetrics.getSecurityRequestCallbackProcessingTimeInMs);
    }

    /**
     * Constructor for HEAD that will eventually reach the {@link Router}.
     */
    SecurityProcessRequestCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
        HeadCallback callback) {
      this(restRequest, restResponseChannel, PROCESS_HEAD, frontendMetrics.headSecurityRequestTimeInMs,
          frontendMetrics.headSecurityRequestCallbackProcessingTimeInMs);
      this.headCallback = callback;
    }

    /**
     * Constructor for DELETE that will eventually reach the {@link Router}.
     */
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
          String receivedId = getRequestPath(restRequest).getOperationOrBlobId(false);
          switch (restMethod) {
            case GET:
              InboundIdConverterCallback idConverterCallback =
                  new InboundIdConverterCallback(restRequest, restResponseChannel, getCallback);
              idConverter.convert(restRequest, receivedId, idConverterCallback);
              break;
            case HEAD:
              idConverterCallback = new InboundIdConverterCallback(restRequest, restResponseChannel, headCallback);
              idConverter.convert(restRequest, receivedId, idConverterCallback);
              break;
            case DELETE:
              idConverterCallback = new InboundIdConverterCallback(restRequest, restResponseChannel, deleteCallback);
              idConverter.convert(restRequest, receivedId, idConverterCallback);
              break;
            default:
              exception = new IllegalStateException("Unrecognized RestMethod: " + restMethod);
          }
        } catch (Exception e) {
          exception = Utils.extractExecutionExceptionCause(e);
        }
      }

      if (exception != null) {
        submitResponse(restRequest, restResponseChannel, null, exception);
      }
      callbackTracker.markCallbackProcessingEnd();
    }
  }

  /**
   * Build a callback to use for {@link SecurityService#postProcessRequest}. This callback forwards request to the
   * {@link Router} once ID conversion is completed. In the case of some sub-resources
   * (e.g., {@link SubResource#Replicas}), the request is completed and not forwarded to the {@link Router}.
   * @param convertedId the converted blob ID to use in router requests.
   * @param restRequest the {@link RestRequest}.
   * @param restResponseChannel the {@link RestResponseChannel}.
   * @param getCallback the {@link GetCallback} to use if this is a {@link RestMethod#GET} request, or null for other
   *                    request types.
   * @param headCallback the {@link HeadCallback} to use if this is a {@link RestMethod#HEAD} request, or null for other
   *                    request types.
   * @param deleteCallback the {@link DeleteCallback} to use if this is a {@link RestMethod#DELETE} request, or null for
   *                       other request types.
   * @return the {@link Callback} to use.
   */
  private Callback<Void> securityPostProcessRequestCallback(String convertedId, RestRequest restRequest,
      RestResponseChannel restResponseChannel, GetCallback getCallback, HeadCallback headCallback,
      DeleteCallback deleteCallback) {
    Callback<ReadableStreamChannel> completionCallback =
        (result, exception) -> submitResponse(restRequest, restResponseChannel, result, exception);
    RestMethod restMethod = restRequest.getRestMethod();
    AsyncOperationTracker.Metrics metrics;
    switch (restMethod) {
      case GET:
        metrics = frontendMetrics.getSecurityPostProcessRequestMetrics;
        break;
      case HEAD:
        metrics = frontendMetrics.headSecurityPostProcessRequestMetrics;
        break;
      case DELETE:
        metrics = frontendMetrics.deleteSecurityPostProcessRequestMetrics;
        break;
      default:
        throw new IllegalStateException("Unrecognized RestMethod: " + restMethod);
    }
    return FrontendUtils.buildCallback(metrics, result -> {
      ReadableStreamChannel response = null;
      switch (restMethod) {
        case GET:
          SubResource subResource = getRequestPath(restRequest).getSubResource();
          // inject encryption metrics if need be
          if (BlobId.isEncrypted(convertedId)) {
            RestRequestMetrics restRequestMetrics =
                getMetricsGroupForGet(frontendMetrics, subResource).getRestRequestMetrics(restRequest.isSslUsed(),
                    true);
            restRequest.getMetricsTracker().injectMetrics(restRequestMetrics);
          }
          if (subResource == null) {
            getCallback.markStartTime();
            router.getBlob(convertedId, getCallback.options, getCallback);
          } else {
            switch (subResource) {
              case BlobInfo:
              case UserMetadata:
              case Segment:
                getCallback.markStartTime();
                router.getBlob(convertedId, getCallback.options, getCallback);
                break;
              case Replicas:
                response = getReplicasHandler.getReplicas(convertedId, restResponseChannel);
                break;
            }
          }
          break;
        case HEAD:
          GetOption getOption = getGetOption(restRequest, frontendConfig.defaultRouterGetOption);
          // inject encryption metrics if need be
          if (BlobId.isEncrypted(convertedId)) {
            RestRequestMetrics requestMetrics =
                frontendMetrics.headBlobMetricsGroup.getRestRequestMetrics(restRequest.isSslUsed(), true);
            restRequest.getMetricsTracker().injectMetrics(requestMetrics);
          }
          headCallback.markStartTime();
          router.getBlob(convertedId, new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.BlobInfo)
              .getOption(getOption)
              .build(), headCallback);
          break;
        case DELETE:
          deleteCallback.markStartTime();
          router.deleteBlob(convertedId, getHeader(restRequest.getArgs(), Headers.SERVICE_ID, false), deleteCallback);
          break;
        default:
          throw new IllegalStateException("Unrecognized RestMethod: " + restMethod);
      }
      if (response != null) {
        completionCallback.onCompletion(response, null);
      }
    }, restRequest.getUri(), logger, completionCallback);
  }

  /**
   * Tracks metrics and logs progress of operations that accept callbacks.
   */
  private class CallbackTracker {
    private long operationStartTime = 0;
    private long processingStartTime = 0;

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
      this.operationType = operationType;
      this.operationTimeTracker = operationTimeTracker;
      this.callbackProcessingTimeTracker = callbackProcessingTimeTracker;
      blobId = getRequestPath(restRequest).getOperationOrBlobId(false);
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
    private final SubResource subResource;
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
    GetCallback(RestRequest restRequest, RestResponseChannel restResponseChannel, SubResource subResource,
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
          accountAndContainerInjector.ensureAccountAndContainerInjected(restRequest,
              routerResult.getBlobInfo().getBlobProperties(), getMetricsGroupForGet(frontendMetrics, subResource));
          securityCallbackTracker.markOperationStart();
          securityService.processResponse(restRequest, restResponseChannel, routerResult.getBlobInfo(),
              (securityResult, securityException) -> {
                securityCallbackTracker.markOperationEnd();
                ReadableStreamChannel response = routerResult.getBlobDataChannel();
                try {
                  if (securityException == null) {
                    if (subResource != null && !subResource.equals(SubResource.Segment)) {
                      BlobInfo blobInfo = routerResult.getBlobInfo();
                      if (restRequest.getArgs().containsKey(SEND_USER_METADATA_AS_RESPONSE_BODY)
                          && (boolean) restRequest.getArgs().get(SEND_USER_METADATA_AS_RESPONSE_BODY)) {
                        restResponseChannel.setHeader(Headers.CONTENT_TYPE, "application/octet-stream");
                        restResponseChannel.setHeader(Headers.CONTENT_LENGTH, blobInfo.getUserMetadata().length);
                        response = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(blobInfo.getUserMetadata()));
                      } else {
                        restResponseChannel.setHeader(Headers.CONTENT_LENGTH, 0);
                        response = new ByteBufferReadableStreamChannel(EMPTY_BUFFER);
                      }
                    } else if (restResponseChannel.getStatus() == ResponseStatus.NotModified) {
                      response = null;
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
  }

  /**
   * Callback for DELETE operations. Sends an ACCEPTED response to the client if operation is successful. Submits
   * response either to handle exceptions or to clean up after a response.
   */
  private class DeleteCallback implements Callback<Void> {
    private final RestRequest restRequest;
    private final RestResponseChannel restResponseChannel;
    private final CallbackTracker callbackTracker;

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
          restResponseChannel.setHeader(Headers.DATE, new GregorianCalendar().getTime());
          restResponseChannel.setStatus(ResponseStatus.Accepted);
          restResponseChannel.setHeader(Headers.CONTENT_LENGTH, 0);
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
          accountAndContainerInjector.ensureAccountAndContainerInjected(restRequest,
              routerResult.getBlobInfo().getBlobProperties(), frontendMetrics.headBlobMetricsGroup);
          securityCallbackTracker.markOperationStart();
          securityService.processResponse(restRequest, restResponseChannel, routerResult.getBlobInfo(),
              (securityResult, securityException) -> {
                callbackTracker.markOperationEnd();
                submitResponse(restRequest, restResponseChannel, null, securityException);
                callbackTracker.markCallbackProcessingEnd();
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
