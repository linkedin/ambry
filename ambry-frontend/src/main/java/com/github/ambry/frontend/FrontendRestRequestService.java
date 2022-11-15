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

import com.github.ambry.account.AccountService;
import com.github.ambry.accountstats.AccountStatsStore;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.Callback;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.named.NamedBlobDb;
import com.github.ambry.quota.QuotaManager;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.ResponseStatus;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestRequestMetrics;
import com.github.ambry.rest.RestRequestService;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestResponseHandler;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.router.Router;
import com.github.ambry.router.RouterErrorCode;
import com.github.ambry.router.RouterException;
import com.github.ambry.utils.AsyncOperationTracker;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.ThrowingConsumer;
import com.github.ambry.utils.Utils;
import java.io.IOException;
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

  private final Router router;
  private final IdConverterFactory idConverterFactory;
  private final SecurityServiceFactory securityServiceFactory;
  private final ClusterMap clusterMap;
  private final FrontendConfig frontendConfig;
  private final FrontendMetrics frontendMetrics;
  private final UrlSigningService urlSigningService;
  private final IdSigningService idSigningService;
  private final NamedBlobDb namedBlobDb;
  private final AccountService accountService;
  private final AccountAndContainerInjector accountAndContainerInjector;
  private final AccountStatsStore accountStatsStore;
  private static final Logger logger = LoggerFactory.getLogger(FrontendRestRequestService.class);
  private final String datacenterName;
  private final String hostname;
  private final String clusterName;
  private RestResponseHandler responseHandler;
  private IdConverter idConverter = null;
  private SecurityService securityService = null;
  private GetPeersHandler getPeersHandler;
  private GetSignedUrlHandler getSignedUrlHandler;
  private NamedBlobListHandler namedBlobListHandler;
  //private NamedBlobGetHandler namedBlobGetHandler;
  //private NamedBlobDeleteHandler namedBlobDeleteHandler;
  private NamedBlobPutHandler namedBlobPutHandler;
  private GetBlobHandler getBlobHandler;
  private PostBlobHandler postBlobHandler;
  private TtlUpdateHandler ttlUpdateHandler;
  private DeleteBlobHandler deleteBlobHandler;
  private HeadBlobHandler headBlobHandler;
  private UndeleteHandler undeleteHandler;
  private GetClusterMapSnapshotHandler getClusterMapSnapshotHandler;
  private GetAccountsHandler getAccountsHandler;
  private PostAccountsHandler postAccountsHandler;
  private GetStatsReportHandler getStatsReportHandler;
  private QuotaManager quotaManager;
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
   * @param namedBlobDb the {@link NamedBlobDb} for named blob metadata operations.
   * @param accountService the {@link AccountService} to use.
   * @param accountAndContainerInjector the {@link AccountAndContainerInjector} to use.
   * @param datacenterName the local datacenter name for this frontend.
   * @param hostname the hostname for this frontend.
   * @param clusterName the name of the storage cluster that the router communicates with.
   * @param accountStatsStore the {@link AccountStatsStore} used to fetch aggregated stats reports.
   */
  FrontendRestRequestService(FrontendConfig frontendConfig, FrontendMetrics frontendMetrics, Router router,
      ClusterMap clusterMap, IdConverterFactory idConverterFactory, SecurityServiceFactory securityServiceFactory,
      UrlSigningService urlSigningService, IdSigningService idSigningService, NamedBlobDb namedBlobDb,
      AccountService accountService, AccountAndContainerInjector accountAndContainerInjector, String datacenterName,
      String hostname, String clusterName, AccountStatsStore accountStatsStore, QuotaManager quotaManager) {
    this.frontendConfig = frontendConfig;
    this.frontendMetrics = frontendMetrics;
    this.router = router;
    this.clusterMap = clusterMap;
    this.idConverterFactory = idConverterFactory;
    this.securityServiceFactory = securityServiceFactory;
    this.urlSigningService = urlSigningService;
    this.idSigningService = idSigningService;
    this.namedBlobDb = namedBlobDb;
    this.accountService = accountService;
    this.accountAndContainerInjector = accountAndContainerInjector;
    this.accountStatsStore = accountStatsStore;
    this.datacenterName = datacenterName;
    this.hostname = hostname;
    this.quotaManager = quotaManager;
    this.clusterName = clusterName.toLowerCase();
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
    try {
      quotaManager.init();
    } catch (Exception e) {
      throw new InstantiationException("FrontendRestRequestService Instantiation failed due to: " + e.getMessage());
    }
    idConverter = idConverterFactory.getIdConverter();
    securityService = securityServiceFactory.getSecurityService();
    getPeersHandler = new GetPeersHandler(clusterMap, securityService, frontendMetrics);
    getSignedUrlHandler =
        new GetSignedUrlHandler(urlSigningService, securityService, idConverter, accountAndContainerInjector,
            frontendMetrics, clusterMap);

    getBlobHandler =
        new GetBlobHandler(frontendConfig, router, securityService, idConverter, accountAndContainerInjector,
            frontendMetrics, clusterMap, quotaManager);
    postBlobHandler =
        new PostBlobHandler(securityService, idConverter, idSigningService, router, accountAndContainerInjector,
            SystemTime.getInstance(), frontendConfig, frontendMetrics, clusterName, quotaManager);

    ttlUpdateHandler =
        new TtlUpdateHandler(router, securityService, idConverter, accountAndContainerInjector, frontendMetrics,
            clusterMap, quotaManager);
    deleteBlobHandler =
        new DeleteBlobHandler(router, securityService, idConverter, accountAndContainerInjector, frontendMetrics,
            clusterMap, quotaManager);
    headBlobHandler =
        new HeadBlobHandler(frontendConfig, router, securityService, idConverter, accountAndContainerInjector,
            frontendMetrics, clusterMap, quotaManager);
    undeleteHandler =
        new UndeleteHandler(router, securityService, idConverter, accountAndContainerInjector, frontendMetrics,
            clusterMap, quotaManager);

    namedBlobListHandler =
        new NamedBlobListHandler(securityService, namedBlobDb, accountAndContainerInjector, frontendMetrics);
    namedBlobPutHandler =
        new NamedBlobPutHandler(securityService, namedBlobDb, idConverter, idSigningService, router, accountAndContainerInjector,
            frontendConfig, frontendMetrics, clusterName, quotaManager);

    getClusterMapSnapshotHandler = new GetClusterMapSnapshotHandler(securityService, frontendMetrics, clusterMap);
    getAccountsHandler = new GetAccountsHandler(securityService, accountService, frontendMetrics);
    getStatsReportHandler = new GetStatsReportHandler(securityService, frontendMetrics, accountStatsStore);
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
      if (quotaManager != null) {
        quotaManager.shutdown();
        quotaManager = null;
      }
      if (securityService != null) {
        securityService.close();
        securityService = null;
      }
      if (idConverter != null) {
        idConverter.close();
        idConverter = null;
      }
      if (accountStatsStore != null) {
        accountStatsStore.shutdown();
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
      } else if (requestPath.matchesOperation(Operations.STATS_REPORT)) {
        getStatsReportHandler.handle(restRequest, restResponseChannel,
            (result, exception) -> submitResponse(restRequest, restResponseChannel, result, exception));
      } else if (requestPath.matchesOperation(Operations.NAMED_BLOB)
          && NamedBlobPath.parse(requestPath, restRequest.getArgs()).getBlobName() == null) {
        namedBlobListHandler.handle(restRequest, restResponseChannel,
            (result, exception) -> submitResponse(restRequest, restResponseChannel, result, exception));
      } else {
        getBlobHandler.handle(requestPath, restRequest, restResponseChannel, (r, e) -> {
          submitResponse(restRequest, restResponseChannel, r, e);
        });
      }
    };
    preProcessAndRouteRequest(restRequest, restResponseChannel, frontendMetrics.getPreProcessingMetrics, routingAction);
  }

  @Override
  public void handlePost(RestRequest restRequest, RestResponseChannel restResponseChannel) {
    ThrowingConsumer<RequestPath> routingAction = requestPath -> {
      if (requestPath.matchesOperation(Operations.ACCOUNTS)) {
        postAccountsHandler.handle(restRequest, restResponseChannel,
            (result, exception) -> submitResponse(restRequest, restResponseChannel, result, exception));
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
      } else if (requestPath.matchesOperation(Operations.NAMED_BLOB)) {
        namedBlobPutHandler.handle(restRequest, restResponseChannel,
            (r, e) -> submitResponse(restRequest, restResponseChannel, null, e));
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
      deleteBlobHandler.handle(restRequest, restResponseChannel, (r, e) -> {
        submitResponse(restRequest, restResponseChannel, null, e);
      });
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

      headBlobHandler.handle(restRequest, restResponseChannel, (r, e) -> {
        submitResponse(restRequest, restResponseChannel, null, e);
      });
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
      exception = Utils.extractFutureExceptionCause(e);
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
}
