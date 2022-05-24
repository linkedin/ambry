package com.github.ambry.frontend;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.Callback;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.quota.QuotaManager;
import com.github.ambry.quota.QuotaUtils;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestRequestMetrics;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.GetBlobOptions;
import com.github.ambry.router.GetBlobOptionsBuilder;
import com.github.ambry.router.GetBlobResult;
import com.github.ambry.router.Router;
import java.util.GregorianCalendar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.frontend.FrontendUtils.*;
import static com.github.ambry.rest.RestUtils.*;


public class HeadHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(HeadHandler.class);

  private final FrontendConfig frontendConfig;
  private final Router router;
  private final SecurityService securityService;
  private final IdConverter idConverter;
  private final AccountAndContainerInjector accountAndContainerInjector;
  private final FrontendMetrics metrics;
  private final ClusterMap clusterMap;
  private final QuotaManager quotaManager;

  HeadHandler(FrontendConfig frontendConfig, Router router, SecurityService securityService, IdConverter idConverter,
      AccountAndContainerInjector accountAndContainerInjector, FrontendMetrics metrics, ClusterMap clusterMap,
      QuotaManager quotaManager) {
    this.frontendConfig = frontendConfig;
    this.router = router;
    this.securityService = securityService;
    this.idConverter = idConverter;
    this.accountAndContainerInjector = accountAndContainerInjector;
    this.metrics = metrics;
    this.clusterMap = clusterMap;
    this.quotaManager = quotaManager;
  }

  void handle(RestRequest restRequest, RestResponseChannel restResponseChannel, Callback<Void> callback) {
    new CallbackChain(restRequest, restResponseChannel, callback).start();
  }

  private class CallbackChain {
    private final RestRequest restRequest;
    private final RestResponseChannel restResponseChannel;
    private final Callback<Void> finalCallback;

    /**
     * @param restRequest the {@link RestRequest}.
     * @param restResponseChannel the {@link RestResponseChannel}.
     * @param finalCallback the {@link Callback} to call on completion.
     */
    private CallbackChain(RestRequest restRequest, RestResponseChannel restResponseChannel,
        Callback<Void> finalCallback) {
      this.restRequest = restRequest;
      this.restResponseChannel = restResponseChannel;
      this.finalCallback = finalCallback;
    }

    /**
     * Start the chain by calling {@link SecurityService#processRequest}.
     */
    private void start() {
      restRequest.setArg(RestUtils.InternalKeys.KEEP_ALIVE_ON_ERROR_HINT, true);
      securityService.processRequest(restRequest, securityProcessRequestCallback());
    }

    /**
     * After {@link SecurityService#processRequest} finishes, call {@link IdConverter#convert} to convert the incoming
     * ID if required.
     * @return a {@link Callback} to be used with {@link SecurityService#processRequest}.
     */
    private Callback<Void> securityProcessRequestCallback() {
      return buildCallback(metrics.headBlobSecurityProcessRequestMetrics, result -> {
        String blobIdStr = RestUtils.getHeader(restRequest.getArgs(), RestUtils.Headers.BLOB_ID, true);
        idConverter.convert(restRequest, blobIdStr, idConverterCallback());
      }, restRequest.getUri(), LOGGER, finalCallback);
    }

    /**
     * After {@link IdConverter#convert} finishes, call {@link SecurityService#postProcessRequest} to perform
     * request time security checks that rely on the request being fully parsed and any additional arguments set.
     * @return a {@link Callback} to be used with {@link IdConverter#convert}.
     */
    private Callback<String> idConverterCallback() {
      return buildCallback(metrics.headBlobIdConversionMetrics, convertedBlobId -> {
        BlobId blobId = FrontendUtils.getBlobIdFromString(convertedBlobId, clusterMap);
        accountAndContainerInjector.injectTargetAccountAndContainerFromBlobId(blobId, restRequest,
            metrics.headBlobMetricsGroup);
        securityService.postProcessRequest(restRequest, securityPostProcessRequestCallback(blobId));
      }, restRequest.getUri(), LOGGER, finalCallback);
    }

    /**
     * After {@link SecurityService#postProcessRequest} finishes, call {@link Router#getBlob} to dget
     * the blob info from the storage layer.
     * @param blobId the {@link BlobId} to get info
     * @return a {@link Callback} to be used with {@link SecurityService#postProcessRequest}.
     */
    private Callback<Void> securityPostProcessRequestCallback(BlobId blobId) {
      return buildCallback(metrics.headBlobSecurityPostProcessRequestMetrics, result -> {
        GetOption getOption = getGetOption(restRequest, frontendConfig.defaultRouterGetOption);
        // inject encryption metrics if need be
        if (BlobId.isEncrypted(blobId.getID())) {
          RestRequestMetrics requestMetrics =
              metrics.headBlobMetricsGroup.getRestRequestMetrics(restRequest.isSslUsed(), true);
          restRequest.getMetricsTracker().injectMetrics(requestMetrics);
        }
        router.getBlob(blobId.getID(), new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.BlobInfo)
            .getOption(getOption)
            .restRequest(restRequest)
            .build(), routerCallback(), QuotaUtils.buildQuotaChargeCallback(restRequest, quotaManager, false));
      }, restRequest.getUri(), LOGGER, finalCallback);
    }

    /**
     * After {@link Router#getBlob} finishes, call {@link SecurityService#processResponse}.
     * @return a {@link Callback} to be used with {@link Router#getBlob}.
     */
    private Callback<GetBlobResult> routerCallback() {
      return buildCallback(metrics.headBlobRouterMetrics, result -> {
        LOGGER.debug("Head {}", RestUtils.getHeader(restRequest.getArgs(), RestUtils.Headers.BLOB_ID, true));
        restResponseChannel.setHeader(RestUtils.Headers.DATE, new GregorianCalendar().getTime());
        restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, 0);
        accountAndContainerInjector.ensureAccountAndContainerInjected(restRequest,
            result.getBlobInfo().getBlobProperties(), metrics.headBlobMetricsGroup);
        securityService.processResponse(restRequest, restResponseChannel, result.getBlobInfo(), securityProcessResponseCallback());
      }, restRequest.getUri(), LOGGER, finalCallback);
    }

    /**
     * After {@link SecurityService#processResponse}, call {@code finalCallback}.
     * @return a {@link Callback} to be used with {@link SecurityService#processResponse}.
     */
    private Callback<Void> securityProcessResponseCallback() {
      return buildCallback(metrics.headBlobSecurityProcessResponseMetrics,
          securityCheckResult -> finalCallback.onCompletion(null, null), restRequest.getUri(), LOGGER, finalCallback);
    }
  }
}
