package com.github.ambry.frontend;

import com.github.ambry.account.AccountService;
import com.github.ambry.account.AccountServiceException;
import com.github.ambry.account.Dataset;
import com.github.ambry.account.DatasetVersionRecord;
import com.github.ambry.commons.Callback;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.ReadableStreamChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.frontend.FrontendUtils.*;
import static com.github.ambry.rest.RestUtils.*;
import static com.github.ambry.rest.RestUtils.InternalKeys.*;


public class DatasetVersionGetHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(DatasetVersionGetHandler.class);
  private final SecurityService securityService;
  private final AccountAndContainerInjector accountAndContainerInjector;
  private final FrontendMetrics metrics;
  private final NamedBlobGetHandler namedBlobGetHandler;
  private final AccountService accountService;

  public DatasetVersionGetHandler(SecurityService securityService,
      AccountAndContainerInjector accountAndContainerInjector, FrontendMetrics metrics,
      NamedBlobGetHandler namedBlobGetHandler, AccountService accountService) {
    this.securityService = securityService;
    this.accountAndContainerInjector = accountAndContainerInjector;
    this.metrics = metrics;
    this.namedBlobGetHandler = namedBlobGetHandler;
    this.accountService = accountService;
  }

  void handle(RestRequest restRequest, RestResponseChannel restResponseChannel,
      Callback<ReadableStreamChannel> callback) throws RestServiceException {
    RequestPath requestPath = getRequestPath(restRequest);
    accountAndContainerInjector.injectAccountContainerForNamedBlob(restRequest,
        getMetricsGroupForGet(metrics, requestPath.getSubResource()));

    securityService.checkAccess(restRequest, "DatasetVersionGetHandler");
    accountAndContainerInjector.injectDatasetForNamedBlob(restRequest);
    Dataset dataset = (Dataset) restRequest.getArgs().get(InternalKeys.TARGET_DATASET);
    long startGetDatasetVersionTime = System.currentTimeMillis();
    String accountName = dataset.getAccountName();
    String containerName = dataset.getContainerName();
    String datasetName = dataset.getDatasetName();
    String version = (String) restRequest.getArgs().get(TARGET_DATASET_VERSION);
    try {
      DatasetVersionRecord datasetVersionRecord =
          accountService.getDatasetVersion(accountName, containerName, datasetName, version);
      FrontendUtils.replaceRequestPathWithNewOperationOrBlobIdIfNeeded(restRequest, datasetVersionRecord, version);
      restResponseChannel.setHeader(RestUtils.Headers.TARGET_ACCOUNT_NAME, accountName);
      restResponseChannel.setHeader(RestUtils.Headers.TARGET_CONTAINER_NAME, containerName);
      restResponseChannel.setHeader(RestUtils.Headers.TARGET_DATASET_NAME, datasetName);
      restResponseChannel.setHeader(RestUtils.Headers.TARGET_DATASET_VERSION, datasetVersionRecord.getVersion());
      metrics.getDatasetVersionProcessingTimeInMs.update(System.currentTimeMillis() - startGetDatasetVersionTime);

      try {
        namedBlobGetHandler.handle(restRequest, restResponseChannel, callback);
      } catch (RestServiceException e) {
        callback.onCompletion(null, e);
      }
    } catch (AccountServiceException ex) {
      LOGGER.error("Failed to get dataset version for accountName: " + accountName + " containerName: " + containerName
          + " datasetName: " + datasetName + " version: " + version);
      callback.onCompletion(null,
          new RestServiceException(ex.getMessage(), RestServiceErrorCode.getRestServiceErrorCode(ex.getErrorCode())));
    }
  }
}
