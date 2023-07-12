package com.github.ambry.frontend;

import com.github.ambry.account.AccountService;
import com.github.ambry.account.AccountServiceException;
import com.github.ambry.account.Dataset;
import com.github.ambry.commons.Callback;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.ReadableStreamChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.rest.RestUtils.InternalKeys.*;


public class DatasetVersionDeleteHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(DatasetVersionDeleteHandler.class);
  private final SecurityService securityService;
  private final AccountAndContainerInjector accountAndContainerInjector;
  private final FrontendMetrics metrics;
  private final NamedBlobDeleteHandler namedBlobDeleteHandler;
  private final AccountService accountService;

  public DatasetVersionDeleteHandler(SecurityService securityService,
      AccountAndContainerInjector accountAndContainerInjector, FrontendMetrics metrics,
      NamedBlobDeleteHandler namedBlobDeleteHandler, AccountService accountService) {
    this.securityService = securityService;
    this.accountAndContainerInjector = accountAndContainerInjector;
    this.metrics = metrics;
    this.namedBlobDeleteHandler = namedBlobDeleteHandler;
    this.accountService = accountService;
  }

  void handle(RestRequest restRequest, RestResponseChannel restResponseChannel,
      Callback<ReadableStreamChannel> callback) throws RestServiceException {
    accountAndContainerInjector.injectAccountContainerForNamedBlob(restRequest, metrics.deleteBlobMetricsGroup);
    securityService.checkAccess(restRequest, "DatasetVersionDeleteHandler");

    namedBlobDeleteHandler.handle(restRequest, restResponseChannel, (result, exception) -> {
      if (exception != null) {
        callback.onCompletion(result, exception);
        return;
      }
      long startDeleteDatasetVersionTime = System.currentTimeMillis();
      String accountName = null;
      String containerName = null;
      String datasetName = null;
      String version = null;
      try {
        Dataset dataset = (Dataset) restRequest.getArgs().get(RestUtils.InternalKeys.TARGET_DATASET);
        accountName = dataset.getAccountName();
        containerName = dataset.getContainerName();
        datasetName = dataset.getDatasetName();
        version = (String) restRequest.getArgs().get(TARGET_DATASET_VERSION);
        accountService.deleteDatasetVersion(accountName, containerName, datasetName, version);
        metrics.deleteDatasetVersionProcessingTimeInMs.update(
            System.currentTimeMillis() - startDeleteDatasetVersionTime);
        // If version is null, use the latest version + 1 from DatasetVersionRecord to construct named blob path.
        callback.onCompletion(null, null);
      } catch (AccountServiceException ex) {
        LOGGER.error(
            "Failed to get dataset version for accountName: " + accountName + " containerName: " + containerName
                + " datasetName: " + datasetName + " version: " + version);
        callback.onCompletion(null,
            new RestServiceException(ex.getMessage(), RestServiceErrorCode.getRestServiceErrorCode(ex.getErrorCode())));
      }
    });
  }
}
