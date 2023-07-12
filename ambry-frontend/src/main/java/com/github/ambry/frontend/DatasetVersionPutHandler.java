package com.github.ambry.frontend;

import com.github.ambry.account.AccountService;
import com.github.ambry.account.AccountServiceException;
import com.github.ambry.account.Dataset;
import com.github.ambry.account.DatasetVersionRecord;
import com.github.ambry.commons.Callback;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.protocol.DatasetVersionState;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.utils.Utils;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.frontend.FrontendUtils.*;
import static com.github.ambry.rest.RestUtils.*;
import static com.github.ambry.rest.RestUtils.InternalKeys.*;


public class DatasetVersionPutHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(DatasetVersionPutHandler.class);
  private final SecurityService securityService;
  private final AccountAndContainerInjector accountAndContainerInjector;
  private final FrontendMetrics metrics;
  private final AccountService accountService;
  private final NamedBlobPutHandler namedBlobPutHandler;
  private final DatasetVersionDeleteHandler datasetVersionDeleteHandler;

  public DatasetVersionPutHandler(SecurityService securityService,
      AccountAndContainerInjector accountAndContainerInjector, FrontendMetrics metrics, AccountService accountService,
      NamedBlobPutHandler namedBlobPutHandler, DatasetVersionDeleteHandler datasetVersionDeleteHandler) {
    this.securityService = securityService;
    this.accountAndContainerInjector = accountAndContainerInjector;
    this.metrics = metrics;
    this.accountService = accountService;
    this.namedBlobPutHandler = namedBlobPutHandler;
    this.datasetVersionDeleteHandler = datasetVersionDeleteHandler;
  }

  public void handle(RestRequest restRequest, RestResponseChannel restResponseChannel,
      Callback<ReadableStreamChannel> callback) throws RestServiceException {
    Callback<ReadableStreamChannel> finalCallback =
        deleteDatasetVersionIfUploadFailedCallBack(restRequest, restResponseChannel, callback);
    accountAndContainerInjector.injectAccountContainerForNamedBlob(restRequest, metrics.putBlobMetricsGroup);
    securityService.checkAccess(restRequest, "DatasetVersionPutHandler");

    accountAndContainerInjector.injectDatasetForNamedBlob(restRequest);
    // now add the user metadata to the rest request
    Dataset dataset = (Dataset) restRequest.getArgs().get(RestUtils.InternalKeys.TARGET_DATASET);
    Map<String, String> userTags = dataset.getUserTags();
    if (userTags != null) {
      for (Map.Entry<String, String> entry : userTags.entrySet()) {
        restRequest.getArgs().put(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + entry.getKey(), entry.getValue());
      }
    }

    try {
      BlobProperties blobProperties = RestUtils.buildBlobProperties(restRequest.getArgs());
      addDatasetVersion(blobProperties, restRequest, restResponseChannel);
    } catch (Exception e) {
      finalCallback.onCompletion(null, e);
    }

    namedBlobPutHandler.handle(restRequest, restResponseChannel, (result, exception) -> {
      if (exception != null) {
        finalCallback.onCompletion(result, exception);
        return;
      }
      try {
        updateVersionStateAndDeleteDatasetVersionOutOfRetentionCount(restRequest, restResponseChannel);
      } catch (RestServiceException e) {
      }
      callback.onCompletion(null, null);
    });
  }

  /**
   * Support add dataset version queries after before the named blob.
   * @param blobProperties The {@link BlobProperties} of this blob.
   * @param restRequest {@link RestRequest} representing the request.
   * @return the {@link Dataset}
   * @throws RestServiceException
   */
  private void addDatasetVersion(BlobProperties blobProperties, RestRequest restRequest,
      RestResponseChannel restResponseChannel) throws RestServiceException {
    long startAddDatasetVersionTime = System.currentTimeMillis();
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
      boolean datasetVersionTtlEnabled =
          RestUtils.getBooleanHeader(restRequest.getArgs(), RestUtils.Headers.DATASET_VERSION_TTL_ENABLED, false);
      long expirationTimeMs =
          Utils.addSecondsToEpochTime(blobProperties.getCreationTimeInMs(), blobProperties.getTimeToLiveInSeconds());
      DatasetVersionRecord datasetVersionRecord =
          accountService.addDatasetVersion(accountName, containerName, datasetName, version,
              blobProperties.getTimeToLiveInSeconds(), blobProperties.getCreationTimeInMs(), datasetVersionTtlEnabled,
              DatasetVersionState.IN_PROGRESS);
      FrontendUtils.replaceRequestPathWithNewOperationOrBlobIdIfNeeded(restRequest, datasetVersionRecord, version);
      restResponseChannel.setHeader(RestUtils.Headers.TARGET_ACCOUNT_NAME, accountName);
      restResponseChannel.setHeader(RestUtils.Headers.TARGET_CONTAINER_NAME, containerName);
      restResponseChannel.setHeader(RestUtils.Headers.TARGET_DATASET_NAME, datasetName);
      restResponseChannel.setHeader(RestUtils.Headers.TARGET_DATASET_VERSION, datasetVersionRecord.getVersion());
      long newExpirationTimeMs = datasetVersionRecord.getExpirationTimeMs();
      // expirationTimeMs = ttl + creationTime. If dataset version inherit the retentionTimeInSeconds from dataset level
      // the ttl should be updated.
      if (expirationTimeMs != newExpirationTimeMs) {
        blobProperties.setTimeToLiveInSeconds(
            Utils.getTtlInSecsFromExpiryMs(newExpirationTimeMs, blobProperties.getCreationTimeInMs()));
      }
      metrics.addDatasetVersionProcessingTimeInMs.update(System.currentTimeMillis() - startAddDatasetVersionTime);
    } catch (AccountServiceException ex) {
      LOGGER.error("Dataset version create failed for accountName: " + accountName + " containerName: " + containerName
          + " datasetName: " + datasetName + " version: " + version);
      throw new RestServiceException(ex.getMessage(), RestServiceErrorCode.getRestServiceErrorCode(ex.getErrorCode()));
    }
  }

  /**
   * When upload named blob failed, we take the best effort to delete the dataset version which create before uploading.
   */
  private <T> Callback<T> deleteDatasetVersionIfUploadFailedCallBack(RestRequest restRequest,
      RestResponseChannel restResponseChannel, Callback<T> callback) {
    return (r, e) -> {
      try {
        Dataset dataset = (Dataset) restRequest.getArgs().get(RestUtils.InternalKeys.TARGET_DATASET);
        String version = (String) restResponseChannel.getHeader(RestUtils.Headers.TARGET_DATASET_VERSION);
        accountService.deleteDatasetVersion(dataset.getAccountName(), dataset.getContainerName(),
            dataset.getDatasetName(), version);
      } catch (Exception ex) {
        LOGGER.error("Best effort to delete the dataset version when upload failed");
      }
      if (callback != null) {
        callback.onCompletion(r, e);
      }
    };
  }

  /**
   * Support delete the dataset version out of retentionCount
   */
  private void updateVersionStateAndDeleteDatasetVersionOutOfRetentionCount(RestRequest restRequest,
      RestResponseChannel restResponseChannel) throws RestServiceException {
    Dataset dataset = (Dataset) restRequest.getArgs().get(RestUtils.InternalKeys.TARGET_DATASET);
    String accountName = dataset.getAccountName();
    String containerName = dataset.getContainerName();
    String datasetName = dataset.getDatasetName();
    String targetVersion = (String) restResponseChannel.getHeader(RestUtils.Headers.TARGET_DATASET_VERSION);
    try {
      accountService.updateDatasetVersionState(accountName, containerName, datasetName, targetVersion,
          DatasetVersionState.READY);
    } catch (AccountServiceException ex) {
      LOGGER.error("Dataset version update failed for accountName: " + accountName + " containerName: " + containerName
          + " datasetName: " + datasetName + " version: " + targetVersion);
      throw new RestServiceException(ex.getMessage(), RestServiceErrorCode.getRestServiceErrorCode(ex.getErrorCode()));
    }
    try {
      List<DatasetVersionRecord> datasetVersionRecordList =
          accountService.getAllValidVersionsOutOfRetentionCount(accountName, containerName, datasetName);
      if (datasetVersionRecordList.size() > 0) {
        LOGGER.info(
            "Number of records need to be deleted due to out of retention count: " + datasetVersionRecordList.size());
        DatasetVersionRecord record = datasetVersionRecordList.get(0);
        String version = record.getVersion();
        RequestPath requestPath = getRequestPath(restRequest);
        RequestPath newRequestPath =
            new RequestPath(requestPath.getPrefix(), requestPath.getClusterName(), requestPath.getPathAfterPrefixes(),
                NAMED_BLOB_PREFIX + SLASH + accountName + SLASH + containerName + SLASH + datasetName + SLASH + version,
                requestPath.getSubResource(), requestPath.getBlobSegmentIdx());
        LOGGER.trace("New request path : " + newRequestPath);
        // Replace RequestPath in the RestRequest and call DeleteBlobHandler.handle.
        restRequest.setArg(InternalKeys.REQUEST_PATH, newRequestPath);
        restRequest.setArg(InternalKeys.TARGET_ACCOUNT_KEY, null);
        restRequest.setArg(InternalKeys.TARGET_CONTAINER_KEY, null);
        //set the rest method to delete in order to delete data in named blob.
        restRequest.setRestMethod(RestMethod.DELETE);
        datasetVersionDeleteHandler.handle(restRequest, restResponseChannel,
            recursiveCallback(restRequest, restResponseChannel, datasetVersionRecordList, 1, accountName, containerName,
                datasetName));
      }
    } catch (Exception e) {
      metrics.deleteDatasetVersionOutOfRetentionError.inc();
      LOGGER.error("Failed to delete dataset version out of retention count for this dataset: " + dataset, e);
    }
  }

  /**
   * Recursively delete the dataset version out of retention count at dataset level.
   * @param datasetVersionRecordList the list of the {@link DatasetVersionRecord}
   * @param idx the index of the datasetVersionRecordList
   * @param accountName the name of the account.
   * @param containerName the name of the container.
   * @param datasetName the name of the dataset.
   */
  private Callback<ReadableStreamChannel> recursiveCallback(RestRequest restRequest,
      RestResponseChannel restResponseChannel, List<DatasetVersionRecord> datasetVersionRecordList, int idx,
      String accountName, String containerName, String datasetName) {
    if (idx == datasetVersionRecordList.size()) {
      return (r, e) -> {
        //In the last callback, set the rest method back.
        restRequest.setRestMethod(RestMethod.PUT);
      };
    }
    Callback<ReadableStreamChannel> nextCallBack =
        recursiveCallback(restRequest, restResponseChannel, datasetVersionRecordList, idx + 1, accountName,
            containerName, datasetName);
    DatasetVersionRecord record = datasetVersionRecordList.get(idx);
    return buildCallback(metrics.deleteBlobSecurityProcessResponseMetrics, securityCheckResult -> {
      String version = record.getVersion();
      RequestPath requestPath = getRequestPath(restRequest);
      RequestPath newRequestPath =
          new RequestPath(requestPath.getPrefix(), requestPath.getClusterName(), requestPath.getPathAfterPrefixes(),
              NAMED_BLOB_PREFIX + SLASH + accountName + SLASH + containerName + SLASH + datasetName + SLASH + version,
              requestPath.getSubResource(), requestPath.getBlobSegmentIdx());
      LOGGER.trace("New request path : " + newRequestPath);
      // Replace RequestPath in the RestRequest and call DeleteBlobHandler.handle.
      restRequest.setArg(InternalKeys.REQUEST_PATH, newRequestPath);
      restRequest.setArg(InternalKeys.TARGET_ACCOUNT_KEY, null);
      restRequest.setArg(InternalKeys.TARGET_CONTAINER_KEY, null);
      datasetVersionDeleteHandler.handle(restRequest, restResponseChannel, nextCallBack);
    }, restRequest.getUri(), LOGGER, null);
  }
}
