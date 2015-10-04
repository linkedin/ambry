package com.github.ambry.admin;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.FutureResult;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.rest.BlobStorageService;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.router.Callback;
import com.github.ambry.router.ReadableStreamChannel;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is an Admin specific implementation of {@link BlobStorageService}.
 * <p/>
 * All the operations that need to be performed by the Admin are supported here.
 */
class AdminBlobStorageService implements BlobStorageService {
  private final AdminConfig adminConfig;
  private final AdminMetrics adminMetrics;
  private final ClusterMap clusterMap;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Create a new instance of AdminBlobStorageService by supplying it with config, metrics and a cluster map.
   * @param adminConfig the configuration to use in the form of {@link AdminConfig}.
   * @param adminMetrics the metrics instance to use in the form of {@link AdminMetrics}.
   * @param clusterMap the {@link ClusterMap} to be used for operations.
   */
  public AdminBlobStorageService(AdminConfig adminConfig, AdminMetrics adminMetrics, ClusterMap clusterMap) {
    this.adminConfig = adminConfig;
    this.clusterMap = clusterMap;
    this.adminMetrics = adminMetrics;
    logger.trace("Instantiated AdminBlobStorageService");
  }

  @Override
  public void start()
      throws InstantiationException {
    logger.info("AdminBlobStorageService has started");
  }

  @Override
  public void shutdown() {
    logger.info("AdminBlobStorageService shutdown complete");
  }

  @Override
  public Future<ReadableStreamChannel> handleGet(RestRequest restRequest, Callback<ReadableStreamChannel> callback) {
    logger.trace("Handling GET request - {}", restRequest.getUri());
    adminMetrics.getOperationRate.mark();
    Future<ReadableStreamChannel> future;
    String operationInUri = getOperationFromRequestUri(restRequest);
    logger.trace("GET operation requested - {}", operationInUri);
    AdminOperationType operationType = AdminOperationType.getAdminOperationType(operationInUri);
    switch (operationType) {
      case echo:
        future = EchoHandler.handleGetRequest(restRequest, callback, adminMetrics);
        break;
      case getReplicasForBlobId:
        future = GetReplicasForBlobIdHandler.handleGetRequest(restRequest, clusterMap, callback, adminMetrics);
        break;
      default:
        adminMetrics.unsupportedGetOperationError.inc();
        // TODO: go to coordinator backed router.
        future = new FutureResult<ReadableStreamChannel>();
        RestServiceException e =
            new RestServiceException("Unsupported operation during GET (" + operationInUri + ") for Admin service",
                RestServiceErrorCode.UnsupportedOperation);
        ((FutureResult<ReadableStreamChannel>) future).done(null, new RuntimeException(e));
        if (callback != null) {
          callback.onCompletion(null, e);
        }
        break;
    }
    return future;
  }

  @Override
  public Future<String> handlePost(RestRequest restRequest, Callback<String> callback) {
    adminMetrics.postOperationRate.mark();
    adminMetrics.unsupportedPostOperationError.inc();
    FutureResult<String> future = new FutureResult<String>();
    RestServiceException e = new RestServiceException("Unsupported operation for Admin service - POST",
        RestServiceErrorCode.UnsupportedOperation);
    future.done(null, new RuntimeException(e));
    if (callback != null) {
      callback.onCompletion(null, e);
    }
    return future;
  }

  @Override
  public Future<Void> handleDelete(RestRequest restRequest, Callback<Void> callback) {
    adminMetrics.deleteOperationRate.mark();
    adminMetrics.unsupportedDeleteOperationError.inc();
    FutureResult<Void> future = new FutureResult<Void>();
    RestServiceException e = new RestServiceException("Unsupported operation for Admin service - DELETE",
        RestServiceErrorCode.UnsupportedOperation);
    future.done(null, new RuntimeException(e));
    if (callback != null) {
      callback.onCompletion(null, e);
    }
    return future;
  }

  @Override
  public Future<BlobInfo> handleHead(RestRequest restRequest, Callback<BlobInfo> callback) {
    adminMetrics.headOperationRate.mark();
    adminMetrics.unsupportedHeadOperationError.inc();
    FutureResult<BlobInfo> future = new FutureResult<BlobInfo>();
    RestServiceException e = new RestServiceException("Unsupported operation for Admin service - HEAD",
        RestServiceErrorCode.UnsupportedOperation);
    future.done(null, new RuntimeException(e));
    if (callback != null) {
      callback.onCompletion(null, e);
    }
    return future;
  }

  /**
   * Looks at the URI to determine the type of operation required.
   * @param restRequest {@link RestRequest} containing metadata about the request.
   * @return extracted operation type from the uri.
   */
  private String getOperationFromRequestUri(RestRequest restRequest) {
    String path = restRequest.getPath();
    return (path.startsWith("/") ? path.substring(1, path.length()) : path);
  }
}
