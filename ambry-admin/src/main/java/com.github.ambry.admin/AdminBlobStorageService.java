package com.github.ambry.admin;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.rest.BlobStorageService;
import com.github.ambry.rest.RestRequestInfo;
import com.github.ambry.rest.RestRequestMetadata;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is an Admin specific implementation of {@link BlobStorageService}.
 * <p/>
 * All the operations that need to be performed by the Admin have to be supported here.
 */
class AdminBlobStorageService implements BlobStorageService {
  private final AdminConfig adminConfig;
  private final AdminMetrics adminMetrics;
  private final ClusterMap clusterMap;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  public AdminBlobStorageService(AdminConfig adminConfig, AdminMetrics adminMetrics, ClusterMap clusterMap) {
    this.adminConfig = adminConfig;
    this.clusterMap = clusterMap;
    this.adminMetrics = adminMetrics;
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
  public void handleGet(RestRequestInfo restRequestInfo)
      throws RestServiceException {
    RestRequestMetadata restRequestMetadata = restRequestInfo.getRestRequestMetadata();
    logger.trace("Handling GET request - {}", restRequestMetadata.getUri());
    try {
      String operationInUri = getOperationFromRequestUri(restRequestMetadata);
      AdminOperationType operationType = AdminOperationType.getAdminOperationType(operationInUri);
      switch (operationType) {
        case echo:
          EchoHandler.handleRequest(restRequestInfo, adminMetrics);
          break;
        case getReplicasForBlobId:
          GetReplicasForBlobIdHandler.handleRequest(restRequestInfo, clusterMap, adminMetrics);
          break;
        default:
          logger.debug("While trying to perform GET: Unsupported operation type - {}", operationType);
          adminMetrics.unsupportedGetOperation.inc();
          throw new RestServiceException("Unsupported operation (" + operationInUri + ") for Admin service",
              RestServiceErrorCode.UnsupportedOperation);
      }
    } finally {
      if (restRequestInfo.isFirstPart()) {
        adminMetrics.getOperationRate.mark();
      }
    }
  }

  @Override
  public void handlePost(RestRequestInfo restRequestInfo)
      throws RestServiceException {
    if (restRequestInfo.isFirstPart()) {
      adminMetrics.postOperationRate.mark();
    }
    logger.debug("While trying to perform POST: Unsupported operation - POST");
    adminMetrics.unsupportedPostOperation.inc();
    throw new RestServiceException("Unsupported operation for Admin service - POST",
        RestServiceErrorCode.UnsupportedOperation);
  }

  @Override
  public void handleDelete(RestRequestInfo restRequestInfo)
      throws RestServiceException {
    if (restRequestInfo.isFirstPart()) {
      adminMetrics.deleteOperationRate.mark();
    }
    logger.debug("While trying to perform DELETE: Unsupported operation - DELETE");
    adminMetrics.unsupportedDeleteOperation.inc();
    throw new RestServiceException("Unsupported operation for Admin service - DELETE",
        RestServiceErrorCode.UnsupportedOperation);
  }

  @Override
  public void handleHead(RestRequestInfo restRequestInfo)
      throws RestServiceException {
    if (restRequestInfo.isFirstPart()) {
      adminMetrics.headOperationRate.mark();
    }
    logger.debug("While trying to perform HEAD: Unsupported operation - HEAD");
    adminMetrics.unsupportedHeadOperation.inc();
    throw new RestServiceException("Unsupported operation for Admin service - HEAD",
        RestServiceErrorCode.UnsupportedOperation);
  }

  /**
   * Looks at the URI to determine the type of operation required.
   * @param restRequestMetadata
   * @return
   */
  private String getOperationFromRequestUri(RestRequestMetadata restRequestMetadata) {
    String path = restRequestMetadata.getPath();
    return (path.startsWith("/") ? path.substring(1, path.length()) : path);
  }
}
