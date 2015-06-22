package com.github.ambry.admin;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.restservice.BlobStorageService;
import com.github.ambry.restservice.RestRequestInfo;
import com.github.ambry.restservice.RestRequestMetadata;
import com.github.ambry.restservice.RestServiceException;
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
    logger.trace("Handling get restRequestMetadata - " + restRequestMetadata.getUri());
    AdminOperationType operationType = getOperationType(restRequestMetadata);
    switch (operationType) {
      case echo:
        EchoHandler.handleRequest(restRequestInfo);
        break;
      case getReplicasForBlobId:
        GetReplicasForBlobIdHandler.handleRequest(restRequestInfo, clusterMap);
        break;
      default:
        throw new IllegalStateException("Blob GET not implemented");
    }
  }

  @Override
  public void handlePost(RestRequestInfo restRequestInfo) {
    throw new IllegalStateException("handleGet() not implemented in " + this.getClass().getSimpleName());
  }

  @Override
  public void handleDelete(RestRequestInfo restRequestInfo) {
    throw new IllegalStateException("handleDelete() not implemented in " + this.getClass().getSimpleName());
  }

  @Override
  public void handleHead(RestRequestInfo restRequestInfo) {
    throw new IllegalStateException("handleHead() not implemented in " + this.getClass().getSimpleName());
  }

  /**
   * Looks at the URI to determine the type of operation required.
   * @param restRequestMetadata
   * @return
   */
  private AdminOperationType getOperationType(RestRequestMetadata restRequestMetadata) {
    String path = restRequestMetadata.getPath();
    path = path.startsWith("/") ? path.substring(1, path.length()) : path;
    try {
      return AdminOperationType.valueOf(path);
    } catch (IllegalArgumentException e) {
      return AdminOperationType.unknown;
    }
  }
}
