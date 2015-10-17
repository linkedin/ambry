package com.github.ambry.admin;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.BlobStorageService;
import com.github.ambry.rest.BlobStorageServiceFactory;
import com.github.ambry.router.Router;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Admin specific implementation of {@link BlobStorageServiceFactory}.
 * <p/>
 * Sets up all the supporting cast required for the operation of {@link AdminBlobStorageService} and returns a new
 * instance on {@link AdminBlobStorageServiceFactory#getBlobStorageService()}.
 */
public class AdminBlobStorageServiceFactory implements BlobStorageServiceFactory {
  private final AdminConfig adminConfig;
  private final AdminMetrics adminMetrics;
  private final ClusterMap clusterMap;
  private final Router router;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Instantiate AdminBlobStorageServiceFactory with properties, metrics registry, a cluster map and a router.
   * @param verifiableProperties the properties to use to create configs.
   * @param clusterMap the {@link ClusterMap} to use.
   * @param router the {@link Router} to use.
   */
  public AdminBlobStorageServiceFactory(VerifiableProperties verifiableProperties, ClusterMap clusterMap,
      Router router) {
    if (verifiableProperties != null && clusterMap != null && router != null) {
      adminConfig = new AdminConfig(verifiableProperties);
      adminMetrics = new AdminMetrics(clusterMap.getMetricRegistry());
      this.clusterMap = clusterMap;
      this.router = router;
    } else {
      StringBuilder errorMessage =
          new StringBuilder("Null arg(s) received during instantiation of AdminBlobStorageServiceFactory -");
      if (verifiableProperties == null) {
        errorMessage.append(" [VerifiableProperties] ");
      }
      if (clusterMap == null) {
        errorMessage.append(" [ClusterMap] ");
      }
      if (router == null) {
        errorMessage.append(" [Router] ");
      }
      throw new IllegalArgumentException(errorMessage.toString());
    }
    logger.trace("Instantiated AdminBlobStorageServiceFactory");
  }

  /**
   * Returns a new instance of {@link AdminBlobStorageService}.
   * @return a new instance of {@link AdminBlobStorageService}.
   */
  @Override
  public BlobStorageService getBlobStorageService() {
    return new AdminBlobStorageService(adminConfig, adminMetrics, clusterMap, router);
  }
}
