package com.github.ambry.admin;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.BlobStorageService;
import com.github.ambry.rest.BlobStorageServiceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Admin specific implementation of {@link BlobStorageServiceFactory}
 * <p/>
 * Sets up all the supporting cast required for the operation of {@link AdminBlobStorageService} and returns a new
 * instance on {@link AdminBlobStorageServiceFactory#getBlobStorageService()}.
 */
public class AdminBlobStorageServiceFactory implements BlobStorageServiceFactory {
  private final AdminConfig adminConfig;
  private final AdminMetrics adminMetrics;
  private final ClusterMap clusterMap;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  public AdminBlobStorageServiceFactory(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry,
      ClusterMap clusterMap)
      throws InstantiationException {
    if (verifiableProperties != null && metricRegistry != null && clusterMap != null) {
      adminConfig = new AdminConfig(verifiableProperties);
      adminMetrics = new AdminMetrics(metricRegistry);
      this.clusterMap = clusterMap;
    } else {
      logger.error("Null arg(s) received during instantiation of AdminBlobStorageServiceFactory");
      throw new InstantiationException("Null arg(s) received during instantiation of AdminBlobStorageServiceFactory");
    }
  }

  /**
   * Returns a new instance of {@link AdminBlobStorageService}.
   * @return
   * @throws InstantiationException
   */
  @Override
  public BlobStorageService getBlobStorageService()
      throws InstantiationException {
    return new AdminBlobStorageService(adminConfig, adminMetrics, clusterMap);
  }
}
