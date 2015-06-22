package com.github.ambry.admin;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.restservice.BlobStorageService;
import com.github.ambry.restservice.BlobStorageServiceFactory;


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

  public AdminBlobStorageServiceFactory(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry,
      ClusterMap clusterMap)
      throws InstantiationException {
    if (verifiableProperties != null && metricRegistry != null && clusterMap != null) {
      adminConfig = new AdminConfig(verifiableProperties);
      adminMetrics = new AdminMetrics(metricRegistry);
      this.clusterMap = clusterMap;
    } else {
      throw new InstantiationException("One of the received arguments is null");
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
