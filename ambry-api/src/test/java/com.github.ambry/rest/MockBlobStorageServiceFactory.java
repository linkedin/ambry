package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.router.Router;


/**
 * Implementation of {@link BlobStorageServiceFactory} that can be used in tests.
 * <p/>
 * Sets up all the supporting cast required for the operation of {@link MockBlobStorageService} and returns a new
 * instance on {@link MockBlobStorageServiceFactory#getBlobStorageService()}.
 */
public class MockBlobStorageServiceFactory implements BlobStorageServiceFactory {
  private final VerifiableProperties verifiableProperties;
  private final Router router;

  public MockBlobStorageServiceFactory(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry,
      ClusterMap clusterMap, Router router) {
    this.verifiableProperties = verifiableProperties;
    this.router = router;
  }

  /**
   *
   * Returns a new instance of {@link MockBlobStorageService}.
   * @return a new instance of {@link MockBlobStorageService}.
   */
  @Override
  public BlobStorageService getBlobStorageService() {
    return new MockBlobStorageService(verifiableProperties, router);
  }
}
