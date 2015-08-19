package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.VerifiableProperties;


/**
 * Implementation of {@link BlobStorageServiceFactory} that can be used in tests.
 * <p/>
 * Sets up all the supporting cast required for the operation of {@link MockBlobStorageService} and returns a new
 * instance on {@link MockBlobStorageServiceFactory#getBlobStorageService()}.
 */
public class MockBlobStorageServiceFactory implements BlobStorageServiceFactory {
  private final ClusterMap clusterMap;

  public MockBlobStorageServiceFactory(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry,
      ClusterMap clusterMap) {
    this.clusterMap = clusterMap;
  }

  /**
   *
   * Returns a new instance of {@link MockBlobStorageService}.
   * @return a new instance of {@link MockBlobStorageService}.
   */
  @Override
  public BlobStorageService getBlobStorageService() {
    return new MockBlobStorageService(clusterMap);
  }
}
