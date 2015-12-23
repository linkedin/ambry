package com.github.ambry.rest;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.router.Router;


/**
 * Implementation of {@link BlobStorageServiceFactory} that can be used in tests.
 * <p/>
 * Sets up all the supporting cast required for the operation of {@link MockBlobStorageService} and returns a new
 * instance on {@link #getBlobStorageService()}.
 */
public class MockBlobStorageServiceFactory implements BlobStorageServiceFactory {
  private final VerifiableProperties verifiableProperties;
  private final RestResponseHandler restResponseHandler;
  private final Router router;

  public MockBlobStorageServiceFactory(VerifiableProperties verifiableProperties, ClusterMap clusterMap,
      RestResponseHandler restResponseHandler, Router router) {
    this.verifiableProperties = verifiableProperties;
    this.restResponseHandler = restResponseHandler;
    this.router = router;
  }

  /**
   *
   * Returns a new instance of {@link MockBlobStorageService}.
   * @return a new instance of {@link MockBlobStorageService}.
   */
  @Override
  public BlobStorageService getBlobStorageService() {
    return new MockBlobStorageService(verifiableProperties, restResponseHandler, router);
  }
}
