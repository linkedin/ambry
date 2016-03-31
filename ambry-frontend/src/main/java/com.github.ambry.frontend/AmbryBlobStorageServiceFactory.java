package com.github.ambry.frontend;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.BlobStorageService;
import com.github.ambry.rest.BlobStorageServiceFactory;
import com.github.ambry.rest.IdConverterFactory;
import com.github.ambry.rest.RestResponseHandler;
import com.github.ambry.rest.SecurityServiceFactory;
import com.github.ambry.router.Router;
import com.github.ambry.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Ambry frontend specific implementation of {@link BlobStorageServiceFactory}.
 * <p/>
 * Sets up all the supporting cast required for the operation of {@link AmbryBlobStorageService} and returns a new
 * instance on {@link #getBlobStorageService()}.
 */
public class AmbryBlobStorageServiceFactory implements BlobStorageServiceFactory {
  private final FrontendMetrics frontendMetrics;
  private final ClusterMap clusterMap;
  private final RestResponseHandler responseHandler;
  private final Router router;
  private final IdConverterFactory idConverterFactory;
  private final SecurityServiceFactory securityServiceFactory;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Creates a new instance of AmbryBlobStorageServiceFactory.
   * @param verifiableProperties the properties to use to create configs.
   * @param clusterMap the {@link ClusterMap} to use.
   * @param responseHandler the {@link RestResponseHandler} that can be used to submit responses that need to be sent
   *                        out.
   * @param router the {@link Router} to use.
   * @throws IllegalArgumentException if any of the arguments are null.
   */
  public AmbryBlobStorageServiceFactory(VerifiableProperties verifiableProperties, ClusterMap clusterMap,
      RestResponseHandler responseHandler, Router router)
      throws Exception {
    if (verifiableProperties == null || clusterMap == null || responseHandler == null || router == null) {
      throw new IllegalArgumentException("Null arguments were provided during instantiation!");
    } else {
      FrontendConfig frontendConfig = new FrontendConfig(verifiableProperties);
      frontendMetrics = new FrontendMetrics(clusterMap.getMetricRegistry());
      this.clusterMap = clusterMap;
      this.responseHandler = responseHandler;
      this.router = router;
      idConverterFactory =
          Utils.getObj(frontendConfig.frontendIdConverterFactory, verifiableProperties, clusterMap.getMetricRegistry());
      securityServiceFactory = Utils
          .getObj(frontendConfig.frontendSecurityServiceFactory, verifiableProperties, clusterMap.getMetricRegistry());
    }
    logger.trace("Instantiated AmbryBlobStorageServiceFactory");
  }

  /**
   * Returns a new instance of {@link AmbryBlobStorageService}.
   * @return a new instance of {@link AmbryBlobStorageService}.
   */
  @Override
  public BlobStorageService getBlobStorageService() {
    return new AmbryBlobStorageService(frontendMetrics, clusterMap, responseHandler, router, idConverterFactory,
        securityServiceFactory);
  }
}
