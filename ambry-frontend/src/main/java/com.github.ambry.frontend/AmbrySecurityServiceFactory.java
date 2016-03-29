package com.github.ambry.frontend;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.SecurityService;
import com.github.ambry.rest.SecurityServiceFactory;


/**
 * Default implementation of {@link SecurityServiceFactory} for Ambry
 * <p/>
 * Returns a new instance of {@link AmbrySecurityService} on {@link #getSecurityService()} call.
 */
public class AmbrySecurityServiceFactory implements SecurityServiceFactory {

  private final VerifiableProperties verifiableProperties;
  private final FrontendMetrics frontendMetrics;

  public AmbrySecurityServiceFactory(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry) {
    this.verifiableProperties = verifiableProperties;
    frontendMetrics = new FrontendMetrics(metricRegistry);
  }

  @Override
  public SecurityService getSecurityService()
      throws InstantiationException {
    FrontendConfig frontendConfig = new FrontendConfig(verifiableProperties);
    return new AmbrySecurityService(frontendConfig, frontendMetrics);
  }
}
