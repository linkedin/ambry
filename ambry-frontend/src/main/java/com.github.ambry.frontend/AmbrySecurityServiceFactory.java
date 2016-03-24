package com.github.ambry.frontend;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.SecurityService;
import com.github.ambry.rest.SecurityServiceFactory;


/**
 * Default implementation of {@link SecurityServiceFactory} for Ambry
 * <p/>
 * Returns a new instance of {@link AmbrySecurityService} on {@link #getSecurityService(VerifiableProperties)} call.
 */
public class AmbrySecurityServiceFactory implements SecurityServiceFactory {

  public SecurityService getSecurityService(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry)
      throws InstantiationException {
    FrontendConfig frontendConfig = new FrontendConfig(verifiableProperties);
    return new AmbrySecurityService(frontendConfig);
  }
}
