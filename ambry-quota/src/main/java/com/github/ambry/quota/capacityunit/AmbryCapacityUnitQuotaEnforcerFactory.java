package com.github.ambry.quota.capacityunit;

import com.github.ambry.config.QuotaConfig;
import com.github.ambry.quota.QuotaSource;
import com.github.ambry.quota.RequestQuotaEnforcer;
import com.github.ambry.quota.RequestQuotaEnforcerFactory;


public class AmbryCapacityUnitQuotaEnforcerFactory implements RequestQuotaEnforcerFactory {
  private final RequestQuotaEnforcer quotaEnforcer;

  public AmbryCapacityUnitQuotaEnforcerFactory(QuotaConfig quotaConfig, QuotaSource quotaSource) {
    quotaEnforcer = new AmbryCapacityUnitQuotaEnforcer(quotaSource);
  }

  @Override
  public RequestQuotaEnforcer getRequestQuotaEnforcer() {
    return quotaEnforcer;
  }
}
