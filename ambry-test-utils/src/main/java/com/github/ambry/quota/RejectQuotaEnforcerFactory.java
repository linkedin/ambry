package com.github.ambry.quota;

import com.github.ambry.config.QuotaConfig;


public class RejectQuotaEnforcerFactory implements RequestQuotaEnforcerFactory {
  private final RejectQuotaEnforcer rejectQuotaEnforcer;

  public RejectQuotaEnforcerFactory(QuotaConfig quotaConfig, QuotaSource quotaSource) {
    this.rejectQuotaEnforcer = new RejectQuotaEnforcer(quotaSource);
  }

  @Override
  public RequestQuotaEnforcer getRequestQuotaEnforcer() {
    return rejectQuotaEnforcer;
  }
}
