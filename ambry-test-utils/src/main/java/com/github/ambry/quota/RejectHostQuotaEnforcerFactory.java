package com.github.ambry.quota;

import com.github.ambry.config.QuotaConfig;


public class RejectHostQuotaEnforcerFactory implements HostQuotaEnforcerFactory {
  private final RejectHostQuotaEnforcer rejectQuotaEnforcer;

  public RejectHostQuotaEnforcerFactory(QuotaConfig quotaConfig) {
    this.rejectQuotaEnforcer = new RejectHostQuotaEnforcer();
  }

  @Override
  public HostQuotaEnforcer getHostQuotaEnforcer() {
    return rejectQuotaEnforcer;
  }
}
