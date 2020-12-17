package com.github.ambry.quota;

public class RejectHostQuotaEnforcer implements HostQuotaEnforcer {

  @Override
  public void init() {
  }

  @Override
  public EnforcementRecommendation recommend() {
    return new RejectEnforcementRecommendation(RejectHostQuotaEnforcer.class.getSimpleName());
  }

  @Override
  public void shutdown() {
  }
}
