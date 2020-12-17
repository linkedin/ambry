package com.github.ambry.quota;

/**
 * {@link EnforcementRecommendation} implementation where recommendation is always to reject.
 */
public class RejectEnforcementRecommendation implements EnforcementRecommendation {
  @Override
  public boolean shouldThrottle() {
    return true;
  }

  @Override
  public float quotaUsagePercentage() {
    return 100;
  }
}
