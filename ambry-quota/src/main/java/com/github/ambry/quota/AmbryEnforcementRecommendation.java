package com.github.ambry.quota;

public class AmbryEnforcementRecommendation implements EnforcementRecommendation {
  private final boolean shouldThrotle;
  private final float usagePercentage;

  AmbryEnforcementRecommendation(boolean shouldThrotle, float usagePercentage) {
    this.shouldThrotle = shouldThrotle;
    this.usagePercentage = usagePercentage;
  }

  public boolean shouldThrottle() {
    return shouldThrotle;
  }

  public float quotaUsagePercentage() {
    return usagePercentage;
  }
}
