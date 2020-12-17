package com.github.ambry.quota;

public class AmbryEnforcementRecommendation implements EnforcementRecommendation {
  private final boolean shouldThrottle;
  private final float usagePercentage;
  private final String quotaEnforcerName;
  private final int recommendedHttpStatus;

  public AmbryEnforcementRecommendation(boolean shouldThrottle, float usagePercentage, String quotaEnforcerName,
      int recommendedHttpStatus) {
    this.shouldThrottle = shouldThrottle;
    this.usagePercentage = usagePercentage;
    this.quotaEnforcerName = quotaEnforcerName;
    this.recommendedHttpStatus = recommendedHttpStatus;
  }

  @Override
  public boolean shouldThrottle() {
    return shouldThrottle;
  }

  @Override
  public float quotaUsagePercentage() {
    return usagePercentage;
  }

  @Override
  public String getQuotaEnforcerName() {
    return quotaEnforcerName;
  }

  @Override
  public int getRecommendedHttpStatus() {
    return recommendedHttpStatus;
  }
}
