package com.github.ambry.quota;

/**
 * Interface for class that would do the quota enforcement based on quota of load on host resources. This type of quota
 * enforcement doesn't need details about the request and can be done much before than deserialization of the request.
 * A {@link HostQuotaEnforcer} object would need a {@link QuotaSource} to get and save quota and usage.
 */
public interface HostQuotaEnforcer {
  /**
   * Method to initialize the {@link HostQuotaEnforcer}.
   */
  void init();

  /**
   * Makes an {@link EnforcementRecommendation} based on quota on host load.
   * @return EnforcementRecommendation object with the recommendation.
   */
  EnforcementRecommendation recommend();

  /**
   * @return QuotaSource object of the enforcer.
   */
  QuotaSource getQuotaSource();

  /**
   * Shutdown the {@link HostQuotaEnforcer} and perform any cleanup.
   */
  void shutdown();
}
