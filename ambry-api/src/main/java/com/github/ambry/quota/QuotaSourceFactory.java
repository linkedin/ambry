package com.github.ambry.quota;

/**
 * Factory to instantiate {@link QuotaSource}.
 */
public interface QuotaSourceFactory {
  /**
   * Return the {@link QuotaSource} object.
   * @return QuotaSource object for the {@link QuotaMetric}.
   */
  QuotaSource getQuotaSource();
}
