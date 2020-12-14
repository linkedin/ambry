package com.github.ambry.quota;

/**
 * Factory to instantiate {@link QuotaSource}.
 */
public interface QuotaSourceFactory {
  /**
   * Return the {@link QuotaSource} for the specified {@link QuotaMetric}.
   * @param quotaMetric {@link QuotaMetric} object.
   * @return QuotaSource object for the {@link QuotaMetric}.
   */
  QuotaSource getQuotaSource(QuotaMetric quotaMetric);
}
