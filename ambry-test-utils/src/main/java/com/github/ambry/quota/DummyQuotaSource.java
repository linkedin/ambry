package com.github.ambry.quota;

import com.github.ambry.config.QuotaConfig;
import com.github.ambry.quota.storage.QuotaOperation;


/**
 * Dummy {@link QuotaSource} implementation.
 */
public class DummyQuotaSource implements QuotaSource {

  @Override
  public Quota getQuota(QuotaResource quotaResource, QuotaOperation quotaOperation, QuotaMetric quotaMetric) {
    return null;
  }
}
