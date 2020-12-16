package com.github.ambry.quota.capacityunit;

import com.github.ambry.quota.Quota;
import com.github.ambry.quota.QuotaMetric;
import com.github.ambry.quota.storage.QuotaOperation;
import com.github.ambry.quota.QuotaResource;
import com.github.ambry.quota.QuotaSource;
import java.util.Arrays;
import java.util.HashSet;


public class UnlimitedQuotaSource implements QuotaSource {

  @Override
  public Quota getQuota(QuotaResource quotaResource, QuotaOperation quotaOperation, QuotaMetric quotaMetric) {
    return new Quota<>(quotaMetric, Long.MAX_VALUE, quotaResource,
        new HashSet<>(Arrays.asList(quotaOperation)));
  }
}
