package com.github.ambry.quota.capacityunit;

import com.github.ambry.config.QuotaConfig;
import com.github.ambry.quota.QuotaSource;
import com.github.ambry.quota.QuotaSourceFactory;


public class UnlimitedQuotaSourceFactory implements QuotaSourceFactory {
  public UnlimitedQuotaSourceFactory(QuotaConfig quotaConfig) {

  }

  @Override
  public QuotaSource getQuotaSource() {
    return new UnlimitedQuotaSource();
  }
}
