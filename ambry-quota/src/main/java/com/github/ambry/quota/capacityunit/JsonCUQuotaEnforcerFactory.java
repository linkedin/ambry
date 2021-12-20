package com.github.ambry.quota.capacityunit;

import com.github.ambry.accountstats.AccountStatsStore;
import com.github.ambry.config.QuotaConfig;
import com.github.ambry.quota.QuotaEnforcer;
import com.github.ambry.quota.QuotaEnforcerFactory;
import com.github.ambry.quota.QuotaSource;


/**
 * A {@link QuotaEnforcerFactory} implementation for {@link JsonCUQuotaEnforcer}.
 */
public class JsonCUQuotaEnforcerFactory implements QuotaEnforcerFactory {
  private final JsonCUQuotaEnforcer jsonCUQuotaEnforcer;

  /**
   * Constructor for {@link JsonCUQuotaEnforcerFactory}.
   * @param quotaConfig {@link QuotaConfig} object.
   * @param quotaSource {@link QuotaSource} object.
   * @param accountStatsStore {@link AccountStatsStore} object.
   */
  public JsonCUQuotaEnforcerFactory(QuotaConfig quotaConfig, QuotaSource quotaSource,
      AccountStatsStore accountStatsStore) {
    jsonCUQuotaEnforcer = new JsonCUQuotaEnforcer(quotaSource);
  }

  @Override
  public QuotaEnforcer getRequestQuotaEnforcer() {
    return jsonCUQuotaEnforcer;
  }
}
