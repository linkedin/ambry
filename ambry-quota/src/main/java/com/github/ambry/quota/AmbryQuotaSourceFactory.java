package com.github.ambry.quota;

import com.github.ambry.config.QuotaConfig;
import com.github.ambry.utils.Utils;
import java.util.HashMap;
import java.util.Map;


/**
 * Factory class to instantiate {@link QuotaSource}s specified in {@link QuotaConfig}.
 */
public class AmbryQuotaSourceFactory implements QuotaSourceFactory {
  private final Map<QuotaMetric, QuotaSource> quotaSourceMap;

  /**
   * Constructor for {@link AmbryQuotaSourceFactory}.
   * @param quotaConfig
   */
  AmbryQuotaSourceFactory(QuotaConfig quotaConfig) throws ReflectiveOperationException {
    quotaSourceMap = new HashMap<>();
    quotaSourceMap.put(QuotaMetric.CAPACITY_UNIT, Utils.getObj(quotaConfig.capacityUnitQuotaSource, quotaConfig));
    quotaSourceMap.put(QuotaMetric.STORAGE_IN_GB, Utils.getObj(quotaConfig.storageQuotaSource, quotaConfig));
    quotaSourceMap.put(QuotaMetric.HOST_LEVEL_RESOURCE_USAGE,
        Utils.getObj(quotaConfig.hostResourcesQuotaSource, quotaConfig));
  }

  @Override
  public QuotaSource getQuotaSource(QuotaMetric quotaMetric) {
    return quotaSourceMap.get(quotaMetric);
  }
}
