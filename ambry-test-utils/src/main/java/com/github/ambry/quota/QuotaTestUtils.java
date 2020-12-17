package com.github.ambry.quota;

import com.github.ambry.config.QuotaConfig;
import com.github.ambry.config.StorageQuotaConfig;
import com.github.ambry.config.VerifiableProperties;
import java.util.Map;
import java.util.Properties;
import org.json.JSONArray;
import org.json.JSONObject;


/**
 * Utils for testing and initializing quota.
 */
public class QuotaTestUtils {

  /**
   * Create a dummy {@link QuotaConfig} object with empty string values for required configs.
   * @return QuotaConfig object.
   */
  public static QuotaConfig createDummyQuotaConfig() {
    Properties properties = new Properties();
    properties.setProperty(StorageQuotaConfig.HELIX_PROPERTY_ROOT_PATH, "");
    properties.setProperty(StorageQuotaConfig.ZK_CLIENT_CONNECT_ADDRESS, "");
    return new QuotaConfig(new VerifiableProperties(properties));
  }

  public static QuotaConfig createQuotaConfig(Map<String, String> map, boolean isQuotaThrottlingEnabled, QuotaMode quotaMode) {
    Properties properties = new Properties();
    properties.setProperty(StorageQuotaConfig.HELIX_PROPERTY_ROOT_PATH, "");
    properties.setProperty(StorageQuotaConfig.ZK_CLIENT_CONNECT_ADDRESS, "");
    properties.setProperty(QuotaConfig.REQUEST_QUOTA_THROTTLING_ENABLED, ""+isQuotaThrottlingEnabled);
    properties.setProperty(QuotaConfig.QUOTA_THROTTLING_MODE, quotaMode.name());
    JSONArray jsonArray = new JSONArray();
    for (String enforcerFactoryClass : map.keySet()) {
      JSONObject jsonObject = new JSONObject();
      jsonObject.put(QuotaConfig.ENFORCER_STR, enforcerFactoryClass);
      jsonObject.put(QuotaConfig.SOURCE_STR, map.get(enforcerFactoryClass));
      jsonArray.put(jsonObject);
    }
    properties.setProperty(QuotaConfig.REQUEST_QUOTA_ENFORCER_SOURCE_PAIR_INFO_JSON,
        new JSONObject().put(QuotaConfig.QUOTA_ENFORCER_SOURCE_PAIR_INFO_STR, jsonArray).toString());
    return new QuotaConfig(new VerifiableProperties(properties));
  }
}
