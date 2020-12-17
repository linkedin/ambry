package com.github.ambry.quota;

import com.github.ambry.config.QuotaConfig;
import com.github.ambry.config.StorageQuotaConfig;
import com.github.ambry.config.VerifiableProperties;
import java.util.Properties;


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
}
