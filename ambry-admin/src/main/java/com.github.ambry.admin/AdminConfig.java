package com.github.ambry.admin;

import com.github.ambry.config.Config;
import com.github.ambry.config.Default;
import com.github.ambry.config.VerifiableProperties;


/**
 * Configuration parameters required by the Admin.
 * <p/>
 * Receives the in-memory representation of a properties file and extracts parameters that are specifically
 * required for admin and presents them for retrieval through defined APIs.
 */
class AdminConfig {

  /**
   * Cache validity in seconds for non-private blobs for GET.
   */
  @Config("admin.cache.validity.seconds")
  @Default("31536000")
  public final long adminCacheValiditySeconds;

  public AdminConfig(VerifiableProperties verifiableProperties) {
    adminCacheValiditySeconds = verifiableProperties.getLong("admin.cache.validity.seconds", 31536000);
  }
}
