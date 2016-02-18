package com.github.ambry.frontend;

import com.github.ambry.config.Config;
import com.github.ambry.config.Default;
import com.github.ambry.config.VerifiableProperties;


/**
 * Configuration parameters required by the Ambry frontend.
 * <p/>
 * Receives the in-memory representation of a properties file and extracts parameters that are specifically
 * required for Ambry frontend and presents them for retrieval through defined APIs.
 */
class FrontendConfig {

  /**
   * Cache validity in seconds for non-private blobs for GET.
   */
  @Config("frontend.cache.validity.seconds")
  @Default("365*24*60*60")
  public final long frontendCacheValiditySeconds;

  public FrontendConfig(VerifiableProperties verifiableProperties) {
    frontendCacheValiditySeconds = verifiableProperties.getLong("frontend.cache.validity.seconds", 365 * 24 * 60 * 60);
  }
}
