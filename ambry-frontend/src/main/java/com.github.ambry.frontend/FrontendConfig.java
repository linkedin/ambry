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

  /**
   * The {@link com.github.ambry.rest.IdConverterFactory} that needs to be used by {@link AmbryBlobStorageService} to
   * convert IDs.
   */
  @Config("frontend.id.converter.factory")
  @Default("com.github.ambry.frontend.AmbryIdConverterFactory")
  public final String frontendIdConverterFactory;

  /**
   * The {@link com.github.ambry.rest.SecurityServiceFactory} that needs to be used by {@link AmbryBlobStorageService}
   * to validate requests.
   */
  @Config("frontend.security.service.factory")
  @Default("com.github.ambry.frontend.AmbryIdConverterFactory")
  public final String frontendSecurityServiceFactory;

  public FrontendConfig(VerifiableProperties verifiableProperties) {
    frontendCacheValiditySeconds = verifiableProperties.getLong("frontend.cache.validity.seconds", 365 * 24 * 60 * 60);
    frontendIdConverterFactory = verifiableProperties
        .getString("frontend.id.converter.factory", "com.github.ambry.frontend.AmbryIdConverterFactory");
    frontendSecurityServiceFactory = verifiableProperties
        .getString("frontend.security.service.factory", "com.github.ambry.frontend.AmbrySecurityServiceFactory");
  }
}
