/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.config;

import java.util.Arrays;
import java.util.List;


/**
 * Configuration parameters required by the Ambry frontend.
 */
public class FrontendConfig {

  /**
   * Cache validity in seconds for non-private blobs for GET.
   */
  @Config("frontend.cache.validity.seconds")
  @Default("365 * 24 * 60 * 60")
  public final long frontendCacheValiditySeconds;

  /**
   * The IdConverterFactory that needs to be used by AmbryBlobStorageService to convert IDs.
   */
  @Config("frontend.id.converter.factory")
  @Default("com.github.ambry.frontend.AmbryIdConverterFactory")
  public final String frontendIdConverterFactory;

  /**
   * The SecurityServiceFactory that needs to be used by AmbryBlobStorageService to validate requests.
   */
  @Config("frontend.security.service.factory")
  @Default("com.github.ambry.frontend.AmbrySecurityServiceFactory")
  public final String frontendSecurityServiceFactory;

  /**
   * The comma separated list of prefixes to remove from paths.
   */
  @Config("frontend.path.prefixes.to.remove")
  @Default("")
  public final List<String> frontendPathPrefixesToRemove;

  /**
   * Specifies the blob size in bytes beyond which chunked response will be sent for a getBlob() call
   */
  @Config("frontend.chunked.get.response.threshold.in.bytes")
  @Default("8192")
  public final Integer frontendChunkedGetResponseThresholdInBytes;

  public FrontendConfig(VerifiableProperties verifiableProperties) {
    frontendCacheValiditySeconds = verifiableProperties.getLong("frontend.cache.validity.seconds", 365 * 24 * 60 * 60);
    frontendIdConverterFactory = verifiableProperties.getString("frontend.id.converter.factory",
        "com.github.ambry.frontend.AmbryIdConverterFactory");
    frontendSecurityServiceFactory = verifiableProperties.getString("frontend.security.service.factory",
        "com.github.ambry.frontend.AmbrySecurityServiceFactory");
    frontendPathPrefixesToRemove =
        Arrays.asList(verifiableProperties.getString("frontend.path.prefixes.to.remove", "").split(","));
    frontendChunkedGetResponseThresholdInBytes =
        verifiableProperties.getInt("frontend.chunked.get.response.threshold.in.bytes", 8192);
  }
}
