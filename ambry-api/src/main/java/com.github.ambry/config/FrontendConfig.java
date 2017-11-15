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
   * Value of "Access-Control-Max-Age" in response headers for OPTIONS requests.
   */
  @Config("frontend.options.validity.seconds")
  @Default("24 * 60 * 60")
  public final long frontendOptionsValiditySeconds;

  /**
   * Value of "Access-Control-Allow-Methods" in response headers for OPTIONS requests.
   */
  @Config("frontend.options.allow.methods")
  @Default("POST, GET, OPTIONS, HEAD, DELETE")
  public final String frontendOptionsAllowMethods;

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
   * The AccountServiceFactory that needs to be used by AmbryBlobStorageService to get account-related information.
   */
  @Config("frontend.account.service.factory")
  @Default("com.github.ambry.account.InMemoryUnknownAccountServiceFactory")
  public final String frontendAccountServiceFactory;

  /**
   * The UrlSigningServiceFactory that needs to be used by AmbryBlobStorageService to sign and verify URLs.
   */
  @Config("frontend.url.signing.service.factory")
  @Default("com.github.ambry.frontend.AmbryUrlSigningServiceFactory")
  public final String frontendUrlSigningServiceFactory;

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

  /**
   * Boolean indicator to specify if frontend should allow the post requests that carry serviceId used as target
   * account name.
   */
  @Config("frontend.allow.service.id.based.post.request")
  @Default("true")
  public final boolean frontendAllowServiceIdBasedPostRequest;

  /**
   * The endpoint that signed POST URLs will point to.
   */
  @Config("frontend.url.signer.upload.endpoint")
  @Default("http://localhost:1174")
  public final String frontendUrlSignerUploadEndpoint;

  /**
   * The endpoint that signed GET URLs will point to.
   */
  @Config("frontend.url.signer.download.endpoint")
  @Default("http://localhost:1174")
  public final String frontendUrlSignerDownloadEndpoint;

  /**
   * The default maximum size (in bytes) that can be uploaded using a signed POST URL unless otherwise specified at
   * the time of URL creation (depends on implementation).
   */
  @Config("frontend.url.signer.default.max.upload.size.bytes")
  @Default("100 * 1024 * 1024")
  public final long frontendUrlSignerDefaultMaxUploadSizeBytes;

  /**
   * The maximum amount of time a signed URL is valid i.e. the highest TTL that requests for signed URLs can set.
   */
  @Config("frontend.url.signer.max.url.ttl.secs")
  @Default("60 * 60")
  public final long frontendUrlSignerMaxUrlTtlSecs;

  /**
   * The default time (in seconds) for which a signed URL is valid unless otherwise specified at the time of URL
   * creation (depends on implementation).
   */
  @Config("frontend.url.signer.default.url.ttl.secs")
  @Default("5 * 60")
  public final long frontendUrlSignerDefaultUrlTtlSecs;

  public FrontendConfig(VerifiableProperties verifiableProperties) {
    frontendCacheValiditySeconds = verifiableProperties.getLong("frontend.cache.validity.seconds", 365 * 24 * 60 * 60);
    frontendOptionsValiditySeconds = verifiableProperties.getLong("frontend.options.validity.seconds", 24 * 60 * 60);
    frontendOptionsAllowMethods =
        verifiableProperties.getString("frontend.options.allow.methods", "POST, GET, OPTIONS, HEAD, DELETE");
    frontendIdConverterFactory = verifiableProperties.getString("frontend.id.converter.factory",
        "com.github.ambry.frontend.AmbryIdConverterFactory");
    frontendSecurityServiceFactory = verifiableProperties.getString("frontend.security.service.factory",
        "com.github.ambry.frontend.AmbrySecurityServiceFactory");
    frontendAccountServiceFactory = verifiableProperties.getString("frontend.account.service.factory",
        "com.github.ambry.account.InMemoryUnknownAccountServiceFactory");
    frontendUrlSigningServiceFactory = verifiableProperties.getString("frontend.url.signing.service.factory",
        "com.github.ambry.frontend.AmbryUrlSigningServiceFactory");
    frontendPathPrefixesToRemove =
        Arrays.asList(verifiableProperties.getString("frontend.path.prefixes.to.remove", "").split(","));
    frontendChunkedGetResponseThresholdInBytes =
        verifiableProperties.getInt("frontend.chunked.get.response.threshold.in.bytes", 8192);
    frontendAllowServiceIdBasedPostRequest =
        verifiableProperties.getBoolean("frontend.allow.service.id.based.post.request", true);
    frontendUrlSignerUploadEndpoint =
        verifiableProperties.getString("frontend.url.signer.upload.endpoint", "http://localhost:1174");
    frontendUrlSignerDownloadEndpoint =
        verifiableProperties.getString("frontend.url.signer.download.endpoint", "http://localhost:1174");
    frontendUrlSignerDefaultMaxUploadSizeBytes =
        verifiableProperties.getLongInRange("frontend.url.signer.default.max.upload.size.bytes", 100 * 1024 * 1024, 0,
            Long.MAX_VALUE);
    frontendUrlSignerMaxUrlTtlSecs =
        verifiableProperties.getLongInRange("frontend.url.signer.max.url.ttl.secs", 60 * 60, 0, Long.MAX_VALUE);
    frontendUrlSignerDefaultUrlTtlSecs =
        verifiableProperties.getLongInRange("frontend.url.signer.default.url.ttl.secs", 5 * 60, 0,
            frontendUrlSignerMaxUrlTtlSecs);
  }
}
