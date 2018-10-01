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

import com.github.ambry.protocol.GetOption;
import com.github.ambry.router.GetBlobOptions;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;


/**
 * Configuration parameters required by the Ambry frontend.
 */
public class FrontendConfig {
  private static final String PREFIX = "frontend.";

  // Property keys
  public static final String URL_SIGNER_ENDPOINTS = PREFIX + "url.signer.endpoints";

  private static final String DEFAULT_ENDPOINT = "http://localhost:1174";

  private static final String DEFAULT_ENDPOINTS_STRING =
      "{\"POST\": \"" + DEFAULT_ENDPOINT + "\", \"GET\": \"" + DEFAULT_ENDPOINT + "\"}";

  /**
   * Cache validity in seconds for non-private blobs for GET.
   */
  @Config("frontend.cache.validity.seconds")
  @Default("365 * 24 * 60 * 60")
  public final long cacheValiditySeconds;

  /**
   * Value of "Access-Control-Max-Age" in response headers for OPTIONS requests.
   */
  @Config("frontend.options.validity.seconds")
  @Default("24 * 60 * 60")
  public final long optionsValiditySeconds;

  /**
   * Value of "Access-Control-Allow-Methods" in response headers for OPTIONS requests.
   */
  @Config("frontend.options.allow.methods")
  @Default("POST, GET, OPTIONS, HEAD, DELETE")
  public final String optionsAllowMethods;

  /**
   * The IdConverterFactory that needs to be used by AmbryBlobStorageService to convert IDs.
   */
  @Config("frontend.id.converter.factory")
  @Default("com.github.ambry.frontend.AmbryIdConverterFactory")
  public final String idConverterFactory;

  /**
   * The SecurityServiceFactory that needs to be used by AmbryBlobStorageService to validate requests.
   */
  @Config("frontend.security.service.factory")
  @Default("com.github.ambry.frontend.AmbrySecurityServiceFactory")
  public final String securityServiceFactory;

  /**
   * The UrlSigningServiceFactory that needs to be used by AmbryBlobStorageService to sign and verify URLs.
   */
  @Config("frontend.url.signing.service.factory")
  @Default("com.github.ambry.frontend.AmbryUrlSigningServiceFactory")
  public final String urlSigningServiceFactory;

  /**
   * The IdSigningService that needs to be used by AmbryBlobStorageService to sign and verify IDs.
   */
  private static final String ID_SIGNING_SERVICE_FACTORY_KEY = PREFIX + "id.signing.service.factory";
  @Config(ID_SIGNING_SERVICE_FACTORY_KEY)
  @Default("com.github.ambry.frontend.AmbryIdSigningServiceFactory")
  public final String idSigningServiceFactory;

  /**
   * The comma separated list of prefixes to remove from paths.
   */
  @Config("frontend.path.prefixes.to.remove")
  @Default("")
  public final List<String> pathPrefixesToRemove;

  /**
   * Specifies the blob size in bytes beyond which chunked response will be sent for a getBlob() call
   */
  @Config("frontend.chunked.get.response.threshold.in.bytes")
  @Default("8192")
  public final Integer chunkedGetResponseThresholdInBytes;

  /**
   * Boolean indicator to specify if frontend should allow the post requests that carry serviceId used as target
   * account name.
   */
  @Config("frontend.allow.service.id.based.post.request")
  @Default("true")
  public final boolean allowServiceIdBasedPostRequest;

  /**
   * Boolean indicator to specify if tracking information should be attached to responses.
   */
  @Config("frontend.attach.tracking.info")
  @Default("true")
  public final boolean attachTrackingInfo;

  /**
   * The various endpoints for signed URLs, in JSON string.
   */
  @Config(URL_SIGNER_ENDPOINTS)
  @Default(DEFAULT_ENDPOINTS_STRING)
  public final String urlSignerEndpoints;

  /**
   * The default maximum size (in bytes) that can be uploaded using a signed POST URL unless otherwise specified at
   * the time of URL creation (depends on implementation).
   */
  @Config("frontend.url.signer.default.max.upload.size.bytes")
  @Default("100 * 1024 * 1024")
  public final long urlSignerDefaultMaxUploadSizeBytes;

  /**
   * The maximum amount of time a signed URL is valid i.e. the highest TTL that requests for signed URLs can set.
   */
  @Config("frontend.url.signer.max.url.ttl.secs")
  @Default("60 * 60")
  public final long urlSignerMaxUrlTtlSecs;

  /**
   * The default time (in seconds) for which a signed URL is valid unless otherwise specified at the time of URL
   * creation (depends on implementation).
   */
  @Config("frontend.url.signer.default.url.ttl.secs")
  @Default("5 * 60")
  public final long urlSignerDefaultUrlTtlSecs;

  /**
   * The default {@link GetOption} that the frontend has to use when constructing
   * {@link GetBlobOptions} for {@link com.github.ambry.router.Router#getBlob(String, GetBlobOptions)}
   * (or the callback equivalent).
   */
  @Config("frontend.default.router.get.option")
  @Default("GetOption.None")
  public final GetOption defaultRouterGetOption;

  /**
   * The blob TTL in seconds to use for data chunks uploaded in a stitched upload session.
   */
  public static final String CHUNK_UPLOAD_INITIAL_CHUNK_TTL_SECS_KEY = PREFIX + "chunk.upload.initial.chunk.ttl.secs";
  @Config(CHUNK_UPLOAD_INITIAL_CHUNK_TTL_SECS_KEY)
  @Default("28 * 24 * 60 * 60")
  public final long chunkUploadInitialChunkTtlSecs;

  public static final String FAIL_IF_TTL_REQUIRED_BUT_NOT_PROVIDED_KEY =
      PREFIX + "fail.if.ttl.required.but.not.provided";
  @Config(FAIL_IF_TTL_REQUIRED_BUT_NOT_PROVIDED_KEY)
  @Default("false")
  public final boolean failIfTtlRequiredButNotProvided;

  public static final String MAX_ACCEPTABLE_TTL_SECS_IF_TTL_REQUIRED_KEY =
      PREFIX + "max.acceptable.ttl.secs.if.ttl.required";
  @Config(MAX_ACCEPTABLE_TTL_SECS_IF_TTL_REQUIRED_KEY)
  @Default("30 * 24 * 60 * 60")
  public final int maxAcceptableTtlSecsIfTtlRequired;

  public FrontendConfig(VerifiableProperties verifiableProperties) {
    cacheValiditySeconds = verifiableProperties.getLong("frontend.cache.validity.seconds", 365 * 24 * 60 * 60);
    optionsValiditySeconds = verifiableProperties.getLong("frontend.options.validity.seconds", 24 * 60 * 60);
    optionsAllowMethods =
        verifiableProperties.getString("frontend.options.allow.methods", "POST, GET, OPTIONS, HEAD, DELETE");
    idConverterFactory = verifiableProperties.getString("frontend.id.converter.factory",
        "com.github.ambry.frontend.AmbryIdConverterFactory");
    securityServiceFactory = verifiableProperties.getString("frontend.security.service.factory",
        "com.github.ambry.frontend.AmbrySecurityServiceFactory");
    urlSigningServiceFactory = verifiableProperties.getString("frontend.url.signing.service.factory",
        "com.github.ambry.frontend.AmbryUrlSigningServiceFactory");
    idSigningServiceFactory = verifiableProperties.getString(ID_SIGNING_SERVICE_FACTORY_KEY,
        "com.github.ambry.frontend.AmbryIdSigningServiceFactory");
    pathPrefixesToRemove =
        Arrays.asList(verifiableProperties.getString("frontend.path.prefixes.to.remove", "").split(","));
    chunkedGetResponseThresholdInBytes =
        verifiableProperties.getInt("frontend.chunked.get.response.threshold.in.bytes", 8192);
    allowServiceIdBasedPostRequest =
        verifiableProperties.getBoolean("frontend.allow.service.id.based.post.request", true);
    attachTrackingInfo = verifiableProperties.getBoolean("frontend.attach.tracking.info", true);
    urlSignerEndpoints = verifiableProperties.getString(URL_SIGNER_ENDPOINTS, DEFAULT_ENDPOINTS_STRING);
    urlSignerDefaultMaxUploadSizeBytes =
        verifiableProperties.getLongInRange("frontend.url.signer.default.max.upload.size.bytes", 100 * 1024 * 1024, 0,
            Long.MAX_VALUE);
    urlSignerMaxUrlTtlSecs =
        verifiableProperties.getLongInRange("frontend.url.signer.max.url.ttl.secs", 60 * 60, 0, Long.MAX_VALUE);
    urlSignerDefaultUrlTtlSecs =
        verifiableProperties.getLongInRange("frontend.url.signer.default.url.ttl.secs", 5 * 60, 0,
            urlSignerMaxUrlTtlSecs);
    defaultRouterGetOption =
        GetOption.valueOf(verifiableProperties.getString("frontend.default.router.get.option", GetOption.None.name()));
    chunkUploadInitialChunkTtlSecs =
        verifiableProperties.getLong(CHUNK_UPLOAD_INITIAL_CHUNK_TTL_SECS_KEY, TimeUnit.DAYS.toSeconds(28));
    failIfTtlRequiredButNotProvided = verifiableProperties.getBoolean(FAIL_IF_TTL_REQUIRED_BUT_NOT_PROVIDED_KEY, false);
    maxAcceptableTtlSecsIfTtlRequired = verifiableProperties.getIntInRange(MAX_ACCEPTABLE_TTL_SECS_IF_TTL_REQUIRED_KEY,
        (int) TimeUnit.DAYS.toSeconds(30), 0, Integer.MAX_VALUE);
  }
}
