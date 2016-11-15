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
 * Configuration parameters required by the Admin.
 */
public class AdminConfig {

  /**
   * Cache validity in seconds for non-private blobs for GET.
   */
  @Config("admin.cache.validity.seconds")
  @Default("365 * 24 * 60 * 60")
  public final long adminCacheValiditySeconds;

  /**
   * The IdConverterFactory that needs to be used by AdminBlobStorageService to convert IDs.
   */
  @Config("admin.id.converter.factory")
  @Default("com.github.ambry.admin.AdminIdConverterFactory")
  public final String adminIdConverterFactory;

  /**
   * The SecurityServiceFactory that needs to be used by AdminBlobStorageService to validate requests.
   */
  @Config("admin.security.service.factory")
  @Default("com.github.ambry.admin.AdminSecurityServiceFactory")
  public final String adminSecurityServiceFactory;

  /**
   * The comma separated list of prefixes to remove from paths.
   */
  @Config("admin.path.prefixes.to.remove")
  @Default("")
  public final List<String> adminPathPrefixesToRemove;

  /**
   * Specifies the blob size in bytes beyond which chunked response will be sent for a getBlob() call
   */
  @Config("admin.chunked.get.response.threshold.in.bytes")
  @Default("8192")
  public final Integer adminChunkedGetResponseThresholdInBytes;

  public AdminConfig(VerifiableProperties verifiableProperties) {
    adminCacheValiditySeconds = verifiableProperties.getLong("admin.cache.validity.seconds", 365 * 24 * 60 * 60);
    adminIdConverterFactory =
        verifiableProperties.getString("admin.id.converter.factory", "com.github.ambry.admin.AdminIdConverterFactory");
    adminSecurityServiceFactory = verifiableProperties.getString("admin.security.service.factory",
        "com.github.ambry.admin.AdminSecurityServiceFactory");
    adminPathPrefixesToRemove =
        Arrays.asList(verifiableProperties.getString("admin.path.prefixes.to.remove", "").split(","));
    adminChunkedGetResponseThresholdInBytes =
        verifiableProperties.getInt("admin.chunked.get.response.threshold.in.bytes", 8192);
  }
}
