/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
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
  @Default("365*24*60*60")
  public final long adminCacheValiditySeconds;

  public AdminConfig(VerifiableProperties verifiableProperties) {
    adminCacheValiditySeconds = verifiableProperties.getLong("admin.cache.validity.seconds", 365 * 24 * 60 * 60);
  }
}
