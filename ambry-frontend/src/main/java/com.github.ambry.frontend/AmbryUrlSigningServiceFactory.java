/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.frontend;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.SystemTime;


/**
 * Default implementation of {@link UrlSigningServiceFactory} for Ambry
 * <p/>
 * Returns a new instance of {@link UrlSigningService} on {@link #getUrlSigningService()} call.
 */
public class AmbryUrlSigningServiceFactory implements UrlSigningServiceFactory {
  private final FrontendConfig config;

  public AmbryUrlSigningServiceFactory(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry) {
    config = new FrontendConfig(verifiableProperties);
  }

  @Override
  public UrlSigningService getUrlSigningService() {
    return new AmbryUrlSigningService(config.frontendUrlSignerUploadEndpoint, config.frontendUrlSignerDownloadEndpoint,
        config.frontendUrlSignerDefaultUrlTtlSecs, config.frontendUrlSignerDefaultMaxUploadSizeBytes,
        config.frontendUrlSignerMaxUrlTtlSecs, SystemTime.getInstance());
  }
}
