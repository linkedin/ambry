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
package com.github.ambry.frontend;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.SecurityService;
import com.github.ambry.rest.SecurityServiceFactory;


/**
 * Default implementation of {@link SecurityServiceFactory} for Ambry
 * <p/>
 * Returns a new instance of {@link AmbrySecurityService} on {@link #getSecurityService()} call.
 */
public class AmbrySecurityServiceFactory implements SecurityServiceFactory {

  private final FrontendConfig frontendConfig;
  private final FrontendMetrics frontendMetrics;

  public AmbrySecurityServiceFactory(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry) {
    frontendConfig = new FrontendConfig(verifiableProperties);
    frontendMetrics = new FrontendMetrics(metricRegistry);
  }

  @Override
  public SecurityService getSecurityService() throws InstantiationException {
    return new AmbrySecurityService(frontendConfig, frontendMetrics);
  }
}
