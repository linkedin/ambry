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

import com.github.ambry.account.AccountService;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.config.VerifiableProperties;


/**
 * Default implementation of {@link SecurityServiceFactory} for Ambry
 * <p/>
 * Returns a new instance of {@link AmbrySecurityService} on {@link #getSecurityService()} call.
 */
public class AmbrySecurityServiceFactory implements SecurityServiceFactory {

  private final FrontendConfig frontendConfig;
  private final FrontendMetrics frontendMetrics;
  private final UrlSigningService urlSigningService;

  public AmbrySecurityServiceFactory(VerifiableProperties verifiableProperties, ClusterMap clusterMap,
      AccountService accountService, UrlSigningService urlSigningService, IdSigningService idSigningService,
      AccountAndContainerInjector accountAndContainerInjector) {
    frontendConfig = new FrontendConfig(verifiableProperties);
    frontendMetrics = new FrontendMetrics(clusterMap.getMetricRegistry());
    this.urlSigningService = urlSigningService;
  }

  @Override
  public SecurityService getSecurityService() throws InstantiationException {
    return new AmbrySecurityService(frontendConfig, frontendMetrics, urlSigningService,
        new QuotaManager(frontendConfig));
  }
}
