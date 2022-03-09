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
import com.github.ambry.commons.HostLevelThrottler;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.config.HostThrottleConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.quota.QuotaManager;


/**
 * Default implementation of {@link SecurityServiceFactory} for Ambry
 * <p/>
 * Returns a new instance of {@link AmbrySecurityService} on {@link #getSecurityService()} call.
 */
public class AmbrySecurityServiceFactory implements SecurityServiceFactory {

  private final FrontendConfig frontendConfig;
  private final HostThrottleConfig hostThrottleConfig;
  private final FrontendMetrics frontendMetrics;
  private final UrlSigningService urlSigningService;
  private final QuotaManager quotaManager;

  public AmbrySecurityServiceFactory(VerifiableProperties verifiableProperties, ClusterMap clusterMap,
      AccountService accountService, UrlSigningService urlSigningService, IdSigningService idSigningService,
      AccountAndContainerInjector accountAndContainerInjector, QuotaManager quotaManager) {
    frontendConfig = new FrontendConfig(verifiableProperties);
    hostThrottleConfig = new HostThrottleConfig(verifiableProperties);
    frontendMetrics = new FrontendMetrics(clusterMap.getMetricRegistry(), frontendConfig);
    this.urlSigningService = urlSigningService;
    this.quotaManager = quotaManager;
  }

  @Override
  public SecurityService getSecurityService() {
    return new AmbrySecurityService(frontendConfig, frontendMetrics, urlSigningService,
        new HostLevelThrottler(hostThrottleConfig), quotaManager);
  }
}
