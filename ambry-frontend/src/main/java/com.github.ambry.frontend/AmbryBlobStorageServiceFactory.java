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
import com.github.ambry.account.AccountServiceFactory;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.Notifier;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.BlobStorageService;
import com.github.ambry.rest.BlobStorageServiceFactory;
import com.github.ambry.rest.RestResponseHandler;
import com.github.ambry.router.Router;
import com.github.ambry.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Ambry frontend specific implementation of {@link BlobStorageServiceFactory}.
 * <p/>
 * Sets up all the supporting cast required for the operation of {@link AmbryBlobStorageService} and returns a new
 * instance on {@link #getBlobStorageService()}.
 */
public class AmbryBlobStorageServiceFactory implements BlobStorageServiceFactory {
  private final FrontendConfig frontendConfig;
  private final FrontendMetrics frontendMetrics;
  private final VerifiableProperties verifiableProperties;
  private final ClusterMap clusterMap;
  private final RestResponseHandler responseHandler;
  private final Router router;
  private final Notifier notifier;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Creates a new instance of AmbryBlobStorageServiceFactory.
   * @param verifiableProperties the properties to use to create configs.
   * @param clusterMap the {@link ClusterMap} to use.
   * @param responseHandler the {@link RestResponseHandler} that can be used to submit responses that need to be sent
   *                        out.
   * @param router the {@link Router} to use.
   * @param notifier the {@link Notifier} to use.
   * @throws IllegalArgumentException if any of the arguments are null.
   */
  public AmbryBlobStorageServiceFactory(VerifiableProperties verifiableProperties, ClusterMap clusterMap,
      RestResponseHandler responseHandler, Router router, Notifier notifier) {
    if (verifiableProperties == null || clusterMap == null || responseHandler == null || router == null) {
      throw new IllegalArgumentException("Null arguments were provided during instantiation!");
    } else {
      frontendConfig = new FrontendConfig(verifiableProperties);
      frontendMetrics = new FrontendMetrics(clusterMap.getMetricRegistry());
      this.verifiableProperties = verifiableProperties;
      this.clusterMap = clusterMap;
      this.responseHandler = responseHandler;
      this.router = router;
      this.notifier = notifier;
    }
    logger.trace("Instantiated AmbryBlobStorageServiceFactory");
  }

  /**
   * Returns a new instance of {@link AmbryBlobStorageService}.
   * @return a new instance of {@link AmbryBlobStorageService}.
   */
  @Override
  public BlobStorageService getBlobStorageService() {
    try {
      AccountServiceFactory accountServiceFactory =
          Utils.getObj(frontendConfig.frontendAccountServiceFactory, verifiableProperties,
              clusterMap.getMetricRegistry(), notifier);
      AccountService accountService = accountServiceFactory.getAccountService();
      IdConverterFactory idConverterFactory =
          Utils.getObj(frontendConfig.frontendIdConverterFactory, verifiableProperties, clusterMap.getMetricRegistry());
      UrlSigningServiceFactory urlSigningServiceFactory =
          Utils.getObj(frontendConfig.frontendUrlSigningServiceFactory, verifiableProperties,
              clusterMap.getMetricRegistry());
      UrlSigningService urlSigningService = urlSigningServiceFactory.getUrlSigningService();
      AccountAndContainerInjector accountAndContainerInjector =
          new AccountAndContainerInjector(accountService, frontendMetrics, frontendConfig);
      SecurityServiceFactory securityServiceFactory =
          Utils.getObj(frontendConfig.frontendSecurityServiceFactory, verifiableProperties, clusterMap, accountService,
              urlSigningService, accountAndContainerInjector);
      return new AmbryBlobStorageService(frontendConfig, frontendMetrics, responseHandler, router, clusterMap,
          idConverterFactory, securityServiceFactory, accountService, urlSigningService, accountAndContainerInjector);
    } catch (Exception e) {
      throw new IllegalStateException("Could not instantiate AmbryBlobStorageService", e);
    }
  }
}
