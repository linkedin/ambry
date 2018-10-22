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
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.BlobStorageService;
import com.github.ambry.rest.BlobStorageServiceFactory;
import com.github.ambry.rest.RestResponseHandler;
import com.github.ambry.router.Router;
import com.github.ambry.utils.Utils;
import java.util.Objects;
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
  private final ClusterMapConfig clusterMapConfig;
  private final RestResponseHandler responseHandler;
  private final Router router;
  private final AccountService accountService;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Creates a new instance of AmbryBlobStorageServiceFactory.
   * @param verifiableProperties the properties to use to create configs.
   * @param clusterMap the {@link ClusterMap} to use.
   * @param responseHandler the {@link RestResponseHandler} that can be used to submit responses that need to be sent
   *                        out.
   * @param router the {@link Router} to use.
   * @param accountService the {@link AccountService} to use.
   * @throws IllegalArgumentException if any of the arguments are null.
   */
  public AmbryBlobStorageServiceFactory(VerifiableProperties verifiableProperties, ClusterMap clusterMap,
      RestResponseHandler responseHandler, Router router, AccountService accountService) {
    this.verifiableProperties = Objects.requireNonNull(verifiableProperties, "Provided VerifiableProperties is null");
    this.clusterMap = Objects.requireNonNull(clusterMap, "Provided ClusterMap is null");
    this.responseHandler = Objects.requireNonNull(responseHandler, "Provided RestResponseHandler is null");
    this.router = Objects.requireNonNull(router, "Provided Router is null");
    this.accountService = Objects.requireNonNull(accountService, "Provided AccountService is null");
    clusterMapConfig = new ClusterMapConfig(verifiableProperties);
    frontendConfig = new FrontendConfig(verifiableProperties);
    frontendMetrics = new FrontendMetrics(clusterMap.getMetricRegistry());
    logger.trace("Instantiated AmbryBlobStorageServiceFactory");
  }

  /**
   * Returns a new instance of {@link AmbryBlobStorageService}.
   * @return a new instance of {@link AmbryBlobStorageService}.
   */
  @Override
  public BlobStorageService getBlobStorageService() {
    try {
      IdSigningService idSigningService =
          Utils.<IdSigningServiceFactory>getObj(frontendConfig.idSigningServiceFactory, verifiableProperties,
              clusterMap.getMetricRegistry()).getIdSigningService();
      IdConverterFactory idConverterFactory =
          Utils.getObj(frontendConfig.idConverterFactory, verifiableProperties, clusterMap.getMetricRegistry(),
              idSigningService);
      UrlSigningService urlSigningService =
          Utils.<UrlSigningServiceFactory>getObj(frontendConfig.urlSigningServiceFactory, verifiableProperties,
              clusterMap.getMetricRegistry()).getUrlSigningService();
      AccountAndContainerInjector accountAndContainerInjector =
          new AccountAndContainerInjector(accountService, frontendMetrics, frontendConfig);
      SecurityServiceFactory securityServiceFactory =
          Utils.getObj(frontendConfig.securityServiceFactory, verifiableProperties, clusterMap, accountService,
              urlSigningService, accountAndContainerInjector);
      return new AmbryBlobStorageService(frontendConfig, frontendMetrics, responseHandler, router, clusterMap,
          idConverterFactory, securityServiceFactory, urlSigningService, idSigningService, accountAndContainerInjector,
          clusterMapConfig.clusterMapDatacenterName, clusterMapConfig.clusterMapHostName);
    } catch (Exception e) {
      throw new IllegalStateException("Could not instantiate AmbryBlobStorageService", e);
    }
  }
}
