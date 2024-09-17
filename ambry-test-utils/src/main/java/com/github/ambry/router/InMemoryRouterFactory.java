/*
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
package com.github.ambry.router;

import com.github.ambry.account.AccountService;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.frontend.IdConverterFactory;
import com.github.ambry.frontend.IdSigningService;
import com.github.ambry.frontend.IdSigningServiceFactory;
import com.github.ambry.named.NamedBlobDb;
import com.github.ambry.named.NamedBlobDbFactory;
import com.github.ambry.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link InMemoryRouter} specific implementation of {@link RouterFactory}.
 * <p/>
 * Sets up all the supporting cast required for the operation of {@link InMemoryRouter} and returns a new instance on
 * {@link #getRouter()}.
 */
public class InMemoryRouterFactory implements RouterFactory {
  private static final Logger logger = LoggerFactory.getLogger(InMemoryRouterFactory.class);
  private static InMemoryRouter latestInstance = null;

  private final VerifiableProperties verifiableProperties;
  private final NotificationSystem notificationSystem;
  private final ClusterMap clusterMap;
  private final RouterConfig routerConfig;
  private final AccountService accountService;
  public InMemoryRouterFactory(VerifiableProperties verifiableProperties, ClusterMap clusterMap,
      NotificationSystem notificationSystem, SSLFactory sslFactory, AccountService accountService) {
    this.verifiableProperties = verifiableProperties;
    this.notificationSystem = notificationSystem;
    this.clusterMap = clusterMap;
    this.routerConfig = new RouterConfig(verifiableProperties);
    this.accountService = accountService;
  }

  @Override
  public Router getRouter() {
    IdConverterFactory idConverterFactory = null;
    if (routerConfig.idConverterFactory != null) {
      try {
        IdSigningService idSigningService =
            Utils.<IdSigningServiceFactory>getObj(routerConfig.idSigningServiceFactory, verifiableProperties,
                clusterMap.getMetricRegistry()).getIdSigningService();
        NamedBlobDb namedBlobDb = Utils.isNullOrEmpty(routerConfig.namedBlobDbFactory) ? null
            : Utils.<NamedBlobDbFactory>getObj(routerConfig.namedBlobDbFactory, verifiableProperties,
                clusterMap.getMetricRegistry(), accountService).getNamedBlobDb();
        idConverterFactory =
            Utils.getObj(routerConfig.idConverterFactory, verifiableProperties, clusterMap.getMetricRegistry(),
                idSigningService, namedBlobDb);
      } catch (Exception e) {
        logger.error("Failed to create idConverterFactory");
      }
    }
    latestInstance = new InMemoryRouter(verifiableProperties, notificationSystem, clusterMap, idConverterFactory);
    return latestInstance;
  }

  /**
   * Gets the instance of {@link InMemoryRouter} that was intanstiated most recently (helps in tests).
   * @return the instance of {@link InMemoryRouter} instantiated most recently.
   */
  public static InMemoryRouter getLatestInstance() {
    return latestInstance;
  }
}
