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
package com.github.ambry.router;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.KMSConfig;
import com.github.ambry.config.VerifiableProperties;
import java.security.GeneralSecurityException;
import javax.crypto.spec.SecretKeySpec;


/**
 * {@link SingleKeyManagementServiceFactory} specific implementation of {@link KeyManagementServiceFactory}.
 * <p/>
 * Sets up all the supporting cast required for the operation of {@link SingleKeyManagementService} and returns a new instance on
 * {@link #getKeyManagementService()}. The default key that the {@link SingleKeyManagementService} returns for any
 * {@link KeyManagementService#getKey(short, short)} is fetched from config "kms.default.container.key" which has to
 * be hex string.
 */
public class SingleKeyManagementServiceFactory implements KeyManagementServiceFactory<SecretKeySpec> {
  private final KMSConfig kmsConfig;
  private final String kmsDefaultContainerKey;

  /**
   * Instantiates {@link SingleKeyManagementServiceFactory}
   * @param verifiableProperties {@link VerifiableProperties} to load configs from
   * @param clusterName Cluster name for which {@link KeyManagementService} is requested for
   * @param registry {@link MetricRegistry} to use
   */
  public SingleKeyManagementServiceFactory(VerifiableProperties verifiableProperties, String clusterName,
      MetricRegistry registry) {
    kmsConfig = new KMSConfig(verifiableProperties);
    kmsDefaultContainerKey = verifiableProperties.getString("kms.default.container.key");
  }

  @Override
  public KeyManagementService<SecretKeySpec> getKeyManagementService() throws GeneralSecurityException {
    return new SingleKeyManagementService(kmsConfig, kmsDefaultContainerKey);
  }
}
