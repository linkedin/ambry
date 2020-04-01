/*
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.router.CryptoServiceFactory;
import com.github.ambry.router.KeyManagementServiceFactory;
import com.github.ambry.utils.Utils;
import java.security.GeneralSecurityException;


/**
 * Factory for constructing {@link CloudBlobCryptoAgentImpl} instances.
 */
public class CloudBlobCryptoAgentFactoryImpl implements CloudBlobCryptoAgentFactory {

  private final CryptoServiceFactory cryptoServiceFactory;
  private final KeyManagementServiceFactory keyManagementServiceFactory;
  private final String context;

  public CloudBlobCryptoAgentFactoryImpl(VerifiableProperties verifiableProperties, String clusterName,
      MetricRegistry metricRegistry) throws ReflectiveOperationException {
    CloudConfig cloudConfig = new CloudConfig(verifiableProperties);
    cryptoServiceFactory = Utils.getObj(cloudConfig.vcrCryptoServiceFactory, verifiableProperties, metricRegistry);
    keyManagementServiceFactory =
        Utils.getObj(cloudConfig.vcrKeyManagementServiceFactory, verifiableProperties, clusterName, metricRegistry);
    context = cloudConfig.kmsServiceKeyContext;
  }

  @Override
  public CloudBlobCryptoAgent getCloudBlobCryptoAgent() {
    try {
      return new CloudBlobCryptoAgentImpl(cryptoServiceFactory.getCryptoService(),
          keyManagementServiceFactory.getKeyManagementService(), context);
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException(e);
    }
  }
}
