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

import com.github.ambry.config.CryptoServiceConfig;
import com.github.ambry.config.VerifiableProperties;
import javax.crypto.spec.SecretKeySpec;


/**
 * {@link DefaultCryptoServiceFactory} specific implementation of {@link CryptoServiceFactory}.
 * <p/>
 * Sets up all the supporting cast required for the operation of {@link DefaultCryptoService} and returns a new instance on
 * {@link #getCryptoService(Object)}.
 */
public class DefaultCryptoServiceFactory implements CryptoServiceFactory<SecretKeySpec> {
  private final CryptoServiceConfig cryptoServiceConfig;

  DefaultCryptoServiceFactory(VerifiableProperties verifiableProperties) {
    this.cryptoServiceConfig = new CryptoServiceConfig(verifiableProperties);
  }

  @Override
  public CryptoService<SecretKeySpec> getCryptoService(SecretKeySpec key) throws InstantiationException {
    return new DefaultCryptoService(cryptoServiceConfig, key);
  }
}
