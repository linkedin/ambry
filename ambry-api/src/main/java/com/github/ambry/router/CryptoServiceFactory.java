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

import java.security.GeneralSecurityException;


/**
 * CryptoService Factory to assist in fetching an instance of {@link CryptoService} which is capable of encrypting or decrypting
 * bytes for a given key
 * Ensure that {@link KeyManagementService} implementation is compatible with the same key type.
 */
public interface CryptoServiceFactory<T> {

  /**
   * Instantiates and returns the {@link CryptoService}
   * @return the {@link CryptoService} instantiated
   * @throws GeneralSecurityException on any exception during instantiation
   */
  CryptoService<T> getCryptoService() throws GeneralSecurityException;
}
