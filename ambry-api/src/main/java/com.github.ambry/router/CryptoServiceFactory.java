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

/**
 * CryptoService Factory to assist in fetching an instance of {@link CryptoService} for a given key
 * T refers to the key type that this {@link CryptoServiceFactory} is compatible with.
 * Ensure that {@link KeyManagementService} implementation is compatible with the same key type.
 */
public interface CryptoServiceFactory<T> {

  /**
   * Instantiates and returns the {@link CryptoService} for a given key.
   * @param key key for which the {@link CryptoService} is requested
   * @return the {@link CryptoService} instantiated for the given key
   */
  CryptoService<T> getCryptoService(T key) throws InstantiationException;
}
