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
package com.github.ambry.commons;

import com.github.ambry.config.HelixPropertyStoreConfig;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.store.HelixPropertyStore;


/**
 * A factory to generate {@link MockHelixPropertyStore}.
 * @param <T>
 */
class MockHelixPropertyStoreFactory<T> extends HelixPropertyStoreFactory<T> {
  // an internal map from store root path to a store.
  private final Map<String, MockHelixPropertyStore<T>> storeKeyToMockStoreMap = new HashMap<>();

  @Override
  HelixPropertyStore<T> getHelixPropertyStore(HelixPropertyStoreConfig storeConfig, List<String> subscribedPaths) {
    if (storeConfig == null) {
      throw new IllegalArgumentException("storeConfig cannot be null");
    }
    String storeRootPath = storeConfig.zkClientConnectString + storeConfig.rootPath;
    MockHelixPropertyStore<T> store = storeKeyToMockStoreMap.get(storeRootPath);
    if (store == null) {
      store = new MockHelixPropertyStore<>();
      storeKeyToMockStoreMap.put(storeRootPath, store);
    }
    return store;
  }
}
