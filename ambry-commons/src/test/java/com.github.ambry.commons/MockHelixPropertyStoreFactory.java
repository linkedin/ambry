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
public class MockHelixPropertyStoreFactory<T> extends HelixPropertyStoreFactory<T> {
  // an internal map from store root path to a store.
  private final Map<String, MockHelixPropertyStore<T>> storeKeyToMockStoreMap = new HashMap<>();
  private boolean shouldFailSetOperation = false;
  private boolean shouldRemoveRecordBeforeNotify = false;

  /**
   * Constructor.
   * @param shouldFailSetOperation A binary indicator to specify if the {@link HelixPropertyStore#set(String, Object, int)}
   *                               operation should fail.
   * @param shouldRemoveRecordBeforeNotify A boolean indicator to specify if the record should be removed before
   *                                        notifying listeners.
   */
  public MockHelixPropertyStoreFactory(boolean shouldFailSetOperation, boolean shouldRemoveRecordBeforeNotify) {
    this.shouldFailSetOperation = shouldFailSetOperation;
    this.shouldRemoveRecordBeforeNotify = shouldRemoveRecordBeforeNotify;
  }

  @Override
  public HelixPropertyStore<T> getHelixPropertyStore(HelixPropertyStoreConfig storeConfig,
      List<String> subscribedPaths) {
    if (storeConfig == null) {
      throw new IllegalArgumentException("storeConfig cannot be null");
    }
    String storeRootPath = storeConfig.zkClientConnectString + storeConfig.rootPath;
    MockHelixPropertyStore<T> store = storeKeyToMockStoreMap.get(storeRootPath);
    if (store == null) {
      store = new MockHelixPropertyStore<>(shouldFailSetOperation, shouldRemoveRecordBeforeNotify);
      storeKeyToMockStoreMap.put(storeRootPath, store);
    }
    return store;
  }
}
