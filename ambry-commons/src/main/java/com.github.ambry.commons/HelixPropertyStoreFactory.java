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
import java.util.ArrayList;
import java.util.List;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A factory to get {@link HelixPropertyStore}.
 * @param <T> The type of the data in the store.
 */
public class HelixPropertyStoreFactory<T> {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Get a {@link HelixPropertyStore}.
   * @param storeConfig A {@link HelixPropertyStoreConfig} instance that provides required configurations to start
   *                    a {@link HelixPropertyStore}.
   * @param subscribedPaths A list of paths that could potentially be listened.
   * @return A {@link HelixPropertyStore} instance.
   */
  public HelixPropertyStore<T> getHelixPropertyStore(HelixPropertyStoreConfig storeConfig, List<String> subscribedPaths) {
    if (storeConfig == null) {
      throw new IllegalArgumentException("zkClientConnectString cannot be null");
    }
    long startTimeMs = System.currentTimeMillis();
    ZkClient zkClient = new ZkClient(storeConfig.zkClientConnectString, storeConfig.zkClientSessionTimeoutMs,
        storeConfig.zkClientConnectionTimeoutMs, new ZNRecordSerializer());
    if (subscribedPaths == null) {
      subscribedPaths = new ArrayList<>();
    }
    HelixPropertyStore<T> helixPropertyStore =
        new ZkHelixPropertyStore<>(new ZkBaseDataAccessor<>(zkClient), storeConfig.rootPath, subscribedPaths);
    logger.info("HelixPropertyStore started with zkClientConnectString={}, zkClientSessionTimeoutMs={}, "
            + "zkClientConnectionTimeoutMs={}, rootPath={}, subscribedPaths={}, took {}ms",
        storeConfig.zkClientConnectString, storeConfig.zkClientSessionTimeoutMs,
        storeConfig.zkClientConnectionTimeoutMs, storeConfig.rootPath, subscribedPaths,
        System.currentTimeMillis() - startTimeMs);
    return helixPropertyStore;
  }
}
