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
package com.github.ambry.account;

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
   * @param zkClientConnectString The connect string for ZooKeeper server.
   * @param zkClientSessionTimeoutMs Session timeout in ms.
   * @param zkClientConnectionTimeoutMs Connection timeout in ms.
   * @param subscribedPaths A list of paths that could potentially be listened.
   * @return A {@link HelixPropertyStore} instance.
   */
  HelixPropertyStore<T> getHelixPropertyStore(String zkClientConnectString, int zkClientSessionTimeoutMs,
      int zkClientConnectionTimeoutMs, String rootPath, List<String> subscribedPaths) {
    if (zkClientConnectString == null) {
      throw new IllegalArgumentException("zkClientConnectString cannot be null");
    }
    long startTime = System.currentTimeMillis();
    ZkClient zkClient = new ZkClient(zkClientConnectString, zkClientSessionTimeoutMs, zkClientConnectionTimeoutMs,
        new ZNRecordSerializer());
    if (subscribedPaths == null) {
      subscribedPaths = new ArrayList<>();
    }
    HelixPropertyStore<T> helixPropertyStore =
        new ZkHelixPropertyStore<>(new ZkBaseDataAccessor<>(zkClient), rootPath, subscribedPaths);
    logger.info("HelixPropertyStore started, took {} ms", System.currentTimeMillis() - startTime);
    return helixPropertyStore;
  }
}
