/*
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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
import java.util.List;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.store.zk.ZkHelixPropertyStore;


public class CommonUtils {
  /**
   * Create a instance of {@link HelixPropertyStore} based on given {@link HelixPropertyStoreConfig}.
   * @param zkServers the ZooKeeper server address.
   * @param propertyStoreConfig the config for {@link HelixPropertyStore}.
   * @param subscribedPaths a list of paths to which the PropertyStore subscribes.
   * @return the instance of {@link HelixPropertyStore}.
   */
  public static HelixPropertyStore<ZNRecord> createHelixPropertyStore(String zkServers,
      HelixPropertyStoreConfig propertyStoreConfig, List<String> subscribedPaths) {
    if (zkServers == null || zkServers.isEmpty() || propertyStoreConfig == null) {
      throw new IllegalArgumentException("Invalid arguments, cannot create HelixPropertyStore");
    }
    ZkClient zkClient = new ZkClient(zkServers, propertyStoreConfig.zkClientSessionTimeoutMs,
        propertyStoreConfig.zkClientConnectionTimeoutMs, new ZNRecordSerializer());
    return new ZkHelixPropertyStore<>(new ZkBaseDataAccessor<>(zkClient), propertyStoreConfig.rootPath,
        subscribedPaths);
  }
}
