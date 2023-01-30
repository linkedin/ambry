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
import com.github.ambry.utils.Pair;
import java.util.List;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.api.client.ZkClientType;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;


public class CommonUtils {
  /**
   * Create an instance of {@link HelixPropertyStore} and {@link ZkBaseDataAccessor} based on given {@link HelixPropertyStoreConfig}.
   * Close {@link ZkBaseDataAccessor} after stopping {@link HelixPropertyStore}.
   * @param zkServers the ZooKeeper server address.
   * @param propertyStoreConfig the config for {@link HelixPropertyStore}.
   * @param subscribedPaths a list of paths to which the PropertyStore subscribes.
   * @return A pair of an instance of {@link HelixPropertyStore} and an instance of {@link ZkBaseDataAccessor}.
   */
  public static Pair<HelixPropertyStore<ZNRecord>, ZkBaseDataAccessor<ZNRecord>> createHelixPropertyStore(
      String zkServers, HelixPropertyStoreConfig propertyStoreConfig, List<String> subscribedPaths) {
    if (zkServers == null || zkServers.isEmpty() || propertyStoreConfig == null) {
      throw new IllegalArgumentException("Invalid arguments, cannot create HelixPropertyStore");
    }
    ZkBaseDataAccessor<ZNRecord> baseDataAccessor =
        new ZkBaseDataAccessor.Builder<ZNRecord>().setZkClientType(ZkClientType.DEDICATED)
            .setZkAddress(zkServers)
            .setRealmAwareZkClientConfig(new RealmAwareZkClient.RealmAwareZkClientConfig().setConnectInitTimeout(
                propertyStoreConfig.zkClientConnectionTimeoutMs).setZkSerializer(new ZNRecordSerializer()))
            .setRealmAwareZkConnectionConfig(
                new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder().setSessionTimeout(
                    propertyStoreConfig.zkClientSessionTimeoutMs).build())
            .build();
    return new Pair<>(new ZkHelixPropertyStore<>(baseDataAccessor, propertyStoreConfig.rootPath, subscribedPaths),
        baseDataAccessor);
  }

  /**
   * Create an instance of {@link HelixPropertyStore} and {@link ZkBaseDataAccessor}, using non-deprecated APIs.
   * Close {@link ZkBaseDataAccessor} after stopping {@link HelixPropertyStore}.
   * TODO replace usages of the other method with this one once verified to be stable.
   * @param zkAddress the ZooKeeper server address.
   * @param rootPath the root path for this {@link HelixPropertyStore}.
   * @param subscribedPaths the paths to subscribe to for change notification. Note that these should be absolute paths,
   *                        not relative paths under {@code rootPath}.
   * @return A pair of an instance of {@link HelixPropertyStore} and an instance of {@link ZkBaseDataAccessor}.
   */
  public static Pair<HelixPropertyStore<ZNRecord>, ZkBaseDataAccessor<ZNRecord>> createHelixPropertyStore(
      String zkAddress, String rootPath, List<String> subscribedPaths) {
    ZkBaseDataAccessor<ZNRecord> baseDataAccessor =
        new ZkBaseDataAccessor.Builder<ZNRecord>().setZkClientType(ZkClientType.DEDICATED)
            .setZkAddress(zkAddress)
            .build();
    return new Pair<>(new ZkHelixPropertyStore<>(baseDataAccessor, rootPath, subscribedPaths), baseDataAccessor);
  }
}
