/**
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
package com.github.ambry.replication;

import com.github.ambry.clustermap.ReplicaType;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.Utils;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helper class to get findtoken based on replica type or input stream.
 */
public class FindTokenHelper {
  private static final Logger logger = LoggerFactory.getLogger(FindTokenHelper.class);
  private final StoreKeyFactory storeKeyFactory;
  private final ReplicationConfig replicationConfig;
  private final Map<ReplicaType, FindTokenFactory> findTokenFactoryMap;

  public FindTokenHelper() {
    storeKeyFactory = null;
    replicationConfig = null;
    findTokenFactoryMap = null;
  }

  /**
   * Create a {@code FindTokenHelper} object.
   * @param storeKeyFactory
   * @param replicationConfig
   */
  public FindTokenHelper(StoreKeyFactory storeKeyFactory, ReplicationConfig replicationConfig)
      throws ReflectiveOperationException {
    this.storeKeyFactory = storeKeyFactory;
    this.replicationConfig = replicationConfig;
    findTokenFactoryMap = new HashMap<>();
    findTokenFactoryMap.put(ReplicaType.DISK_BACKED,
        Utils.getObj(replicationConfig.replicationStoreTokenFactory, storeKeyFactory));
    findTokenFactoryMap.put(ReplicaType.CLOUD_BACKED, Utils.getObj(replicationConfig.replicationCloudTokenFactory));
  }

  /**
   * Get {@code FindTokenFactory} object based on {@code ReplicaType}
   * @param replicaType for which to get the {@code FindTokenFactory} object
   * @return {@code FindTokenFactory} object.
   * @throws ReflectiveOperationException
   */
  public FindTokenFactory getFindTokenFactoryFromReplicaType(ReplicaType replicaType) {
    if (!findTokenFactoryMap.containsKey(replicaType)) {
      throw new IllegalArgumentException("Invalid replica type " + replicaType.getClass());
    }
    return findTokenFactoryMap.get(replicaType);
  }
}
