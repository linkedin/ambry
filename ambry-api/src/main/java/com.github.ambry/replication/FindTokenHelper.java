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
import com.github.ambry.utils.PeekableInputStream;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.io.InputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helper class to get findtoken based on replica type or input stream.
 */
public class FindTokenHelper {
  private static final Logger logger = LoggerFactory.getLogger(FindTokenHelper.class);
  private final short LAST_STORE_ONLY_VERSION = 2;
  private final StoreKeyFactory storeKeyFactory;
  private final ReplicationConfig replicationConfig;

  /**
   * Create a {@code FindTokenHelper} object.
   * @param storeKeyFactory
   * @param replicationConfig
   */
  public FindTokenHelper(StoreKeyFactory storeKeyFactory, ReplicationConfig replicationConfig) {
    this.storeKeyFactory = storeKeyFactory;
    this.replicationConfig = replicationConfig;
  }

  /**
   * Get {@code FindTokenFactory} object based on {@code ReplicaType}
   * @param replicaType for which to get the {@code FindTokenFactory} object
   * @return {@code FindTokenFactory} object.
   * @throws ReflectiveOperationException
   */
  public FindTokenFactory getFindTokenFactoryFromType(ReplicaType replicaType) throws ReflectiveOperationException {
    FindTokenFactory findTokenFactory = null;
    if (replicaType.equals(ReplicaType.DISK_BACKED)) {
      findTokenFactory = Utils.getObj(replicationConfig.replicationStoreTokenFactory, storeKeyFactory);
    } else if (replicaType.equals(ReplicaType.CLOUD_BACKED)) {
      findTokenFactory = Utils.getObj(replicationConfig.replicationCloudTokenFactory, storeKeyFactory);
    }
    return findTokenFactory;
  }

  /**
   * Deserialize the correct {@code FindToken} object from the input stream.
   * @param inputStream from which to get the {@code FindToken} object.
   * @return {@code FindToken} object.
   * @throws IOException
   * @throws ReflectiveOperationException
   */
  public FindToken getFindTokenFromStream(InputStream inputStream) throws IOException, ReflectiveOperationException {
    PeekableInputStream peekableInputStream = new PeekableInputStream(inputStream);
    FindTokenFactory findTokenFactory = null;
    short version = peekableInputStream.peekShort();
    FindTokenType type = FindTokenType.values()[peekableInputStream.peekShort()];
    if (type == FindTokenType.CloudBased) {
      findTokenFactory = Utils.getObj(replicationConfig.replicationCloudTokenFactory, storeKeyFactory);
    } else {
      findTokenFactory = Utils.getObj(replicationConfig.replicationStoreTokenFactory, storeKeyFactory);
    }
    return findTokenFactory.getFindToken(peekableInputStream);
  }
}
