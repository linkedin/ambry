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
import com.github.ambry.utils.PeekableInputStream;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.CrcInputStream;
import com.github.ambry.utils.Utils;
import java.io.EOFException;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FindTokenFactoryFactory {
  protected final Logger logger = LoggerFactory.getLogger(getClass());
  private final short LAST_STORE_ONLY_VERSION = 2;
  private final StoreKeyFactory storeKeyFactory;
  private final ReplicationConfig replicationConfig;

  public FindTokenFactoryFactory(StoreKeyFactory storeKeyFactory, ReplicationConfig replicationConfig) {
    this.storeKeyFactory = storeKeyFactory;
    this.replicationConfig = replicationConfig;
  }

  public FindTokenFactory getFindTokenFactoryFromType(ReplicaType replicaType) throws ReflectiveOperationException {
    FindTokenFactory findTokenFactory = null;
    if (replicaType.equals(ReplicaType.DISK_BACKED)) {
      findTokenFactory = Utils.getObj(replicationConfig.replicationStoreTokenFactory, storeKeyFactory);
    } else if (replicaType.equals(ReplicaType.CLOUD_BACKED)) {
      findTokenFactory = Utils.getObj(replicationConfig.replicationCloudTokenFactory, storeKeyFactory);
    }
    return findTokenFactory;
  }

  public FindTokenFactory getFindTokenFactoryFromStream(PeekableInputStream inputStream)
      throws IOException, ReflectiveOperationException {

    FindTokenFactory findTokenFactory = null;
    short version = peekShort(inputStream);
    FindTokenType type = FindTokenType.values()[peekShort(inputStream)];
    if (type == FindTokenType.CloudBased) {
      findTokenFactory = Utils.getObj(replicationConfig.replicationCloudTokenFactory, storeKeyFactory);
    } else {
      findTokenFactory = Utils.getObj(replicationConfig.replicationStoreTokenFactory, storeKeyFactory);
    }
    return findTokenFactory;
  }

  private final short peekShort(PeekableInputStream in) throws IOException {
    int ch1 = in.peek();
    int ch2 = in.peek();
    if ((ch1 | ch2) < 0)
      throw new EOFException();
    return (short)((ch1 << 8) + (ch2 << 0));
  }
}
