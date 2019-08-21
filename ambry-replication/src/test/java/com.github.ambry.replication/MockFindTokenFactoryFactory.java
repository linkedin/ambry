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
import java.io.DataInputStream;


public class MockFindTokenFactoryFactory extends FindTokenFactoryFactory {

  public MockFindTokenFactoryFactory(StoreKeyFactory storeKeyFactory, ReplicationConfig replicationConfig) {
    super(storeKeyFactory, replicationConfig);
  }

  @Override
  public FindTokenFactory getFindTokenFactoryFromType(ReplicaType replicaType) throws ReflectiveOperationException {
    return new MockFindToken.MockFindTokenFactory();
  }

  @Override
  public FindTokenFactory getFindTokenFactoryFromStream(PeekableInputStream inputStream) {
    return new MockFindToken.MockFindTokenFactory();
  }
}
