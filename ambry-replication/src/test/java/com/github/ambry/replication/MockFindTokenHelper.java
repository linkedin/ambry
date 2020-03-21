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


public class MockFindTokenHelper extends FindTokenHelper {

  public MockFindTokenHelper(StoreKeyFactory storeKeyFactory, ReplicationConfig replicationConfig)
      throws ReflectiveOperationException {
  }

  @Override
  public FindTokenFactory getFindTokenFactoryFromReplicaType(ReplicaType replicaType) {
    return new MockFindTokenFactory();
  }
}
