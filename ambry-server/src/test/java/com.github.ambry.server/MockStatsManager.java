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
package com.github.ambry.server;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.config.StatsManagerConfig;
import com.github.ambry.store.StorageManager;
import com.github.ambry.utils.MockTime;
import java.util.List;


/**
 * An extension of {@link StatsManager} to help with tests.
 */
class MockStatsManager extends StatsManager {
  boolean returnValOfAddReplica = true;

  MockStatsManager(StorageManager storageManager, List<? extends ReplicaId> replicaIds, MetricRegistry metricRegistry,
      StatsManagerConfig statsManagerConfig) {
    super(storageManager, replicaIds, metricRegistry, statsManagerConfig, new MockTime());
  }

  @Override
  boolean addReplica(ReplicaId id) {
    return returnValOfAddReplica;
  }
}
