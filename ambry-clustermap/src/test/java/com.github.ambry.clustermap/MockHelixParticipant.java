/*
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
package com.github.ambry.clustermap;

import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.server.AmbryHealthReport;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


public class MockHelixParticipant extends HelixParticipant {
  Set<ReplicaId> sealedReplicas = new HashSet<>();
  Set<ReplicaId> stoppedReplicas = new HashSet<>();

  public MockHelixParticipant(ClusterMapConfig clusterMapConfig) throws IOException {
    super(clusterMapConfig, new MockHelixManagerFactory());
  }

  @Override
  public void participate(List<AmbryHealthReport> ambryHealthReports) throws IOException {
    // no op
  }

  @Override
  public boolean setReplicaSealedState(ReplicaId replicaId, boolean isSealed) {
    if (isSealed) {
      sealedReplicas.add(replicaId);
    } else {
      sealedReplicas.remove(replicaId);
    }
    return true;
  }

  @Override
  public boolean setReplicaStoppedState(List<ReplicaId> replicaIds, boolean markStop) {
    if (markStop) {
      stoppedReplicas.addAll(replicaIds);
    } else {
      stoppedReplicas.removeAll(replicaIds);
    }
    return true;
  }

  @Override
  public List<String> getSealedReplicas() {
    return sealedReplicas.stream().map(r -> r.getPartitionId().toPathString()).collect(Collectors.toList());
  }

  @Override
  public List<String> getStoppedReplicas() {
    return stoppedReplicas.stream().map(r -> r.getPartitionId().toPathString()).collect(Collectors.toList());
  }

  @Override
  public void close() {
    // no op
  }

  /**
   * @return a snapshot of current state change listeners.
   */
  public Map<StateModelListenerType, PartitionStateChangeListener> getPartitionStateChangeListeners() {
    return Collections.unmodifiableMap(partitionStateChangeListeners);
  }
}
