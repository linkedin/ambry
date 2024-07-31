/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.clustermap.DataNodeId;
import java.util.ArrayList;
import java.util.List;


class DataNodeTracker {
  private final DataNodeId nodeId;
  private final List<ActiveGroupTracker> activeGroupTrackers;
  private final StandByGroupTracker standByGroupTracker;

  DataNodeTracker(DataNodeId nodeId, List<ActiveGroupTracker> activeGroupTrackers,
      StandByGroupTracker standByGroupTracker) {
    this.nodeId = nodeId;
    this.activeGroupTrackers = activeGroupTrackers;
    this.standByGroupTracker = standByGroupTracker;
  }

  DataNodeId getNodeId() {
    return nodeId;
  }

  List<ActiveGroupTracker> getActiveGroupTrackers(){
    return activeGroupTrackers;
  }

  StandByGroupTracker getStandByGroupTracker(){
    return standByGroupTracker;
  }

  List<RemoteReplicaGroupTracker> getGroupTrackers() {
    List<RemoteReplicaGroupTracker> res = new ArrayList<>(activeGroupTrackers);
    res.add(standByGroupTracker);
    return res;
  }


}