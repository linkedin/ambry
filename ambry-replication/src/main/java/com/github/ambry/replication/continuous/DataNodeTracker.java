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
package com.github.ambry.replication.continuous;

import com.github.ambry.clustermap.DataNodeId;
import java.util.List;


public class DataNodeTracker {
  private final DataNodeId dataNodeId;
  private final List<ActiveGroupTracker> activeGroupTrackers;
  private final StandByGroupTracker standByGroupTracker;

  public DataNodeTracker(DataNodeId dataNodeId, List<ActiveGroupTracker> activeGroupTrackers,
      StandByGroupTracker standByGroupTracker) {
    this.dataNodeId = dataNodeId;
    this.activeGroupTrackers = activeGroupTrackers;
    this.standByGroupTracker = standByGroupTracker;
  }

  public DataNodeId getDataNodeId() {
    return dataNodeId;
  }

  public List<ActiveGroupTracker> getActiveGroupTrackers() {
    return activeGroupTrackers;
  }

  public StandByGroupTracker getStandByGroupTracker() {
    return standByGroupTracker;
  }
}
