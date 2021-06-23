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
package com.github.ambry.clustermap;

import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import java.util.List;


/**
 * Mock {@link VcrClusterAgentsFactory} to instantiate {@link MockVcrClusterSpectator}.
 */
public class MockVcrClusterAgentsFactory implements VcrClusterAgentsFactory {
  private final List<MockDataNodeId> cloudDataNodes;

  /**
   * Constructor for {@link MockVcrClusterAgentsFactory} object.
   * @param cloudDataNodes list of cloud {@link MockDataNodeId}s in instance config of {@link MockVcrClusterSpectator}.
   */
  public MockVcrClusterAgentsFactory(List<MockDataNodeId> cloudDataNodes) {
    this.cloudDataNodes = cloudDataNodes;
  }

  @Override
  public VcrClusterParticipant getVcrClusterParticipant() throws Exception {
    return null;
  }

  @Override
  public VcrClusterSpectator getVcrClusterSpectator(CloudConfig cloudConfig, ClusterMapConfig clusterMapConfig) {
    return new MockVcrClusterSpectator(cloudDataNodes);
  }
}
