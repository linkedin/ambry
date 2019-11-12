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

import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;


/**
 * A factory to create {@link ClusterSpectator} object.
 */
public class HelixClusterSpectatorFactory implements ClusterSpectatorFactory {

  @Override
  public ClusterSpectator getClusterSpectator(CloudConfig cloudConfig, ClusterMapConfig clusterMapConfig) {
    return new HelixClusterSpectator(cloudConfig, clusterMapConfig);
  }
}
