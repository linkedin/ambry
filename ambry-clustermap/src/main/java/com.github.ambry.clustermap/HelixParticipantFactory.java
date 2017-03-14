/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.config.VerifiableProperties;
import org.json.JSONException;


/**
 * Factory for creating a {@link HelixParticipant}
 */
public class HelixParticipantFactory implements ClusterParticipantFactory {
  private final ClusterMapConfig clusterMapConfig;

  /**
   * Instantiate the factory with the given parameters.
   * @param clusterMapConfig the {@link ClusterMapConfig} to associate with the cluster manager.
   */
  public HelixParticipantFactory(ClusterMapConfig clusterMapConfig) {
    this.clusterMapConfig = clusterMapConfig;
  }

  @Override
  public ClusterParticipant getClusterParticipant() throws JSONException {
    return new HelixParticipant(clusterMapConfig);
  }
}

