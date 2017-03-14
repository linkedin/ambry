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


/**
 * A no-op {@link ClusterParticipant} associated with static cluster management.
 */
public class StaticParticipantFactory implements ClusterParticipantFactory {

  /**
   * Construct an instance of this factory.
   * @param clusterMapConfig unused.
   */
  public StaticParticipantFactory(ClusterMapConfig clusterMapConfig) {
    // no-op.
  }

  @Override
  public ClusterParticipant getClusterParticipant() {
    // Static participant methods are no-op.
    return new ClusterParticipant() {
      @Override
      public void initialize(String hostname, int port) throws Exception {
      }

      @Override
      public void terminate() {
      }
    };
  }
}

