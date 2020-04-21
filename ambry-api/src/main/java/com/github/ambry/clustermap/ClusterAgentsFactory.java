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

import java.io.IOException;


/**
 * A factory interface to get cluster agents such as {@link ClusterMap} and {@link ClusterParticipant}. Each type of
 * agents should be constructed at most once, and only on demand.
 */
public interface ClusterAgentsFactory {
  /**
   * Construct and return the reference or return the reference to the previously constructed
   * {@link ClusterMap}
   */
  ClusterMap getClusterMap() throws IOException;

  /**
   * Construct and return the reference or return the reference to the previously constructed
   * {@link ClusterParticipant}
   */
  ClusterParticipant getClusterParticipant() throws IOException;
}

