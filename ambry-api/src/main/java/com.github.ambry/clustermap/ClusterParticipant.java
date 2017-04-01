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
 * A ClusterParticipant is a component that makes up the Ambry cluster.
 */
public interface ClusterParticipant extends AutoCloseable {
  /**
   * Initialize the participant.
   */
  void initialize(String hostname, int port) throws IOException;

  /**
   * Terminate the participant.
   */
  @Override
  void close();
}
