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

import com.github.ambry.server.AmbryHealthReport;
import java.io.IOException;
import java.util.List;


/**
 * A ClusterParticipant is a component that makes up the Ambry cluster.
 */
public interface ClusterParticipant extends AutoCloseable {

  /**
   * Initialize the participant.
   * @param hostname the hostname to use when registering as a participant.
   * @param port the port to use when registering as a participant.
   * @param ambryHealthReports {@link List} of {@link AmbryHealthReport} to be registered to the participant.
   * @throws IOException
   */
  void initialize(String hostname, int port, List<AmbryHealthReport> ambryHealthReports) throws IOException;

  /**
   * Set or reset the sealed state of the given replica.
   * @param replicaId the {@link ReplicaId}
   * @param isSealed if true, the replica will be marked as sealed; otherwise it will be marked as read-write.
   */
  boolean setReplicaSealedState(ReplicaId replicaId, boolean isSealed);

  /**
   * Terminate the participant.
   */
  @Override
  void close();
}
