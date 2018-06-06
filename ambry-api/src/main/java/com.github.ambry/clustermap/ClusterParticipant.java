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
   * Initiate the participation of cluster participant.
   * @param ambryHealthReports {@link List} of {@link AmbryHealthReport} to be registered to the participant.
   * @throws IOException
   */
  void participate(List<AmbryHealthReport> ambryHealthReports) throws IOException;

  /**
   * Set or reset the sealed state of the given replica.
   * @param replicaId the {@link ReplicaId} whose sealed state will be updated.
   * @param isSealed if true, the replica will be marked as sealed; otherwise it will be marked as read-write.
   * @return {@code true} if set replica sealed state was successful. {@code false} if not.
   */
  boolean setReplicaSealedState(ReplicaId replicaId, boolean isSealed);

  /**
   * Set or reset the stopped state of the given replica.
   * @param replicaIds a list of replicas whose stopped state will be updated
   * @param markStop if true, the replica will be marked as stopped; otherwise it will be marked as started.
   * @return {@code true} if set replica stopped state was successful. {@code false} if not.
   */
  boolean setReplicaStoppedState(List<ReplicaId> replicaIds, boolean markStop);

  /**
   * Get a list of replicas that are marked as sealed (read-only).
   * @return a list of all sealed replicas.
   */
  List<String> getSealedReplicas();

  /**
   * Get a list of replicas that are marked as stopped.
   * @return a list of all stopped replicas.
   */
  List<String> getStoppedReplicas();

  /**
   * Terminate the participant.
   */
  @Override
  void close();
}
