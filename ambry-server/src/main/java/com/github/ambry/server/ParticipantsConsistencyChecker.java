/**
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.server;

import com.github.ambry.clustermap.ClusterParticipant;
import com.github.ambry.commons.ServerMetrics;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A thread that helps periodically check consistency of participants. Specifically, it checks if there is a mismatch
 * in sealed and stopped replicas from each participant.
 */
public class ParticipantsConsistencyChecker implements Runnable {
  private final List<ClusterParticipant> participants;
  private final ServerMetrics metrics;
  private static final Logger logger = LoggerFactory.getLogger(ParticipantsConsistencyChecker.class);

  public ParticipantsConsistencyChecker(List<ClusterParticipant> participants, ServerMetrics metrics) {
    this.participants = participants;
    this.metrics = metrics;
    metrics.registerParticipantsMismatchMetrics();
  }

  @Override
  public void run() {
    logger.debug("Participant consistency checker is initiated. Participants count = {}", participants.size());
    try {
      // when code reaches here, it means there are at least two participants.
      ClusterParticipant clusterParticipant = participants.get(0);
      Set<String> sealedReplicas1 = new HashSet<>(clusterParticipant.getSealedReplicas());
      Set<String> stoppedReplicas1 = new HashSet<>(clusterParticipant.getStoppedReplicas());
      for (int i = 1; i < participants.size(); ++i) {
        logger.debug("Checking sealed replica list");
        Set<String> sealedReplicas2 = new HashSet<>(participants.get(i).getSealedReplicas());
        if (!sealedReplicas1.equals(sealedReplicas2)) {
          logger.warn("Mismatch in sealed replicas. Set {} is different from set {}", sealedReplicas1, sealedReplicas2);
          metrics.sealedReplicasMismatchCount.inc();
        }
        logger.debug("Checking stopped replica list");
        Set<String> stoppedReplicas2 = new HashSet<>(participants.get(i).getStoppedReplicas());
        if (!stoppedReplicas1.equals(stoppedReplicas2)) {
          logger.warn("Mismatch in stopped replicas. Set {} is different from set {}", stoppedReplicas1,
              stoppedReplicas2);
          metrics.stoppedReplicasMismatchCount.inc();
        }
      }
    } catch (Throwable t) {
      logger.error("Exception occurs when running consistency checker ", t);
    }
  }
}
