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
package com.github.ambry.cloud;

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link StateModel} to use when the VCR participants register to Helix. The methods are callbacks
 * that get called within a participant whenever its state changes in Helix.
 */
@StateModelInfo(initialState = "OFFLINE", states = {"OFFLINE", "ONLINE", "DROPPED"})
public class OnlineOfflineHelixVcrStateModel extends StateModel {
  private static final Logger logger = LoggerFactory.getLogger(OnlineOfflineHelixVcrStateModel.class);
  private final HelixVcrClusterParticipant helixVcrClusterParticipant;

  OnlineOfflineHelixVcrStateModel(HelixVcrClusterParticipant helixVcrClusterParticipant) {
    this.helixVcrClusterParticipant = helixVcrClusterParticipant;
  }

  @Transition(to = "ONLINE", from = "OFFLINE")
  public void onTransitionToOnlineFromOffline(Message message, NotificationContext context) {
    logger.info("{} Transitioning to ONLINE from OFFLINE for Partition {}",
        helixVcrClusterParticipant.getCurrentDataNodeId(), message.getPartitionName());
    helixVcrClusterParticipant.addPartition(message.getPartitionName());
  }

  @Transition(to = "OFFLINE", from = "ONLINE")
  public void onTransitionToOfflineFromOnline(Message message, NotificationContext context) {
    logger.info("{} Transitioning to OFFLINE from ONLINE of Partition {}",
        helixVcrClusterParticipant.getCurrentDataNodeId(), message.getPartitionName());
    helixVcrClusterParticipant.removePartition(message.getPartitionName());
  }

  @Transition(to = "DROPPED", from = "OFFLINE")
  public void onTransitionToDroppedFromOffline(Message message, NotificationContext context) {
    logger.info("{} Transitioning to DROPPED from OFFLINE of Partition {}",
        helixVcrClusterParticipant.getCurrentDataNodeId(), message.getPartitionName());
  }

  @Override
  public void reset() {
    // no op
  }
}
