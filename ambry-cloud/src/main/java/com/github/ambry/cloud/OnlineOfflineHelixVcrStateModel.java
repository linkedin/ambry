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
  private final HelixVcrCluster helixVcrCluster;

  OnlineOfflineHelixVcrStateModel(HelixVcrCluster helixVcrCluster) {
    this.helixVcrCluster = helixVcrCluster;
  }

  @Transition(to = "ONLINE", from = "OFFLINE")
  public void onTransitionToOnlineFromOffline(Message message, NotificationContext context) {
    logger.info("{} Transitioning to ONLINE from OFFLINE for Partition {}", helixVcrCluster.getCurrentDataNodeId(),
        message.getPartitionName());
    helixVcrCluster.addPartition(message.getPartitionName());
  }

  @Transition(to = "OFFLINE", from = "ONLINE")
  public void onTransitionToOfflineFromOnline(Message message, NotificationContext context) {
    logger.info("{} Transitioning to OFFLINE from ONLINE of Partition {}", helixVcrCluster.getCurrentDataNodeId(),
        message.getPartitionName());
    helixVcrCluster.removePartition(message.getPartitionName());
  }

  @Transition(to = "DROPPED", from = "OFFLINE")
  public void onTransitionToDroppedFromOffline(Message message, NotificationContext context) {
    logger.info("{} Transitioning to DROPPED from OFFLINE of Partition {}", helixVcrCluster.getCurrentDataNodeId(),
        message.getPartitionName());
  }

  @Override
  public void reset() {
    // no op
  }
}
