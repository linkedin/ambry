package com.github.ambry;

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;


// The OnlineOfflineStateModel Helix comes with would be sufficient in Ambry.
// Including the below to give clarity on how the state model works
// and in case we want to bring in the "SEALED" aspect into the state model.
// The reason to define that at the resource level is that a replica does not
// ever individually become SEALED - the resource (ambry partition) as a whole is
// either SEALED or WRITABLE.
public class AmbryStateModelFactory extends StateModelFactory<AmbryStateModel> {
  @Override
  public AmbryStateModel createNewStateModel(String resource, String partitionName) {
    return new AmbryStateModel();
  }
}

@StateModelInfo(initialState = "ONLINE", states = {
    "ONLINE", "OFFLINE"
})

class AmbryStateModel extends StateModel {
  @Transition(to = "OFFLINE", from = "ONLINE")
  public void onOfflineFromOnline(Message message, NotificationContext context) {

  }

  @Transition(to = "ONLINE", from = "OFFLINE")
  public void onOnlineFromOffline(Message message, NotificationContext context) {
  }
}