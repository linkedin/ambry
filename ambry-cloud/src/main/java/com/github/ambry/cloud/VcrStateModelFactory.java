package com.github.ambry.cloud;

import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;


/**
 * An abstract class to create {@link StateModelFactory} for vcr.
 */
public abstract class VcrStateModelFactory extends StateModelFactory<StateModel> {
  protected HelixVcrCluster helixVcrCluster;

  /**
   * @return Helix state model name of this state model.
   */
  abstract String getStateModelName();
}
