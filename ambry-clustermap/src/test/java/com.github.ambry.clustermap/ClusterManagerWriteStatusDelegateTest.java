package com.github.ambry.clustermap;

import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.*;


public class ClusterManagerWriteStatusDelegateTest {

  /**
   * Tests ClusterManagerWriteStatusDelegate
   */
  @Test
  public void testDelegate() {
    //Initializes delegate and arguments
    ClusterParticipant clusterParticipant = mock(ClusterParticipant.class);
    ReplicaId replicaId = mock(ReplicaId.class);
    ClusterManagerWriteStatusDelegate delegate = ClusterManagerWriteStatusDelegate.getInstance(clusterParticipant);

    //Checks that the right underlying ClusterParticipant methods are called
    delegate.setToRO(replicaId);
    verify(clusterParticipant).setReplicaSealedState(replicaId, true);
    delegate.setToRW(replicaId);
    verify(clusterParticipant).setReplicaSealedState(replicaId, false);
  }

}
