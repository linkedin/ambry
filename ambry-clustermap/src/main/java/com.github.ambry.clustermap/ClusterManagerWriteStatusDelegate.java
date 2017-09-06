package com.github.ambry.clustermap;

public class ClusterManagerWriteStatusDelegate {

  private static ClusterManagerWriteStatusDelegate instance;

  private final HelixParticipant helixParticipant;

  /**
   *
   * @param helixParticipant
   * @return
   */
  public static ClusterManagerWriteStatusDelegate getInstance(HelixParticipant helixParticipant) {
    if (instance == null)
    {
      instance = new ClusterManagerWriteStatusDelegate(helixParticipant);
    }
    return instance;
  }

  private ClusterManagerWriteStatusDelegate(HelixParticipant helixParticipant) {
    this.helixParticipant = helixParticipant;
  }

  /**
   * Tells ClusterManager that blob store is now read only
   * @param replicaId
   */
  public void setToRO(ReplicaId replicaId) {
    helixParticipant.setReplicaSealedState(replicaId, true);
  }

  /**
   * Tells ClusterManager that blob store is now read write
   * @param replicaId
   */
  public void setToRW(ReplicaId replicaId) {
    helixParticipant.setReplicaSealedState(replicaId, false);
  }
}
