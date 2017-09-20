package com.github.ambry.clustermap;

public class ClusterManagerWriteStatusDelegate {

  private static ClusterManagerWriteStatusDelegate instance;

  private final ClusterParticipant clusterParticipant;

  /**
   *
   * @param clusterParticipant
   * @return
   */
  public static ClusterManagerWriteStatusDelegate getInstance(ClusterParticipant clusterParticipant) {
    if (instance == null)
    {
      instance = new ClusterManagerWriteStatusDelegate(clusterParticipant);
    }
    return instance;
  }

  private ClusterManagerWriteStatusDelegate(ClusterParticipant clusterParticipant) {
    this.clusterParticipant = clusterParticipant;
  }

  /**
   * Tells ClusterManager that blob store is now read only
   * @param replicaId
   */
  public void setToRO(ReplicaId replicaId) {
    clusterParticipant.setReplicaSealedState(replicaId, true);
  }

  /**
   * Tells ClusterManager that blob store is now read write
   * @param replicaId
   */
  public void setToRW(ReplicaId replicaId) {
    clusterParticipant.setReplicaSealedState(replicaId, false);
  }
}
