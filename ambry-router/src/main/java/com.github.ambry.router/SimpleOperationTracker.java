package com.github.ambry.router;

import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;

import java.util.*;


/**
 * A implementation of {@link OperationTracker} used by non-blocking router. It internally maintains
 * the status of the corresponding operation, and returns information to progress or terminate the
 * operation.
 *
 * This implementation simplifies such that it unifies parallelism. That is, a single parallelism
 * parameter controls the maximum number of allowed inflight requests to both local and remote
 * replicas. This simplification is valid for PUT operation, yet a maturized implementation will take
 * a more sophisiticate control of parallelism in the future.
 *
 * This class assumes a request will be {@code succeeded, failed, or timed out} (which means failed).
 * No request will pend forever.
 *
 * A typical usage of an {@link SimpleOperationTracker} would be:
 *<pre>
 *{@code
 *
 *   RouterOperationTracker operationTracker = new RouterOperationTracker(datacenterName,
 *            partitionId, localOnly, successTarget, parallelism);
 *   //...
 *       Iterator<ReplicaId> itr = operationTracker.getIterator();
 *       while (itr.hasNext()) {
 *         ReplicaId nextReplica = itr.next();
 *         //send a request to the replica.
 *         if(send is successful) {
 *           itr.remove();
 *         }
 *       }
 *}
 *</pre>
 *
 */
public class SimpleOperationTracker implements OperationTracker {
  final private boolean localDcOnly;
  final private int successTarget;
  final private int parallelism;

  private int numTotalReplica = 0;
  private int numInflight = 0;
  private int numSucceeded = 0;
  private int numFailed = 0;
  private final String localDcName;
  private final PartitionId partitionId;
  private Iterator<ReplicaId> iterator;
  private final LinkedList<ReplicaId> unsentReplicas = new LinkedList<ReplicaId>();


  /**
   * Constructor for an {@code OperationTracker}.
   *
   * @param datacenterName The datacenter where the router is located.
   * @param pId The partition on which the operation is performed.
   * @param localDcOnly {@code true} if requests only can be sent to local replicas, {@code false}
   *                                otherwise.
   * @param successTarget The number of successful responses received to succeed the operation.
   * @param parallelism The maximum number of inflight requests sent to all replicas.
   */
  public SimpleOperationTracker(String datacenterName, PartitionId pId, boolean localDcOnly, int successTarget,
      int parallelism) {
    this.localDcOnly = localDcOnly;
    this.successTarget = successTarget;
    this.parallelism = parallelism;
    this.localDcName = datacenterName;
    this.partitionId = pId;
    List<ReplicaId> replicas = partitionId.getReplicaIds();
    Collections.shuffle(replicas);
    for (ReplicaId replicaId : replicas) {
      if (!replicaId.isDown()) {
        String replicaDcName = replicaId.getDataNodeId().getDatacenterName();
        if (replicaDcName.equals(localDcName)) {
          unsentReplicas.add(0, replicaId);
        } else if (!localDcOnly) {
          unsentReplicas.add(replicaId);
        }
      }
    }
    numTotalReplica = unsentReplicas.size();
    iterator = unsentReplicas.iterator();
  }

  @Override
  public boolean hasNext() {
    if (hasSucceeded() || hasFailed() || numInflight == parallelism) {
      return false;
    } else {
      return iterator.hasNext();
    }
  }

  @Override
  public void remove() {
    iterator.remove();
    numInflight++;
  }

  @Override
  public boolean hasSucceeded() {
    return numSucceeded >= successTarget;
  }

  @Override
  public boolean isDone() {
    return hasSucceeded() || hasFailed();
  }

  @Override
  public void onResponse(ReplicaId replicaId, Exception e) {
    numInflight--;
    if (e != null) {
      numFailed++;
    } else {
      numSucceeded++;
    }
  }

  @Override
  public ReplicaId next() {
    if (!hasNext()) {
      return null;
    } else {
      return iterator.next();
    }
  }

  @Override
  public Iterator<ReplicaId> getIterator() {
    iterator = unsentReplicas.iterator();
    return this;
  }

  private boolean hasFailed() {
    return numTotalReplica - numFailed < successTarget;
  }
}
