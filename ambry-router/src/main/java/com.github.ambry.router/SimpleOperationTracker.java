package com.github.ambry.router;

import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;


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
 *       Iterator<ReplicaId> itr = operationTracker.getReplicaIterator();
 *       while (itr.hasNext()) {
 *         ReplicaId nextReplica = itr.next();
 *         //determine request can be sent to the replica, i.e., connection available.
 *         if(true) {
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
  private Iterator<ReplicaId> replicaIterator;
  private final LinkedList<ReplicaId> unsentReplicas = new LinkedList<ReplicaId>();
  private final OpTrackerIterator otIterator;

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
    this.otIterator = new OpTrackerIterator();
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
  public Iterator<ReplicaId> getReplicaIterator() {
    replicaIterator = unsentReplicas.iterator();
    return otIterator;
  }

  private class OpTrackerIterator implements Iterator<ReplicaId> {
    @Override
    public boolean hasNext() {
      if (numInflight == parallelism) {
        return false;
      } else {
        return replicaIterator.hasNext();
      }
    }

    @Override
    public void remove() {
      replicaIterator.remove();
      numInflight++;
    }

    @Override
    public ReplicaId next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      } else {
        return replicaIterator.next();
      }
    }
  }

  private boolean hasFailed() {
    return numTotalReplica - numFailed < successTarget;
  }
}
