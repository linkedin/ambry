package com.github.ambry.router;

import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;


/**
 * A implementation of {@link OperationTracker}. It internally maintains the status of a
 * corresponding operation, and returns information that decides if the operation should
 * continue or terminate.
 *
 * This implementation simplifies such that it unifies parallelism. That is, a single parallelism
 * parameter controls the maximum number of total allowed inflight requests to both local and remote
 * replicas. This simplification is valid for PUT operation, yet a maturized implementation will take
 * a more sophisiticate control of parallelism in the future.
 *
 * This class assumes a request will be {@code succeeded, failed, or timedout} (which means failed).
 * So a deterministic response will be received in a definite time, and no request will pend forever.
 * When a request is timed out, it is considered as failed.
 *
 * A typical usage of an {@code SimpleOperationTracker} would be:
 *<pre>
 *{@code
 *
 *   SimpleOperationTracker operationTracker = new SimpleOperationTracker(datacenterName,
 *            partitionId, crossColoEnabled, successTarget, parallelism);
 *   //...
 *   Iterator<ReplicaId> itr = operationTracker.getReplicaIterator();
 *   while (itr.hasNext()) {
 *     ReplicaId nextReplica = itr.next();
 *     //determine request can be sent to the replica, i.e., connection available.
 *     if(true) {
 *       itr.remove();
 *     }
 *   }
 *}
 *</pre>
 *
 */
class SimpleOperationTracker implements OperationTracker {
  private final int successTarget;
  private final int parallelism;
  private final LinkedList<ReplicaId> replicaPool = new LinkedList<ReplicaId>();
  private final OpTrackerIterator otIterator;

  private int totalReplicaCount = 0;
  private int inflightCount = 0;
  private int succeededCount = 0;
  private int failedCount = 0;
  private Iterator<ReplicaId> replicaIterator;

  /**
   * Constructor for an {@code SimpleOperationTracker}.
   *
   * @param datacenterName The datacenter where the router is located.
   * @param partitionId The partition on which the operation is performed.
   * @param crossColoEnabled {@code true} if requests can be sent to remote replicas, {@code false}
   *                                otherwise.
   * @param successTarget The number of successful responses required to succeed the operation.
   * @param parallelism The maximum number of inflight requests at any point of time.
   */
  SimpleOperationTracker(String datacenterName, PartitionId partitionId, boolean crossColoEnabled, int successTarget,
      int parallelism) {
    this.successTarget = successTarget;
    this.parallelism = parallelism;
    List<ReplicaId> replicas = partitionId.getReplicaIds();
    Collections.shuffle(replicas);
    for (ReplicaId replicaId : replicas) {
      if (!replicaId.isDown()) {
        String replicaDcName = replicaId.getDataNodeId().getDatacenterName();
        if (replicaDcName.equals(datacenterName)) {
          replicaPool.add(0, replicaId);
        } else if (crossColoEnabled) {
          replicaPool.add(replicaId);
        }
      }
    }
    totalReplicaCount = replicaPool.size();
    this.otIterator = new OpTrackerIterator();
  }

  @Override
  public boolean hasSucceeded() {
    return succeededCount >= successTarget;
  }

  @Override
  public boolean isDone() {
    return hasSucceeded() || hasFailed();
  }

  @Override
  public void onResponse(ReplicaId replicaId, Exception e) {
    inflightCount--;
    if (e != null) {
      failedCount++;
    } else {
      succeededCount++;
    }
  }

  @Override
  public Iterator<ReplicaId> getReplicaIterator() {
    replicaIterator = replicaPool.iterator();
    return otIterator;
  }

  private class OpTrackerIterator implements Iterator<ReplicaId> {
    @Override
    public boolean hasNext() {
      return inflightCount < parallelism && replicaIterator.hasNext();
    }

    @Override
    public void remove() {
      replicaIterator.remove();
      inflightCount++;
    }

    @Override
    public ReplicaId next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return replicaIterator.next();
    }
  }

  private boolean hasFailed() {
    return (totalReplicaCount - failedCount ) < successTarget;
  }
}
