package com.github.ambry.router;

import com.github.ambry.clustermap.ReplicaId;
import java.util.Iterator;


/**
 * An operation is an action that a router takes to handle a request it receives. An operation
 * will then send requests to multiple replicas. An operation succeeds if a pre-set number of
 * successful responses are received from the replicas, or fails if this number cannot be met.
 *
 * An {@code OperationTracker} tracks and determines the status of an operation, and decides the
 * next replica to send a request.
 *
 * When an operation is progressing by receiving responses from replicas, its {@code OperationTracker}
 * needs to be informed by calling {@link #onResponse(ReplicaId, Exception)}.
 */
interface OperationTracker {
  /**
   * Determines if an operation has succeeded.
   *
   * @return {@code true} if the operation has successfully completed.
   */
  boolean hasSucceeded();

  /**
   * Determines if an operation has completed (either succeeded or failed).
   *
   * @return {@code true} if the operation has completed.
   */
  boolean isDone();

  /**
   * Accounts for response from, or exception for a replica. must invoke this method
   * if a response or an exception is received.
   *
   * @param replicaId ReplicaId that returns a response or an excepton.
   * @param e Exception returned by the replica. {@code null} if the response is successful.
   */
  void onResponse(ReplicaId replicaId, Exception e);

  /**
   * Provide an iterator to the replicas to which requests may be sent. Each time when start to iterate
   * the replicas of an {@code OperationTracker}, an iterator needs to be obtained by calling this
   * method.
   *
   * @return An iterator that iterates all possible and valid replicas.
   */
  Iterator<ReplicaId> getReplicaIterator();
}
