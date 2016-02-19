package com.github.ambry.router;

import com.github.ambry.clustermap.ReplicaId;
import java.util.Iterator;


/**
 * An operation is an action that a router takes to handle a request it receives. An operation
 * will then send requests to multiple replicas. An operation succeeded if a pre-set number of
 * successful responses are received from the replicas, or failed if this number cannot be met.
 *
 * An {@link OperationTracker} tracks and determines the status of an operation (e.g.,
 * succeeded or done), and decides the next replica to send a request. An {@link OperationTracker}
 * one-to-one tracks an operation.
 *
 * <p>
 * When an operation is progressing by receiving responses from replicas, its operation tracker
 * needs to be informed by calling {@code onResponse()}.
 * </p>
 */
public interface OperationTracker extends Iterator<ReplicaId> {
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
   * Accounts for response from, or exception for a replica. Operation must invoke this method
   * if a successful response or an exception is received.
   *
   * @param replicaId ReplicaId that handled the request.
   * @param e Exception returned by the replica. {@code null} if the response is successful.
   */
  void onResponse(ReplicaId replicaId, Exception e);

  /**
   * Provide an iterator for the underlying replica collection.
   *
   * @return An iterator that iterates all possible and valid replicas
   */
  Iterator<ReplicaId> getIterator();
}
