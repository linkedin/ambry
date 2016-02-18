package com.github.ambry.router;

import com.github.ambry.clustermap.ReplicaId;


/**
 * An operation is an action that a router takes to handle a request it receives. An operation
 * will then send requests to multiple replicas. An operation succeeded if a pre-set number of
 * replicas return successful responses, or failed if this number cannot be met.
 *
 * An {@link OperationTracker} tracks and determines the status of an operation (e.g.,
 * succeeded or failed), and decides the next replica to send request. An {@link OperationTracker}
 * one-to-one tracks an operation.
 *
 * <p>
 * When an operation is progressing by receiving responses from replicas, its operation tracker
 * needs to be informed by calling {@code onResponse()}.
 * </p>
 */
public interface OperationTracker {
  /**
   * Determines if an operation has succeeded.
   *
   * @return {@code true} if the operation has successfully completed.
   */
  boolean hasSucceeded();

  /**
   * Determines if an operation has failed.
   *
   * @return {@code true} if the operation has failed.
   */
  boolean hasFailed();

  /**
   * Accounts for response from, or exception for a replica. Operation must invoke this method
   * if a successful response or an exception is received.
   *
   * @param replicaId ReplicaId that handled the request.
   * @param e Exception returned by the replica. {@code null} if the response is successful.
   */
  void onResponse(ReplicaId replicaId, Exception e);

  /**
   * Determines the next replica to send a request.
   *
   * @return ReplicaId of next replica to send to, {@code null} if no replica can be selected to send
   * request at the time this method is called.
   */
  ReplicaId getNextReplica();
}
