package com.github.ambry.router;

import com.github.ambry.clustermap.ReplicaId;


/**
 * An OperationPolicy controls parallelism of an operation (how many requests in flight at a time), determines
 * the status of an operation (<i>completed, succeeded, failed, more replicas to send requests</i>), and decides
 * the next replica to send request if more replicas are allowed.
 * <p>
 * When an operation is progressing by sending requests to more replicas, or by receiving responses from replicas,
 * its OperationPolicy is informed by calling onSend() or onResponse() method.
 * </p>
 */
public interface OperationPolicy {
  /**
   * Determines if more requests should be sent.
   *
   * @return {@code true} iff one or more additional requests should be in flight
   */
  boolean shouldSendMoreRequests();

  /**
   * Determines if an operation has succeeded.
   *
   * @return {@code true} iff the operation has successfully completed.
   */
  boolean isSucceeded();

  /**
   * Determines if an operation has failed.
   *
   * @return {@code true} iff the operation has failed.
   */
  boolean isFailed();

  /**
   * Check if an operation has completed or not. An operation is in completed status if it is either in
   * successful or failed status.
   *
   * @return {@code true} if the operation has completed.
   */
  boolean isComplete();

  /**
   * Accounts for response from, or exception for a replica. Operation must invoke this method.
   *
   * @param replicaId ReplicaId that handled the request.
   * @param e Exception returned by the replica. {@code null} if the response is successful.
   */
  void onResponse(ReplicaId replicaId, Exception e);

  /**
   * Called when a request is sent to a replica.
   * @param replicaId the replica that the request is sent to.
   */
  void onSend(ReplicaId replicaId);

  /**
   * Determines the next replica to send a request. Decision is made depending on policy configuration.
   * A non-null replicaId is returned only when {@code shouldSendMoreRequests()} returns {@code true},
   * otherwise {@code false}.
   *
   * @return ReplicaId of next replica to send to.
   */
  ReplicaId getNextReplicaIdForSend();
}
