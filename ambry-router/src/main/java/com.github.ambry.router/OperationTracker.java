package com.github.ambry.router;

import com.github.ambry.clustermap.ReplicaId;


/**
 * An operation is an action that a router takes to handle a request it receives. An operation
 * will then send requests to multiple replicas. An operation succeeded if a pre-set number of
 * replicas return successful responses, or failed if this number cannot be met.
 *
 * An {@code OperationTracker} tracks and determines the status of an operation (e.g., <i>completed,
 * succeeded, failed, if more replicas to send requests to succeed the operation</i>), and
 * decides the next replica to contact if more replicas are allowed. An OperationTracker
 * one-to-one tracks an operation.
 *
 * <p>
 * When an operation is progressing by sending requests to more replicas, or by receiving
 * responses from replicas, its operation tracker needs to be informed by calling {@code
 * onSend()} or {@code onResponse()}.
 * </p>
 */
public interface OperationTracker {
  /**
   * Determines if requests can be sent to more replicas.
   *
   * @return {@code true} if one or more additional requests can be sent out.
   */
  boolean shouldSendMoreRequests();

  /**
   * Determines if an operation has succeeded.
   *
   * @return {@code true} if the operation has successfully completed.
   */
  boolean isSucceeded();

  /**
   * Determines if an operation has failed.
   *
   * @return {@code true} if the operation has failed.
   */
  boolean isFailed();

  /**
   * Check if an operation has completed or not. An operation has completed when its status
   * is deterministic (either in successful or failed status).
   *
   * @return {@code true} if the operation has completed.
   */
  boolean isComplete();

  /**
   * Accounts for response from, or exception for a replica. Operation must invoke this method
   * if a successful response or an exception is received.
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
   * Determines the next replica to send a request. A non-null replicaId is returned only when
   * {@code shouldSendMoreRequests()} returns {@code true}, otherwise {@code false}.
   *
   * @return ReplicaId of next replica to send to, {@code null} if requests cannot be sent.
   */
  ReplicaId getNextReplicaIdForSend();
}
