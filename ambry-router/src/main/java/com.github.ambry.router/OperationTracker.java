/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
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
 * needs to be informed by calling {@link #onResponse(ReplicaId, boolean)}.
 *
 * Typical usage of an {@code OperationTracker} would be:
 * <pre>
 *{@code
 *
 *   Operation operationTracker = new OperationTrackerImpl(..);
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
 * </pre>
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
   * Accounts for successful or failed response from a replica. Must invoke this method
   * if a successful or failed response is received for a replica.
   *
   * @param replicaId ReplicaId associated with this response.
   * @param isSuccessful Whether the request to the replicaId is successful or not.
   */
  void onResponse(ReplicaId replicaId, boolean isSuccessful);

  /**
   * Provide an iterator to the replicas to which requests may be sent. Each time when start to iterate
   * the replicas of an {@code OperationTracker}, an iterator needs to be obtained by calling this
   * method. If a request is sent to a {@link ReplicaId} obtained from the returned iterator, the {@link ReplicaId}
   * has to be removed from the iterator in order for it to be tracked.
   *
   * @return An iterator that iterates all possible and valid replicas.
   */
  Iterator<ReplicaId> getReplicaIterator();
}
