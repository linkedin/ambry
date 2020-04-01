/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.clustermap;

/**
 * An extension of {@link RuntimeException} used to record exceptions occurred during state transition.
 */
public class StateTransitionException extends RuntimeException {
  private static final long serialVersionUID = 1L;
  private final TransitionErrorCode error;

  public StateTransitionException(String s, TransitionErrorCode error) {
    super(s);
    this.error = error;
  }

  public TransitionErrorCode getErrorCode() {
    return error;
  }

  /**
   * All types of error code that associate with {@link StateTransitionException}. The error code is currently used by
   * tests to determine location of exception. In production environment, if transition exception occurs, the message
   * together with error code should be recorded in Helix log which helps us investigate failure cause.
   */
  public enum TransitionErrorCode {
    /**
     * If replica is not present in Helix and not found on current node.
     */
    ReplicaNotFound,
    /**
     * If failure occurs during replica operation (i.e. replica addition/removal in StoreManager, ReplicationManager).
     */
    ReplicaOperationFailure,
    /**
     * If store is not started and unavailable for specific operations.
     */
    StoreNotStarted,
    /**
     * If bootstrap process fails at some point for specific replica.
     */
    BootstrapFailure,
    /**
     * If failure occurs during Standby-To-Inactive transition.
     */
    DeactivationFailure,
    /**
     * If disconnection process fails on specific replica.
     */
    DisconnectionFailure,
    /**
     * If updating cluster info in Helix fails at some point for specific replica.
     */
    HelixUpdateFailure
  }
}
