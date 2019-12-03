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

  public enum TransitionErrorCode {
    /**
     * If replica is not present in Helix and not found on current node.
     */
    ReplicaNotFound,
    /**
     * If failure occurs during store operation (i.e. store addition/removal in StoreManager).
     */
    StoreOperationFailure,
    /**
     * If store is not started and unavailable for specific operations.
     */
    StoreNotStarted
  }
}
