/**
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.Callback;


/**
 * If some operation like {@link TtlUpdateOperation} or {@link DeleteOperation} fails due to some replicas don't have the Blob.
 * We use {@link ReplicateBlobOperation} to replicate the blob to the NOT_FOUND replicas.
 * After that, retry the failed operation for example the {@link TtlUpdateOperation}
 * ReplicateBlobCallback is the callback function of the {@link ReplicateBlobOperation},
 * also it stores the retry state.
 */
public class ReplicateBlobCallback implements Callback<Void> {

  /**
   * There are three stages during the retry
   * 1. Sends out the ReplicateBlob, waiting for the response
   * 2. ReplicateBlob is finished. ReplicateBlobCallback is called.
   * 3. Retrying the original operation like TtlUpdateOperation or DeleteOperation
   */
  public enum State {
    REPLICATING_BLOB, // replicating the blob
    REPLICATION_DONE, // blob replication is done
    RETRYING          // retry the original failed operation
  }

  private final BlobId blobId;
  private final DataNodeId sourceDataNode;
  private volatile State retryState;
  private Exception exception;

  public ReplicateBlobCallback(BlobId blobId, DataNodeId sourceDataNode) {
    this.blobId = blobId;
    this.sourceDataNode = sourceDataNode;
    this.retryState = State.REPLICATING_BLOB;
  }

  /**
   * @return the BlobId
   */
  public BlobId getBlobId() {
    return blobId;
  }

  /**
   * @return the source data node from which we get the Blob
   */
  public DataNodeId getSourceDataNode() {
    return sourceDataNode;
  }

  /**
   * @return the {@link State}
   */
  public State getState() {
    return retryState;
  }

  /**
   * Change the {@link State}
   */
  public void setState(State newState) {
    this.retryState = newState;
  }

  /**
   * @return the {@link Exception} of the ReplicateBlob operation
   */
  public Exception getException() {
    return exception;
  }

  /**
   * Will be called when the ReplicateBlob operation is done.
   * @param exception The exception that was reported on execution of the request (if any).
   */
  @Override
  public void onCompletion(Void v, Exception exception) {
    this.exception = exception;
    this.retryState = State.REPLICATION_DONE;
  }
}
