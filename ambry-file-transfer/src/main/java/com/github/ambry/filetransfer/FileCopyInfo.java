/**
 * Copyright 2025 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.filetransfer;

import com.github.ambry.clustermap.ReplicaId;
import java.util.Objects;
import javax.annotation.Nonnull;


/**
 * FileCopyInfo contains the information required to copy a file from one node to another.
 */
public class FileCopyInfo {
  /**
   * The correlation id of the request
   */
  private final int correlationId;

  /**
   * The client id of the request
   */
  private final String clientId;

  /**
   * The source replica id
   */
  private final ReplicaId sourceReplicaId;

  /**
   * The target replica id
   */
  private final ReplicaId targetReplicaId;

  /**
   * Constructor to create FileCopyInfo
   * @param correlationId The correlation id of the request
   * @param clientId The client id of the request
   * @param sourceReplicaId The source replica id
   * @param targetReplicaId The target replica id
   */
  public FileCopyInfo(int correlationId, @Nonnull String clientId, @Nonnull ReplicaId sourceReplicaId,
      @Nonnull ReplicaId targetReplicaId) {
    Objects.requireNonNull(clientId, "clientId cannot be null");
    Objects.requireNonNull(sourceReplicaId, "sourceReplicaId cannot be null");
    Objects.requireNonNull(targetReplicaId, "targetReplicaId cannot be null");

    this.correlationId = correlationId;
    this.clientId = clientId;
    this.sourceReplicaId = sourceReplicaId;
    this.targetReplicaId = targetReplicaId;
  }

  /**
   * Get the correlation id of the request
   */
  public int getCorrelationId() {
    return correlationId;
  }

  /**
   * Get the client id of the request
   */
  public String getClientId() {
    return clientId;
  }

  /**
   * Get the source replica id
   */
  public ReplicaId getSourceReplicaId() {
    return sourceReplicaId;
  }

  /**
   * Get the target replica id
   */
  public ReplicaId getTargetReplicaId() {
    return targetReplicaId;
  }
}
