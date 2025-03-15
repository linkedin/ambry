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
   * The hostname of the target node
   */
  private final String hostName;

  /**
   * The name of the file to be copied
   */
  private String fileName;

  /**
   * The start offset of the file to be copied
   */
  private long startOffset;

  /**
   * The length of the chunk to be copied
   */
  private long chunkLengthInBytes;

  /**
   * Whether the file is chunked or not
   */
  private boolean isChunked;

  /**
   * Constructor to create FileCopyInfo
   * @param correlationId The correlation id of the request
   * @param clientId The client id of the request
   * @param sourceReplicaId The source replica id
   * @param targetReplicaId The target replica id
   * @param hostName The hostname of the target node
   */
  public FileCopyInfo(int correlationId, @Nonnull String clientId, @Nonnull ReplicaId sourceReplicaId,
      @Nonnull ReplicaId targetReplicaId, @Nonnull String hostName) {
    Objects.requireNonNull(clientId, "clientId cannot be null");
    Objects.requireNonNull(sourceReplicaId, "sourceReplicaId cannot be null");
    Objects.requireNonNull(targetReplicaId, "targetReplicaId cannot be null");
    Objects.requireNonNull(hostName, "hostName cannot be null");

    this.correlationId = correlationId;
    this.clientId = clientId;
    this.sourceReplicaId = sourceReplicaId;
    this.targetReplicaId = targetReplicaId;
    this.hostName = hostName;
  }

  /**
   * Set the chunk info. This is required for making GetChunkData request
   * @param fileName
   * @param startOffset
   * @param chunkLengthInBytes
   * @param isChunked
   */
  public void setChunkInfo(String fileName, long startOffset, long chunkLengthInBytes, boolean isChunked) {
    this.fileName = fileName;
    this.startOffset = startOffset;
    this.chunkLengthInBytes = chunkLengthInBytes;
    this.isChunked = isChunked;
  }

  public int getCorrelationId() {
    return correlationId;
  }

  public String getClientId() {
    return clientId;
  }

  public ReplicaId getSourceReplicaId() {
    return sourceReplicaId;
  }

  public ReplicaId getTargetReplicaId() {
    return targetReplicaId;
  }

  public String getHostName() {
    return hostName;
  }

  public String getFileName() {
    return fileName;
  }

  public long getStartOffset() {
    return startOffset;
  }

  public long getChunkLengthInBytes() {
    return chunkLengthInBytes;
  }

  public boolean isChunked() {
    return isChunked;
  }
}
