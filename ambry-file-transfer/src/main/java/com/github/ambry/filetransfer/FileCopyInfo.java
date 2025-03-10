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

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ReplicaId;


public class FileCopyInfo {
  private final int correlationId;
  private final String clientId;
  private final ReplicaId sourceReplicaId;
  private final ReplicaId targetReplicaId;
  private final String hostName;

  private String fileName;
  private long startOffset;
  private long chunkLengthInBytes;
  private boolean isChunked;

  private boolean initialised = false;

  public FileCopyInfo(int correlationId, String clientId, ReplicaId sourceReplicaId,
      ReplicaId targetReplicaId, String hostName) {
    this.correlationId = correlationId;
    this.clientId = clientId;
    this.sourceReplicaId = sourceReplicaId;
    this.targetReplicaId = targetReplicaId;
    this.hostName = hostName;

    this.initialised = true;
  }

  public FileCopyInfo setChunkInfo(String fileName, long startOffset, long chunkLengthInBytes, boolean isChunked) {
    if (!initialised) {
      throw new IllegalStateException("FileCopyInfo not initialised");
    }
    return new FileCopyInfo(correlationId, clientId, sourceReplicaId, targetReplicaId, hostName, fileName,
        startOffset, chunkLengthInBytes, isChunked);
  }

  private FileCopyInfo(int correlationId, String clientId, ReplicaId sourceReplicaId,
      ReplicaId targetReplicaId, String hostName, String fileName,
      long startOffset, long chunkLengthInBytes, boolean isChunked) {
    this.correlationId = correlationId;
    this.clientId = clientId;
    this.sourceReplicaId = sourceReplicaId;
    this.targetReplicaId = targetReplicaId;
    this.hostName = hostName;
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
