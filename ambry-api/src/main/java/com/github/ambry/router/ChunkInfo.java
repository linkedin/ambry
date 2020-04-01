/*
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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

import java.util.Objects;


/**
 * Represents a data chunk to be stitched by a {@link Router#stitchBlob} call.
 */
public class ChunkInfo {
  private final String blobId;
  private final long chunkSizeInBytes;
  private final long expirationTimeInMs;

  /**
   * @param blobId the blob ID for the chunk.
   * @param chunkSizeInBytes the size of the chunk content in bytes.
   * @param expirationTimeInMs the expiration time of the chunk in milliseconds.
   */
  public ChunkInfo(String blobId, long chunkSizeInBytes, long expirationTimeInMs) {
    this.blobId = Objects.requireNonNull(blobId, "blobId cannot be null");
    this.chunkSizeInBytes = chunkSizeInBytes;
    this.expirationTimeInMs = expirationTimeInMs;
  }

  /**
   * @return the blob ID for the chunk.
   */
  public String getBlobId() {
    return blobId;
  }

  /**
   * @return the size of the chunk content in bytes.
   */
  public long getChunkSizeInBytes() {
    return chunkSizeInBytes;
  }

  /**
   * @return the expiration time of the chunk in milliseconds.
   */
  public long getExpirationTimeInMs() {
    return expirationTimeInMs;
  }

  @Override
  public String toString() {
    return "ChunkInfo{" + "blobId='" + blobId + '\'' + ", chunkSizeInBytes=" + chunkSizeInBytes
        + ", expirationTimeInMs=" + expirationTimeInMs + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ChunkInfo chunkInfo = (ChunkInfo) o;
    return chunkSizeInBytes == chunkInfo.chunkSizeInBytes && expirationTimeInMs == chunkInfo.expirationTimeInMs
        && Objects.equals(blobId, chunkInfo.blobId);
  }

  @Override
  public int hashCode() {
    return blobId.hashCode();
  }
}
