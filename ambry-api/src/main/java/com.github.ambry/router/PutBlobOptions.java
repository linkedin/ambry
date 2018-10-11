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
 *
 */

package com.github.ambry.router;

/**
 * Represents any options associated with a putBlob request.
 * @todo honor these options within the router impl
 */
public class PutBlobOptions {
  public static final PutBlobOptions DEFAULT = new PutBlobOptionsBuilder().build();
  private final boolean chunkUpload;
  private final long maxUploadSize;

  /**
   * @param chunkUpload {@code true} to indicate that the {@code putBlob()} call is for a single data chunk of a
   *                    stitched blob.
   * @param maxUploadSize the max size of the uploaded blob in bytes. To be enforced by the router. Can be null.
   */
  public PutBlobOptions(boolean chunkUpload, long maxUploadSize) {
    this.chunkUpload = chunkUpload;
    this.maxUploadSize = maxUploadSize;
  }

  /**
   * @return {@code true} to indicate that the {@code putBlob()} call is for a single data chunk of a
   *         stitched blob.
   */
  public boolean isChunkUpload() {
    return chunkUpload;
  }

  /**
   * @return the max size of the uploaded blob in bytes. To be enforced by the router. Can be null.
   */
  public long getMaxUploadSize() {
    return maxUploadSize;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PutBlobOptions options = (PutBlobOptions) o;
    return chunkUpload == options.chunkUpload && maxUploadSize == options.maxUploadSize;
  }

  @Override
  public int hashCode() {
    int result = (chunkUpload ? 1 : 0);
    result = 31 * result + (int) (maxUploadSize ^ (maxUploadSize >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "PutBlobOptions{" + "chunkUpload=" + chunkUpload + ", maxUploadSize=" + maxUploadSize + '}';
  }
}
