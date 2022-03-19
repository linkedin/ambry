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

import com.github.ambry.rest.RestRequest;
import java.util.Objects;


/**
 * Represents any options associated with a putBlob request.
 */
public class PutBlobOptions {
  public static final PutBlobOptions DEFAULT = new PutBlobOptionsBuilder().build();
  private final boolean chunkUpload;
  private final long maxUploadSize;
  private final RestRequest restRequest;

  /**
   * @param chunkUpload {@code true} to indicate that the {@code putBlob()} call is for a single data chunk of a
   *                    stitched blob.
   * @param maxUploadSize the max size of the uploaded blob in bytes. To be enforced by the router. Can be null.
   * @param restRequest The {@link RestRequest} that triggered this put operation.
   */
  public PutBlobOptions(boolean chunkUpload, long maxUploadSize, RestRequest restRequest) {
    this.chunkUpload = chunkUpload;
    this.maxUploadSize = maxUploadSize;
    this.restRequest = restRequest;
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

  /**
   * @return The {@link RestRequest} that triggered this put operation.
   */
  public RestRequest getRestRequest() {
    return restRequest;
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
    return chunkUpload == options.chunkUpload && maxUploadSize == options.maxUploadSize && Objects.equals(restRequest,
        options.restRequest);
  }

  @Override
  public int hashCode() {
    return Objects.hash(chunkUpload, maxUploadSize, restRequest);
  }

  @Override
  public String toString() {
    return "PutBlobOptions{" + "chunkUpload=" + chunkUpload + ", maxUploadSize=" + maxUploadSize + ", restRequest="
        + restRequest + '}';
  }
}
