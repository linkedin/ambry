/*
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

import com.github.ambry.commons.Callback;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.rest.RestRequest;
import java.util.Objects;


/**
 * Represents any options associated with a getBlob request when making a
 * {@link Router#getBlob(String, GetBlobOptions, Callback)} call.
 */
public class GetBlobOptions {

  private final OperationType operationType;
  private final GetOption getOption;
  private final ByteRange range;
  private final boolean resolveRangeOnEmptyBlob;
  // Flag indicating whether to return the raw blob payload without deserialization.
  private final boolean rawMode;
  private final int blobSegmentIdx;
  private final RestRequest restRequest;
  public static final int NO_BLOB_SEGMENT_IDX_SPECIFIED = -1;

  /**
   * Construct a {@link GetBlobOptions} object that represents any options associated with a getBlob request.
   * @param operationType the {@link OperationType} for this request. This must be non-null.
   * @param getOption the {@link GetOption} associated with the request.
   * @param range a {@link ByteRange} for this get request. This can be null, if the entire blob is desired.
   * @param resolveRangeOnEmptyBlob {@code true} to indicate that the router should a successful response for a range
   *                                request against an empty (0 byte) blob instead of returning a
   *                                {@link RouterErrorCode#RangeNotSatisfiable} error.
   * @param rawMode a system flag indicating that the raw bytes should be returned.
   * @param blobSegmentIdx if not NO_BLOB_SEGMENT_IDX_SPECIFIED, the blob segment requested to be returned (only
   *                       relevant for metadata blobs)
   * @param restRequest the {@link RestRequest} that triggered this get operation.
   */
  GetBlobOptions(OperationType operationType, GetOption getOption, ByteRange range, boolean resolveRangeOnEmptyBlob,
      boolean rawMode, int blobSegmentIdx, RestRequest restRequest) {
    if (operationType == null || getOption == null) {
      throw new IllegalArgumentException("operationType and getOption must be defined");
    }
    if (rawMode && range != null) {
      throw new IllegalArgumentException("Raw mode and range cannot be used together");
    }
    if (rawMode && operationType != OperationType.All) {
      throw new IllegalArgumentException("Raw mode can be used only with OperationType.ALL");
    }
    this.operationType = operationType;
    this.getOption = getOption;
    this.range = range;
    this.resolveRangeOnEmptyBlob = resolveRangeOnEmptyBlob;
    this.rawMode = rawMode;
    this.blobSegmentIdx = blobSegmentIdx;
    this.restRequest = restRequest;
  }

  /**
   * @return The value of the rawMode flag.
   */
  public boolean isRawMode() {
    return rawMode;
  }

  /**
   * @return the {@link OperationType} for the request.
   */
  public OperationType getOperationType() {
    return operationType;
  }

  /**
   * @return the {@link GetOption} associated with the request.
   */
  public GetOption getGetOption() {
    return getOption;
  }

  /**
   * @return the {@link ByteRange}, if set, or {@code null} if no range was set.
   */
  public ByteRange getRange() {
    return range;
  }

  /**
   * @return {@code true} if the router should a successful response for a range request against an empty (0 byte) blob
   *         instead of returning a {@link RouterErrorCode#RangeNotSatisfiable} error.
   */
  public boolean resolveRangeOnEmptyBlob() {
    return resolveRangeOnEmptyBlob;
  }

  /**
   * @return the blob segment index
   */
  public int getBlobSegmentIdx() {
    return blobSegmentIdx;
  }

  /**
   * @return whether a blob segment index has been specified or not
   */
  public boolean hasBlobSegmentIdx() {
    return blobSegmentIdx != NO_BLOB_SEGMENT_IDX_SPECIFIED;
  }

  /**
   * @return The {@link RestRequest} that triggered this get operation.
   */
  public RestRequest getRestRequest() {
    return restRequest;
  }

  @Override
  public String toString() {
    return "GetBlobOptions{operationType=" + operationType + ", getOption=" + getOption + ", range=" + range
        + ", rawMode=" + rawMode + ", blobSegmentIdx=" + blobSegmentIdx + ", restRequest=" + restRequest + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GetBlobOptions that = (GetBlobOptions) o;
    return resolveRangeOnEmptyBlob == that.resolveRangeOnEmptyBlob && rawMode == that.rawMode
        && blobSegmentIdx == that.blobSegmentIdx && operationType == that.operationType && getOption == that.getOption
        && Objects.equals(range, that.range) && Objects.equals(restRequest, that.restRequest);
  }

  @Override
  public int hashCode() {
    return Objects.hash(operationType, getOption, range, resolveRangeOnEmptyBlob, rawMode, blobSegmentIdx, restRequest);
  }

  /**
   * Describes the type of getBlob operation to perform.
   */
  public enum OperationType {
    /**
     * Return blob info and blob data in the response.
     */
    All,

    /**
     * Return just the blob data in the response.
     */
    Data,

    /**
     * Return just the blob info in the response.
     */
    BlobInfo,

    /**
     *  Return all chunk IDs of a composite blob ID.
     */
    BlobChunkIds
  }
}
