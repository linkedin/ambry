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

import com.github.ambry.protocol.GetOption;

import static com.github.ambry.router.GetBlobOptions.*;


/**
 * A builder for {@link GetBlobOptions} objects. The following options will be assigned by default:
 * {@link GetBlobOptions.OperationType#All}, {@link GetOption#None} and no {@link ByteRange}.
 */
public class GetBlobOptionsBuilder {
  private GetBlobOptions.OperationType operationType = GetBlobOptions.OperationType.All;
  private GetOption getOption = GetOption.None;
  private ByteRange range = null;
  private boolean rawMode = false;
  private int blobSegmentIdx = NO_BLOB_SEGMENT_IDX_SPECIFIED;

  /**
   * @param operationType the {@link GetBlobOptions.OperationType} for this request.
   * @return this builder
   */
  public GetBlobOptionsBuilder operationType(GetBlobOptions.OperationType operationType) {
    this.operationType = operationType;
    return this;
  }

  /**
   * @param getOption the {@link GetOption} associated with the request.
   * @return this builder
   */
  public GetBlobOptionsBuilder getOption(GetOption getOption) {
    this.getOption = getOption;
    return this;
  }

  /**
   * @param range a {@link ByteRange} for this get request. This can be null, if the entire blob is desired.
   * @return this builder
   */
  public GetBlobOptionsBuilder range(ByteRange range) {
    this.range = range;
    return this;
  }

  /**
   * @param rawMode the raw mode flag for this get request.
   * If rawMode is true, the returned {@link GetBlobResult} will contain the raw (unserialized) blob payload in the
   * data channel and null blobInfo.  This option cannot be used in conjunction with a byte range.
   * @return this builder
   */
  public GetBlobOptionsBuilder rawMode(boolean rawMode) {
    this.rawMode = rawMode;
    return this;
  }

  /**
   * @param blobSegmentIdx blob segment index of a metadata blob that one wants to get.
   * @return this builder
   */
  public GetBlobOptionsBuilder blobSegment(int blobSegmentIdx) {
    this.blobSegmentIdx = blobSegmentIdx;
    return this;
  }

  /**
   * @return the {@link GetBlobOptions} built.
   */
  public GetBlobOptions build() {
    return new GetBlobOptions(operationType, getOption, range, rawMode, blobSegmentIdx);
  }
}
