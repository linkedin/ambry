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

import com.github.ambry.protocol.GetOptions;


/**
 * Represents any options associated with a getBlob request when making a
 * {@link Router#getBlob(String, GetBlobOptions, Callback)} call.
 */
public class GetBlobOptions {

  private final OperationType operationType;
  private final GetOptions getOptions;
  private final ByteRange range;

  /**
   * Construct a {@link GetBlobOptions} object with the default options: {@link OperationType#All},
   * {@link GetOptions#None} and no {@link ByteRange}.
   */
  public GetBlobOptions() {
    this(OperationType.All, GetOptions.None, null);
  }

  /**
   * Construct a {@link GetBlobOptions} object that represents any options associated with a getBlob request.
   * @param operationType the {@link OperationType} for this request. This must be non-null.
   * @param getOptions the {@link GetOptions} associated with the request.
   * @param range a {@link ByteRange} for this get request. This can be null, if the entire blob is desired.
   */
  public GetBlobOptions(OperationType operationType, GetOptions getOptions, ByteRange range) {
    if (operationType == null || getOptions == null) {
      throw new IllegalArgumentException("operationType and getOptions must be defined");
    }
    this.operationType = operationType;
    this.getOptions = getOptions;
    this.range = range;
  }

  /**
   * @return the {@link OperationType} for the request.
   */
  public OperationType getOperationType() {
    return operationType;
  }

  /**
   * @return the {@link GetOptions} associated with the request.
   */
  public GetOptions getGetOptions() {
    return getOptions;
  }

  /**
   * @return the {@link ByteRange}, if set, or {@code null} if no range was set.
   */
  public ByteRange getRange() {
    return range;
  }

  @Override
  public String toString() {
    return "GetBlobOptions{operationType=" + operationType + ", getOptions=" + getOptions + ", range=" + range + '}';
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

    if (operationType != that.operationType) {
      return false;
    }
    if (getOptions != that.getOptions) {
      return false;
    }
    return !(range != null ? !range.equals(that.range) : that.range != null);
  }

  @Override
  public int hashCode() {
    int result = operationType.hashCode();
    result = 31 * result + getOptions.hashCode();
    result = 31 * result + (range != null ? range.hashCode() : 0);
    return result;
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
    BlobInfo;
  }
}
