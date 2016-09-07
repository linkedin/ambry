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

public enum GetOperationType {
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

  /**
   * {@link GetOperationType#All}
   */
  public final static GetOperationType DEFAULT_TYPE = GetOperationType.All;

  /**
   * Either get the {@link GetOperationType} from the provided {@link GetBlobOptions},
   * or return {@link GetOperationType#DEFAULT_TYPE}
   * @param options if this is non-null and contains a {@link GetOperationType}, return the {@link GetOperationType}
   *                provided in these {@link GetBlobOptions}.
   * @return the {@link GetOperationType} from {@code options}, or {@link GetOperationType#DEFAULT_TYPE}
   *         ({@link GetOperationType#All} right now).
   */
  public static GetOperationType getTypeFromOptions(GetBlobOptions options) {
    return options != null && options.getGetOperationType() != null ? options.getGetOperationType() : DEFAULT_TYPE;
  }
}
