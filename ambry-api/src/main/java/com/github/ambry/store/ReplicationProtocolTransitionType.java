/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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

package com.github.ambry.store;

/**
 * The types of hydration protocol transitions that can happen.
 * The possible options are :- Blob based and File copy based hydration
 */
public enum ReplicationProtocolTransitionType {
  /**
   * Pre restart protocol: NA
   * Bootstrap status: NA
   * Post restart protocol: Blob based
   */
  NEW_PARTITION_TO_BLOB_BASED_HYDRATION,

  /**
   * Pre restart protocol: NA
   * Bootstrap status: NA
   * Post restart protocol: File based
   */
  NEW_PARTITION_TO_FILE_BASED_HYDRATION,

  /**
   * Pre restart protocol: Blob based
   * Bootstrap status: Complete
   * Post restart protocol: File based
   */
  BLOB_BASED_HYDRATION_COMPLETE_TO_FILE_BASED_HYDRATION,

  /**
   * Pre restart protocol: File based
   * Bootstrap status: Complete
   * Post restart protocol: Blob based
   */
  FILE_BASED_HYDRATION_COMPLETE_TO_BLOB_BASED_HYDRATION,

  /**
   * Pre restart protocol: Blob based
   * Bootstrap status: Complete
   * Post restart protocol: Blob based
   */
  BLOB_BASED_HYDRATION_COMPLETE_TO_BLOB_BASED_HYDRATION,

  /**
   * Pre restart protocol: File based
   * Bootstrap status: Complete
   * Post restart protocol: File based
   */
  FILE_BASED_HYDRATION_COMPLETE_TO_FILE_BASED_HYDRATION,

  /**
   * Pre restart protocol: Blob based
   * Bootstrap status: InComplete
   * Post restart protocol: Blob based
   */
  BLOB_BASED_HYDRATION_INCOMPLETE_TO_BLOB_BASED_HYDRATION,

  /**
   * Pre restart protocol: File based
   * Bootstrap status: InComplete
   * Post restart protocol: File based
   */
  FILE_BASED_HYDRATION_INCOMPLETE_TO_FILE_BASED_HYDRATION,

  /**
   * Pre restart protocol: Blob based
   * Bootstrap status: InComplete
   * Post restart protocol: File based
   */
  BLOB_BASED_HYDRATION_INCOMPLETE_TO_FILE_BASED_HYDRATION,

  /**
   * Pre restart protocol: File based
   * Bootstrap status: InComplete
   * Post restart protocol: Blob based
   */
  FILE_BASED_HYDRATION_INCOMPLETE_TO_BLOB_BASED_HYDRATION
}
