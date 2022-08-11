/*
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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
 * Disk Health Status Levels
 * When determining the health of the disk we can use these levels to determine what went wrong with the healthcheck
 */
public enum DiskHealthStatus {
  /**
   * The Disk is healthy after performing the disk healthcheck
   */
  HEALTHY,
  /**
   * The Disk's mountpath isn't accessible
   */
  MOUNT_NOT_ACCESSIBLE,
  /**
   * The Disk's healthcheck encountered an exception when creating a file or directory to disk
   */
  CREATE_EXCEPTION,
  /**
   * The Disk's healthcheck encountered a timeout when writing to disk
   */
  WRITE_TIMEOUT,
  /**
   * The Disk's healthcheck encountered any other exception when writing to disk
   */
  WRITE_EXCEPTION,
  /**
   * The Disk's healthcheck encountered a timeout when reading from disk
   */
  READ_TIMEOUT,
  /**
   * The Disk's healthcheck encountered any other exception when reading from disk
   */
  READ_EXCEPTION,
  /**
   * The Disk's healthcheck reads a different value than what was expected
   */
  READ_DIFFERENT,
  /**
   * The Disk's healthcheck encountered an io exception when deleting a file from disk
   */
  DELETE_IO_EXCEPTION,
}

