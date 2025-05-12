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

package com.github.ambry.store;

/**
 * Custom exception class for FileStore-related errors.
 * Extends RuntimeException to allow unchecked exception handling.
 * Includes specific error codes for different failure scenarios.
 */
public class FileStoreException extends RuntimeException {

  // Ensures proper serialization across JVM versions
  private static final long serialVersionUID = 1L;

  // Stores the specific error code associated with this exception
  private final FileStoreErrorCode error;

  /**
   * Creates a new FileStoreException with a message and error code.
   *
   * @param s The error message describing what went wrong
   * @param error The specific error code categorizing the failure
   */
  public FileStoreException(String s, FileStoreErrorCode error) {
    super(s);
    this.error = error;
  }

  /**
   * Creates a new FileStoreException with a message, error code, and cause.
   *
   * @param s The error message describing what went wrong
   * @param error The specific error code categorizing the failure
   * @param throwable The underlying cause of this exception
   */
  public FileStoreException(String s, Throwable throwable, FileStoreErrorCode error) {
    super(s, throwable);
    this.error = error;
  }

  /**
   * Enumeration of possible FileStore error codes.
   * Each code represents a specific category of failure.
   */
  public enum FileStoreErrorCode {
    /**
     * Indicates that the FileStore service is not in running state
     * when an operation was attempted.
     */
    FileStoreRunningFailure,
    /**
     * Indicates that the FileStore service encountered an error during write
     */
    FileStoreWriteError,
    /**
     * Indicates that the FileStore service encountered an error during read
     */
    FileStoreReadError,
    /**
     * Indicates that the FileStore service encountered an error during files move
     */
    FileStoreMoveFilesError,
    /**
     * Indicates that the FileStore service encountered an error during cleanup
     */
    FileStoreFileFailedCleanUp,
    /**
     * Indicates that the File allocation by disk space allocator has failed
     */
    FileStoreFileAllocationFailed,
    /**
     /**
     * Indicates that the FileStore service encountered an error during delete
     */
    UnknownError
  }
}
