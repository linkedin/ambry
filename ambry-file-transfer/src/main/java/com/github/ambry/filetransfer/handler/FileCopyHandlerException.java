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
package com.github.ambry.filetransfer.handler;

/**
 * Custom exception class for FileCopyHandler related errors.
 * Extends RuntimeException to allow unchecked exception handling.
 * Includes specific error codes for different failure scenarios.
 */
public class FileCopyHandlerException extends RuntimeException {

  // Ensures proper serialization across JVM versions
  private static final long serialVersionUID = 1L;

  // Stores the specific error code associated with this exception
  private final FileCopyHandlerErrorCode error;

  /**
   * Creates a new FileCopyException with a message and error code.
   * @param s The error message describing what went wrong
   * @param error The specific error code categorizing the failure
   */
  public FileCopyHandlerException(String s, FileCopyHandlerErrorCode error) {
    super(s);
    this.error = error;
  }

  /**
   * Creates a new FileCopyException with a message, error code, and cause.
   * @param s The error message describing what went wrong
   * @param error The specific error code categorizing the failure
   * @param throwable The underlying cause of this exception
   */
  public FileCopyHandlerException(String s, Throwable throwable, FileCopyHandlerErrorCode error) {
    super(s, throwable);
    this.error = error;
  }

  /**
   * Returns the specific error code associated with this exception.
   * @return The error code associated with this exception
   */
  public FileCopyHandlerErrorCode getErrorCode() {
    return error;
  }

  /**
   * Enumeration of possible FileCopyHandler error codes.
   */
  public enum FileCopyHandlerErrorCode {
    /**
     * Indicates that the {@link FileCopyHandler} is not in running state when an operation was attempted.
     */
    FileCopyHandlerRunningFailure,
    /**
     * Indicates that FileCopyHandler encountered an error while writing data to disk.
     */
    FileCopyHandlerWriteToDiskError,
    /**
     * Indicates that FileCopyHandler encountered an error while making GetMetadata Api request.
     */
    FileCopyHandlerGetMetadataApiError,
    /**
     * Indicates that FileCopyHandler encountered an error while making GetChunkData Api request.
     */
    FileCopyHandlerGetChunkDataApiError,
    /**
     * Indicates that FiceCopyHandler encountered an unknown error.
     */
    UnknownError
  }
}
