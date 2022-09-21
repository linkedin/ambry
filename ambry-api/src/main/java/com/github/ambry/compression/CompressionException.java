/**
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
package com.github.ambry.compression;

/**
 * Generic compression failure exception.
 */
public class CompressionException extends Exception {
  /**
   * Create new instance with a specific message.
   * @param message The error message.
   */
  public CompressionException(String message) {
    super(message);
  }

  /**
   * Create new instance with specific error message and cause.
   * @param message The error message.
   * @param cause Cause/inner exception.
   */
  public CompressionException(String message, Exception cause) {
    super(message, cause);
  }
}
