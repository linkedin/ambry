/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud;

/**
 * Exception class indicating an error moving data between Ambry and cloud storage.
 */
public class CloudStorageException extends Exception {
  private static final long serialVersionUID = 1;

  public CloudStorageException(String message) {
    super(message);
  }

  public CloudStorageException(String message, Throwable e) {
    super(message, e);
  }

  public CloudStorageException(Throwable e) {
    super(e);
  }
}
