/**
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
package com.github.ambry.network;

/**
 * Exception used by the connection pool to indicate that the operation timedout
 */
public class ConnectionPoolTimeoutException extends Exception {
  private static final long serialVersionUID = 1;

  public ConnectionPoolTimeoutException(String message) {
    super(message);
  }

  public ConnectionPoolTimeoutException(String message, Throwable e) {
    super(message, e);
  }

  public ConnectionPoolTimeoutException(Throwable e) {
    super(e);
  }
}
