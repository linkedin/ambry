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
package com.github.ambry.router;

/**
 * Exceptions thrown by a {@link Router}. All exceptions are accompanied by a {@link RouterErrorCode}.
 */

public class RouterException extends Exception {
  private final RouterErrorCode errorCode;

  public RouterException(String message, RouterErrorCode errorCode) {
    super(message + " Error: " + errorCode);
    this.errorCode = errorCode;
  }

  public RouterException(String message, Throwable e, RouterErrorCode errorCode) {
    super(message + " Error: " + errorCode, e);
    this.errorCode = errorCode;
  }

  public RouterException(Throwable e, RouterErrorCode errorCode) {
    super(e);
    this.errorCode = errorCode;
  }

  public RouterErrorCode getErrorCode() {
    return this.errorCode;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    RouterException that = (RouterException) o;
    return this.errorCode == that.errorCode && this.getCause().equals(that.getCause());
  }
}
