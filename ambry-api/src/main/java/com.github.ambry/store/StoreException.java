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
package com.github.ambry.store;

public class StoreException extends Exception {
  private static final long serialVersionUID = 1;
  private final StoreErrorCodes error;

  public StoreException(String message, StoreErrorCodes error) {
    super(message);
    this.error = error;
  }

  public StoreException(String message, Throwable e, StoreErrorCodes error) {
    super(message, e);
    this.error = error;
  }

  public StoreException(Throwable e, StoreErrorCodes error) {
    super(e);
    this.error = error;
  }

  public StoreErrorCodes getErrorCode() {
    return this.error;
  }
}
