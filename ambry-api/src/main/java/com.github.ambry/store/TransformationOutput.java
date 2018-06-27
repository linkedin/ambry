/*
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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
 * A class that holds the result of a transformation (by the {@link Transformer}).
 */
public class TransformationOutput {
  Exception exception;
  Message msg;

  /**
   * Instantiate an instance of this class
   * @param exception the exception encountered with the associated transformation, if any.
   * @param msg the resulting message after the transformation. Should be null if exception is non-null. May be null
   *            even if exception is null, which signifies that the transformation resulted in the message being discarded.
   */
  public TransformationOutput(Exception exception, Message msg) {
    this.exception = exception;
    this.msg = msg;
  }

  /**
   * @return the {@link Exception}, if any, that the transformation encountered. May be null.
   */
  public Exception getException() {
    return exception;
  }

  /**
   * @return the {@link Message}, if any, that the transformation resulted in. May be null.
   */
  public Message getMsg() {
    return msg;
  }
}
