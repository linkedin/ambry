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
package com.github.ambry.dependency;

/**
 * DependencyException is just a wrapper of an exception thrown by the application at runtime.
 * This exception is thrown when:
 * - in provideSupplier(interface, supplier) when supplier.get() throws exception.
 * - in provideFactory(interface, factory) when factory.get() throws exception.
 * - in get() when calling constructor of the DependencyProvider annotated class throws exception.
 * - calling a subscriber/listener throws an exception.
 * <p>
 * This exception extends RuntimeException instead of checked Exception for the following reason.
 * When this exception is thrown, it means there's a code issue that throwing unintended exception.
 * Caller can't do anything and caller can't fix the problem.  If this is a checked exception,
 * "throws DependencyException" signature will appear in many places across the application since caller can't handle
 * the exception.
 */
public class DependencyException extends RuntimeException {
  /**
   * The key passed to get(key).  It can be a string or a class.
   */
  private final Object key;

  /**
   * Get the key that was trying to create the instance.
   * @return The key of the get() or tryGet() call.
   */
  public Object getKey() {
    return key;
  }

  /**
   * Create new instance with a specific message.
   * @param key The key passed to get(key) or tryGet(key).
   * @param message The error message.
   */
  public DependencyException(Object key, String message) {
    super(message);
    this.key = key;
  }

  /**
   * Create new instance with specific error message and cause.
   * @param key The key passed to get(key) or tryGet(key).
   * @param message The error message.
   * @param cause Cause/inner exception.
   */
  public DependencyException(Object key, String message, Exception cause) {
    super(message, cause);
    this.key = key;
  }
}
