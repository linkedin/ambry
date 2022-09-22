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
 * Dependency.get() throws this exception if the dependency is not found.
 * DependencyNotFoundException is arguably whether it should be checked or runtime exception.
 * If a dependency is required, use Dependency.get().  If it is optional, use Dependency.tryGet().
 * Patterns of using tryGet().
 * <pre>{@code
 * MyService myDependent = Dependency.tryGet(MyService.class);
 * if (myDependent == null) {
 *   throw new MyException("MyService not available."0;
 * }
 * myDependent.doSomething();
 * }</pre>
 * It is simpler to use the code below, if you don't mind the DependencyNotFoundException.
 * <pre>{@code Dependency.get(MyService.class).doSomething();}</pre>
 * The difference above is one throws MyException() while other throws DependencyNotFoundException().
 * <p>
 * When caller can't do anything if the dependency is not available, use get() and let this exception throw.
 * Throwing DependencyNotFoundException is just like throwing IllegalArgumentException.
 * For this reason, this exception should be a runtime exception.
 */
public class DependencyNotFoundException extends RuntimeException {
  /**
   * The key passed to get().
   */
  private final Object key;

  /**
   * Get the key passed to get(key).
   * @return The key passed to get(key).
   */
  public Object getKey() {
    return key;
  }

  /**
   * Create new instance of this exception a specific message and key.
   * @param key The key passed to get(key).
   * @param message The error message.
   */
  public DependencyNotFoundException(Object key, String message) {
    super(message);
    this.key = key;
  }
}
