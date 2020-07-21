/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
 *
 */

package com.github.ambry.utils;

import java.util.function.Supplier;


/**
 * A helper class to ensure exactly one instance of an object of type T per {@link Singleton} instance is created.
 * @param <T> the type of the singleton object
 */
public class Singleton<T> {
  private final Supplier<T> supplier;
  private volatile T instance = null;

  /**
   * @param supplier a constructor for the singleton instance. This will be called exactly once.
   */
  public Singleton(Supplier<T> supplier) {
    this.supplier = supplier;
  }

  /**
   * @return the single lazily-instantiated object.
   */
  public T get() {
    // Based on guidance from https://en.wikipedia.org/wiki/Double-checked_locking#Usage_in_Java
    if (instance == null) {
      synchronized (this) {
        if (instance == null) {
          instance = supplier.get();
        }
      }
    }
    return instance;
  }
}
