/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.utils;

import java.io.Closeable;
import java.nio.ByteBuffer;


public interface IFilter extends Closeable {
  /**
   * Add the key to the filter
   * @param key The key that needs to be added to the filter
   */
  public abstract void add(ByteBuffer key);

  /**
   * Determines if the given key is present. This is a non deterministic
   * api. If the key is present, it would return true. If the key is not
   * present, it may or may not return true.
   * @param key The key to do the presence check
   * @return True, if key is present. False, if key may or may not be present.
   */
  public abstract boolean isPresent(ByteBuffer key);

  /**
   * Clears the filter
   */
  public abstract void clear();
}