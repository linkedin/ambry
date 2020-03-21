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
package com.github.ambry.utils;

import java.io.Closeable;
import java.io.DataOutput;
import java.io.IOException;


public interface IBitSet extends Closeable {
  public long capacity();

  /**
   * Returns true or false for the specified bit index. The index should be
   * less than the capacity.
   */
  public boolean get(long index);

  /**
   * Sets the bit at the specified index. The index should be less than the
   * capacity.
   */
  public void set(long index);

  /**
   * clears the bit. The index should be less than the capacity.
   */
  public void clear(long index);

  public void serialize(DataOutput out) throws IOException;

  public void clear();
}
