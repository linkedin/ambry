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

import java.io.IOException;
import java.nio.ByteBuffer;


/**
 * Provides a read interface into the underlying storage layer
 */
public interface Read {

  /**
   * Read from the underlying store(file) into the buffer starting at the given position in the store. Reads
   * exactly {@code buffer.remaining()} amount of data or throws an exception.
   * @param buffer The buffer into which the read needs to write to
   * @param position The position to start the read from
   * @throws IOException
   */
  void readInto(ByteBuffer buffer, long position) throws IOException;
}
