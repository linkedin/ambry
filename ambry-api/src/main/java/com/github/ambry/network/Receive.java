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

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;


/**
 * Used to receive data from the network channel. Any implementation of this interface
 * can be used to read data from the network
 */
public interface Receive {
  /**
   * Indicates if the read has been completed
   * @return true if read is complete, else false
   */
  boolean isReadComplete();

  /**
   * Reads some bytes from the provided channel
   * @param channel The channel to read from
   * @return Number of bytes read. Returns -1 if EOS is reached
   * @throws IOException
   */

  long readFrom(ReadableByteChannel channel) throws IOException;
}
