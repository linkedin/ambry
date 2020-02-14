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

import io.netty.buffer.ByteBuf;
import java.io.InputStream;


/**
 * Simple request
 */
public interface NetworkRequest {
  /**
   * The request as an input stream is returned to the caller
   * @return The inputstream that represents the request
   */
  InputStream getInputStream();

  /**
   * Gets the start time in ms when this request started
   * @return The start time in ms when the request started
   */
  long getStartTimeInMs();

  /**
   * Release any resource this request is holding. By default it returns false so this method can be compatible
   * with {@link ByteBuf#release()}
   * @return {@code true} if and only if the reference count became {@code 0} and this object has been deallocated
   */
  default boolean release() {
    return false;
  }
}
