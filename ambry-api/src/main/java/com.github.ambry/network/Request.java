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

import java.io.InputStream;


/**
 * Simple request
 */
public interface Request {
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
}
