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

/**
 * Simple response
 */
public interface NetworkResponse {

  /**
   * Provides the send object that can be sent over the network
   * @return The send object that is part of this response
   */
  Send getPayload();

  /**
   * The original request object that this response maps to
   * @return The request object that maps to this response
   */
  NetworkRequest getRequest();
}
