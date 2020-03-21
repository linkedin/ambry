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


/**
 * Basic network server used to accept / send request and responses
 */
public interface NetworkServer {

  /**
   * Starts the network server
   * @throws IOException
   * @throws InterruptedException
   */
  void start() throws IOException, InterruptedException;

  /**
   * Shuts down the network server
   */
  void shutdown();

  /**
   * Provides the request response channel used by this network server
   * @return The RequestResponseChannel used by the network layer to queue requests and response
   */
  RequestResponseChannel getRequestResponseChannel();
}
