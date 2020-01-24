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
package com.github.ambry.rest;

/**
 * Represents a NIO (non blocking I/O) server. Responsible for all network communication i.e. receiving requests
 * and sending responses and supports doing these operations in a non-blocking manner.
 * <p/>
 * A typical implementation of a NioServer will handle incoming connections from clients, decode the REST protocol
 * (usually HTTP), convert them to generic objects that the {@link RestRequestService} can understand, instantiate a
 * NioServer specific implementation of {@link RestResponseChannel} to return responses to clients and push them down
 * the pipeline. The NioServer should be non-blocking while receiving requests and sending responses. It should also
 * provide methods that the downstream can use to control the flow of data towards the {@link RestRequestService}.
 */
public interface NioServer {

  /**
   * Do startup tasks for the NioServer. When the function returns, startup is FULLY complete.
   * @throws InstantiationException if the NioServer is unable to start.
   */
  public void start() throws InstantiationException;

  /**
   * Do shutdown tasks for the NioServer. When the function returns, shutdown is FULLY complete.
   */
  public void shutdown();
}
