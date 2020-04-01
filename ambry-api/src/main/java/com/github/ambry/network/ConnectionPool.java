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
 * A Connection pool interface that pools a list of connections, does connection management
 * and connection cleanup. A checkoutConnection should be followed by a checkInConnection or
 * destroyConnection. The pool is also responsible to close and delete connections on shutdown.
 * This includes connection that are live and idle.
 */
public interface ConnectionPool {
  /**
   * Starts the connection pool.
   */
  public void start();

  /**
   * Shutsdown the connection pool. This also includes cleaning up all idle and active connections
   */
  public void shutdown();

  /**
   * Returns a connected channel that represents the give host and port. If no connection is available, this
   * method blocks for the timeout specified
   * @param host The remote host to which a connection is required
   * @param port The remote {@Port} to which a connection is required
   * @param timeout The time up to which to wait to get a connection
   * @return The connected channel that represents the given host and port.
   * @throws IOException
   * @throws InterruptedException
   */
  public ConnectedChannel checkOutConnection(String host, Port port, long timeout)
      throws IOException, InterruptedException, ConnectionPoolTimeoutException;

  /**
   * The connected channel that needs to be put back into the pool after a successful usage
   * @param connectedChannel The channel to check in
   */
  public void checkInConnection(ConnectedChannel connectedChannel);

  /**
   * The connected channel that needs to be destroyed/disconnected after an error
   * @param connectedChannel The channel to destroy/disconnect
   */
  public void destroyConnection(ConnectedChannel connectedChannel);
}
