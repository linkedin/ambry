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
  void start()
      throws IOException, InterruptedException;

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
