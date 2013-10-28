package com.github.ambry;

import java.io.IOException;

/**
 * Basic network server used to accept / send request and responses
 */
public interface NetworkServer {
  void start() throws IOException, InterruptedException;

  void shutdown();

  RequestResponseChannel getRequestResponseChannel();
}
