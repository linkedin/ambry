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
 * A channel that is connected to a specific remote host and port.
 * The connected channel can be used to send a request and receive a response.
 * All connection management is done by the connection pool
 */
public interface ConnectedChannel {

  /**
   * Connect remote host.
   * @throws IOException
   */
  void connect() throws IOException;

  /**
   * Disconnect remote host.
   * @throws IOException
   */
  void disconnect() throws IOException;

  /**
   * Sends the request to a remote host
   * @param request The request that needs to be sent
   * @throws IOException
   */
  void send(Send request) throws IOException;

  /**
   * Receives a response from the remote host
   * @return The input stream that can be used to consume data from the remote host
   * @throws IOException
   */
  ChannelOutput receive() throws IOException;

  /**
   * Gets the remote host that this channel is connected to
   * @return The remote host the channel is connected to
   */
  String getRemoteHost();

  /**
   * Gets the remote port that this channel is connected to
   * @return The remote port that this channel is connected to
   */
  int getRemotePort();
}
