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
 * RequestResponse Channel used by the network layer to queue new requests and
 * send responses over the network from the channel. This is used by the server
 * and hence cannot be used to receive response or open a connection as they would
 * be client related operations.
 */
public interface RequestResponseChannel {

  /**
   * Queue's the response into the channel for the network server to pick up
   * @param payloadToSend The payload to be sent over the network
   * @param originalRequest The original request this response belongs to
   * @param metrics The set of metrics tracked at the network layer
   * @throws InterruptedException
   */
  void sendResponse(Send payloadToSend, Request originalRequest, ServerNetworkResponseMetrics metrics)
      throws InterruptedException;

  /**
   * Receives the request from the channel
   * @return A bundle of requests that were queued by the network layer into the channel. The bundle may include 0 or 1
   *         requests to handle, and 0 or more requests that should be dropped.
   * @throws InterruptedException if the thread was interrupted.
   */
  RequestBundle receiveRequest() throws InterruptedException;


  /**
   * Sends a request over the network. The request gets queued by the channel.
   * @param request The request to be queued by the channel
   * @throws InterruptedException
   */
  void sendRequest(Request request) throws InterruptedException;

  /**
   * Closes the connection on which the original request came
   * @param request The request whose connection needs to be closed
   * @throws InterruptedException
   */
  void closeConnection(Request request) throws InterruptedException;
}
