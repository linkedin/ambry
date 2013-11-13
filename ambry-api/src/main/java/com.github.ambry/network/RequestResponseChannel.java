package com.github.ambry.network;

/**
 * RequestResponse Channel used by the network layer to queue new requests and
 * send responses over the network from the channel
 */
public interface RequestResponseChannel {

  /**
   * Queue's the response into the channel for the network server to pick up
   * @param payloadToSend The payload to be sent over the network
   * @param originalRequest The original request this response belongs to
   * @throws InterruptedException
   */
  public void sendResponse(Send payloadToSend, Request originalRequest) throws InterruptedException;

  /**
   * Receives the request from the channel
   * @return The request that was queued by the network layer into the channel
   * @throws InterruptedException
   */
  public Request receiveRequest() throws InterruptedException;

  /**
   * Sends a request over the network. The request gets queued by the channel.
   * @param request The request to be queued by the channel
   * @throws InterruptedException
   */
  public void sendRequest(Request request) throws InterruptedException;

  /**
   * Shuts down the request response channel
   */
  public void shutdown();
}
