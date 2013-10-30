package com.github.ambry.network;

/**
 * RequestResponse Channel
 */
public interface RequestResponseChannel {

  public void sendResponse(Response response) throws InterruptedException;

  public Request receiveRequest() throws InterruptedException;

  public void sendRequest(Request request) throws InterruptedException;

  public void shutdown();
}
