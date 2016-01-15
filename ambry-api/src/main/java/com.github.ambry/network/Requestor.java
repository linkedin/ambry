package com.github.ambry.network;

import java.util.List;


/**
 * This is an interface that needs to be implemented by any component that needs to use the RequestResponseHandler.
 * The latter will be initialized with a Requestor and will use this API to poll for NetworkSends and inform about
 * any connections, disconnections, completed sends and receives.
 */
public interface Requestor {
  //@todo
  public List<NetworkSend> poll();

  //@todo
  public void onResponse(List<String> connected, List<String> disconnected, List<NetworkSend> completedSends,
      List<NetworkReceive> completedReceives);

  //@todo
  public void onException(Exception e);
}
