package com.github.ambry.network;

import java.util.List;


/**
 * The Response object from a ConnectionManager poll.
 */
public class ConnectionManagerPollResponse {
  private List<String> connectedIds;
  private List<String> disconnectedIds;
  private List<NetworkSend> completedSends;
  private List<NetworkReceive> completedReceives;

  public ConnectionManagerPollResponse(List<String> connectedIds, List<String> disconnectedIds,
      List<NetworkSend> completedSends, List<NetworkReceive> completedReceives) {
    this.connectedIds = connectedIds;
    this.disconnectedIds = disconnectedIds;
    this.completedSends = completedSends;
    this.completedReceives = completedReceives;
  }

  public List<String> getConnectedIds() {
    return connectedIds;
  }

  public List<String> getDisconnectedIds() {
    return disconnectedIds;
  }

  public List<NetworkSend> getCompletedSends() {
    return completedSends;
  }

  public List<NetworkReceive> getCompletedReceives() {
    return completedReceives;
  }
}
