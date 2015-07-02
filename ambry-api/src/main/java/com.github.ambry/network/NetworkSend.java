package com.github.ambry.network;

import com.github.ambry.utils.Time;


/**
 * Contains the destination information and bytes to send
 */
public class NetworkSend {
  /**
   * The Id of the connection to which the bytes need to be sent
   */
  private final String connectionId;

  /**
   * The bytes to be sent over the connection
   */
  private final Send payload;

  /**
   * The start time of this send
   */
  private final long sendStartTimeInMs;

  public NetworkSend(String connectionId, Send payload, Time time) {
    this.connectionId = connectionId;
    this.payload = payload;
    this.sendStartTimeInMs = time.milliseconds();
  }

  public long getSendStartTimeInMs() {
    return sendStartTimeInMs;
  }

  public String getConnectionId() {
    return connectionId;
  }

  public Send getPayload() {
    return payload;
  }
}
