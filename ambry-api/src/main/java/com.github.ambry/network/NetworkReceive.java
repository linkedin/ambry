package com.github.ambry.network;

import com.github.ambry.utils.Time;


/**
 * Contains the information about the initial send and the response bytes from the destination
 */
public class NetworkReceive {
  /**
   * The connection Id which is used for this request
   */
  private final String connectionId;
  /**
   * The bytes received from the destination
   */
  private final BoundedByteBufferReceive receivedBytes;

  /**
   * The start time of when the receive started
   */
  private final long receiveStartTimeInMs;

  public NetworkReceive(String connectionId, BoundedByteBufferReceive receivedBytes, Time time) {
    this.connectionId = connectionId;
    this.receivedBytes = receivedBytes;
    this.receiveStartTimeInMs = time.milliseconds();
  }

  public String getConnectionId() {
    return connectionId;
  }

  public BoundedByteBufferReceive getReceivedBytes() {
    return receivedBytes;
  }

  public long getReceiveStartTimeInMs() {
    return receiveStartTimeInMs;
  }
}
