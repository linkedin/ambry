package com.github.ambry.network;

import com.github.ambry.utils.Time;


/**
 * Contains the destination information and bytes to send
 */
public class MocNetworkSend implements NetworkSend {
  /**
   * The Id of the connection to which the bytes need to be sent
   */
  private final String connectionId;

  /**
   * The bytes to be sent over the connection
   */
  private final BoundedByteBufferSend bytesToSend;

  /**
   * The start time of this send
   */
  private final long sendStartTimeInMs;

  public MocNetworkSend(String connectionId, BoundedByteBufferSend bytesToSend, Time time) {
    this.connectionId = connectionId;
    this.bytesToSend = bytesToSend;
    this.sendStartTimeInMs = time.milliseconds();
  }

  @Override
  public long getSendStartTimeInMs() {
    return sendStartTimeInMs;
  }

  public String getConnectionId() {
    return connectionId;
  }

  @Override
  public Send getPayload() {
    return bytesToSend;
  }

  @Override
  public void onSendComplete() {

  }
}
