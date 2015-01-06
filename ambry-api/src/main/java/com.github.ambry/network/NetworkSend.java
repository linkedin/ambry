package com.github.ambry.network;

import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;


/**
 * Contains the destination information and bytes to send
 */
public class NetworkSend {
  /**
   * The Id of the connection to which the bytes need to be sent
   */
  private final long connectionId;

  /**
   * The bytes to be sent over the connection
   */
  private final BoundedByteBufferSend bytesToSend;

  /**
   * The start time of this send
   */
  private final long sendStartTimeInNanos;

  public NetworkSend(long connectionId, BoundedByteBufferSend bytesToSend, Time time) {
    this.connectionId = connectionId;
    this.bytesToSend = bytesToSend;
    this.sendStartTimeInNanos = time.nanoseconds();
  }

  public long getSendStartTimeInNanos() {
    return sendStartTimeInNanos;
  }

  public long getConnectionId() {
    return connectionId;
  }

  public BoundedByteBufferSend getBytesToSend() {
    return bytesToSend;
  }
}
