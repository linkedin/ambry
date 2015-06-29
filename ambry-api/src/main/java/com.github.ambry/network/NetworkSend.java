package com.github.ambry.network;

/**
 * Contains the destination information and bytes to send
 */
public interface NetworkSend {

  public String getConnectionId();

  public Send getPayload();

  public void onSendComplete();

  public long getSendStartTimeInMs();
}
