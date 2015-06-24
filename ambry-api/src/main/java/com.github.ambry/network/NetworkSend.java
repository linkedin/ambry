package com.github.ambry.network;

/**
 * Contains the destination information and bytes to send
 */
public interface NetworkSend {

  String getConnectionId();

  Send getPayload();

  void onSendComplete();

}
