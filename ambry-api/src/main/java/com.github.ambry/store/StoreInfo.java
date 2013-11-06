package com.github.ambry.store;

import java.util.List;

/**
 * The info returned by the store on a get call
 */
public class StoreInfo {
  private final MessageReadSet readSet;
  private final List<MessageInfo> messageSetInfo;

  public StoreInfo(MessageReadSet readSet, List<MessageInfo> messageSetInfo) {
    this.readSet = readSet;
    this.messageSetInfo = messageSetInfo;
  }

  public MessageReadSet getMessageReadSet() {
    return readSet;
  }

  public List<MessageInfo> getMessageReadSetInfo() {
    return messageSetInfo;
  }
}
