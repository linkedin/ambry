package com.github.ambry.store;

import java.util.List;


/**
 * The info returned by the store on a get call
 */
public class StoreInfo {
  private final MessageReadSet readSet;
  private final List<MessageInfo> messageSetInfos;

  public StoreInfo(MessageReadSet readSet, List<MessageInfo> messageSetInfos) {
    this.readSet = readSet;
    this.messageSetInfos = messageSetInfos;
  }

  public MessageReadSet getMessageReadSet() {
    return readSet;
  }

  public List<MessageInfo> getMessageReadSetInfo() {
    return messageSetInfos;
  }
}
