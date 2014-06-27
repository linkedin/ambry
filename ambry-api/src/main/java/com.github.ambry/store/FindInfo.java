package com.github.ambry.store;

import java.util.List;


/**
 * Contains the information from the store after a find operation. It consist of message info entries and
 * new find token that can be used for subsequent searches.
 */
public class FindInfo {
  private List<MessageInfo> messageEntries;
  private FindToken findToken;
  // Total bytes read so far by remote replica with respect to local store
  private long totalBytesRead;

  public FindInfo(List<MessageInfo> messageEntries, FindToken findToken, long totalBytesRead) {
    this.messageEntries = messageEntries;
    this.findToken = findToken;
    this.totalBytesRead = totalBytesRead;
  }

  public List<MessageInfo> getMessageEntries() {
    return messageEntries;
  }

  public FindToken getFindToken() {
    return findToken;
  }

  public long getBytesReadSoFar() {
    return this.totalBytesRead;
  }
}
