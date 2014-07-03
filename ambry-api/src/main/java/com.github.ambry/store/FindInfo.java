package com.github.ambry.store;

import java.util.List;


/**
 * Contains the information from the store after a find operation. It consist of message info entries and
 * new find token that can be used for subsequent searches.
 */
public class FindInfo {
  private final List<MessageInfo> messageEntries;
  private final FindToken findToken;

  public FindInfo(List<MessageInfo> messageEntries, FindToken findToken) {
    this.messageEntries = messageEntries;
    this.findToken = findToken;
  }

  public List<MessageInfo> getMessageEntries() {
    return messageEntries;
  }

  public FindToken getFindToken() {
    return findToken;
  }
}
