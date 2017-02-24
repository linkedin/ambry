package com.github.ambry.store;

/**
 * Hold all relevant information of new writes during a scan so they can be buffered and processed later.
 */
public class StatsEvent {
  private MessageInfo messageInfo;
  private IndexValue putIndexValue;
  private Offset offset;
  private long eventTimeInMs;
  private boolean isDelete;

  StatsEvent(MessageInfo messageInfo, Offset offset, IndexValue putIndexValue,
      long eventTimeInMs, boolean isDelete) {
    this.messageInfo = messageInfo;
    this.putIndexValue = putIndexValue;
    this.offset = offset;
    this.eventTimeInMs = eventTimeInMs;
    this.isDelete =  isDelete;
  }

  StatsEvent(MessageInfo messageInfo, Offset offset, long eventTimeInMs) {
    this(messageInfo, offset, null, eventTimeInMs, false);
  }

  boolean isDelete() {
    return isDelete;
  }

  MessageInfo getMessageInfo() {
    return messageInfo;
  }

  IndexValue getPutIndexValue() {
    return putIndexValue;
  }

  Offset getOffset() {
    return offset;
  }

  long getEventTime() {
    return eventTimeInMs;
  }
}
