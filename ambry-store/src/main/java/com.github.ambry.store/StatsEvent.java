package com.github.ambry.store;

public class StatsEvent {
  private MessageInfo messageInfo;
  private IndexValue putIndexValue;
  private String segmentName;
  private long eventTimeInMs;
  private boolean isDelete;

  public StatsEvent(MessageInfo messageInfo, String segmentName, IndexValue putIndexValue,
      long eventTimeInMs, boolean isDelete) {
    this.messageInfo = messageInfo;
    this.putIndexValue = putIndexValue;
    this.segmentName = segmentName;
    this.eventTimeInMs = eventTimeInMs;
    this.isDelete =  isDelete;
  }

  public StatsEvent(MessageInfo messageInfo, String segmentName, long eventTimeInMs) {
    this(messageInfo, segmentName, null, eventTimeInMs, false);
  }

  public boolean isDelete() {
    return isDelete;
  }

  public MessageInfo getMessageInfo() {
    return messageInfo;
  }

  public IndexValue getPutIndexValue() {
    return putIndexValue;
  }

  public String getSegmentName() {
    return segmentName;
  }

  public long getEventTime() {
    return eventTimeInMs;
  }
}
