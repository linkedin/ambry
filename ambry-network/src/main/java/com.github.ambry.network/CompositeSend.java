package com.github.ambry.network;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.List;


/**
 * Holds multiple Send instances and sends them over the network
 */
public class CompositeSend implements Send {

  private final List<Send> compositSendList;
  private long totalSizeToWrite;
  private int currentIndexInProgress;

  public CompositeSend(List<Send> compositSendList) {
    this.compositSendList = compositSendList;
    this.currentIndexInProgress = 0;
    for (Send messageFormatSend : compositSendList) {
      totalSizeToWrite += messageFormatSend.sizeInBytes();
    }
  }

  @Override
  public void writeTo(WritableByteChannel channel)
      throws IOException {
    if (currentIndexInProgress < compositSendList.size()) {
      compositSendList.get(currentIndexInProgress).writeTo(channel);
      if (compositSendList.get(currentIndexInProgress).isSendComplete()) {
        currentIndexInProgress++;
      }
    }
  }

  @Override
  public boolean isSendComplete() {
    return currentIndexInProgress == compositSendList.size();
  }

  @Override
  public long sizeInBytes() {
    return totalSizeToWrite;
  }
}
