package com.github.ambry.messageformat;

import com.github.ambry.network.Send;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.List;


/**
 * Holds multiple MessageFormatSend instances and sends them over the network
 */
public class CompositeMessageFormatSend implements Send {

  private final List<MessageFormatSend> compositeMessageSet;
  private long totalSizeToWrite;
  private int currentIndexInProgress;

  public CompositeMessageFormatSend(List<MessageFormatSend> compositeMessageSet) {
    this.compositeMessageSet = compositeMessageSet;
    this.currentIndexInProgress = 0;
    for (MessageFormatSend messageFormatSend : compositeMessageSet) {
      totalSizeToWrite += messageFormatSend.sizeInBytes();
    }
  }

  @Override
  public void writeTo(WritableByteChannel channel)
      throws IOException {
    if (currentIndexInProgress < compositeMessageSet.size()) {
      compositeMessageSet.get(currentIndexInProgress).writeTo(channel);
      if (compositeMessageSet.get(currentIndexInProgress).isSendComplete()) {
        currentIndexInProgress++;
      }
    }
  }

  @Override
  public boolean isSendComplete() {
    return currentIndexInProgress == compositeMessageSet.size();
  }

  @Override
  public long sizeInBytes() {
    return totalSizeToWrite;
  }
}
