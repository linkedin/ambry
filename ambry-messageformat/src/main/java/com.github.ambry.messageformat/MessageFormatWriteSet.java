package com.github.ambry.messageformat;

import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.MessageWriteSet;
import com.github.ambry.store.Write;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.List;

/**
 * A message write set that writes to the underlying write interface
 */
public class MessageFormatWriteSet implements MessageWriteSet {

  private final InputStream streamToWrite;
  private long sizeToWrite;
  private List<MessageInfo> streamInfo;

  public MessageFormatWriteSet(InputStream stream, List<MessageInfo> streamInfo) {
    streamToWrite = stream;
    sizeToWrite = 0;
    for (MessageInfo info : streamInfo) {
      sizeToWrite += info.getSize();
    }
    this.streamInfo = streamInfo;
  }

  @Override
  public long writeTo(Write writeChannel) throws IOException {
    long sizeWritten = 0;
    while (sizeWritten < sizeToWrite) {
      sizeWritten += writeChannel.appendFrom(Channels.newChannel(streamToWrite), sizeToWrite);
    }
    return sizeWritten;
  }

  @Override
  public List<MessageInfo> getMessageSetInfo() {
    return streamInfo;
  }
}
