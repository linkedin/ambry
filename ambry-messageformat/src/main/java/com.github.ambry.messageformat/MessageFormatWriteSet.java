package com.github.ambry.messageformat;

import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.MessageWriteSet;
import com.github.ambry.store.Write;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A message write set that writes to the underlying write interface
 */
public class MessageFormatWriteSet implements MessageWriteSet {

  private final InputStream streamToWrite;
  private long sizeToWrite;
  private List<MessageInfo> streamInfo;

  public MessageFormatWriteSet(InputStream streamToWrite, List<MessageInfo> streamInfo)
      throws IOException {
    sizeToWrite = 0;
    for (MessageInfo info : streamInfo) {
      sizeToWrite += info.getSize();
    }
    this.streamInfo = streamInfo;
    this.streamToWrite = streamToWrite;
  }

  @Override
  public long writeTo(Write writeChannel)
      throws IOException {
    ReadableByteChannel readableByteChannel = Channels.newChannel(streamToWrite);
    writeChannel.appendFrom(readableByteChannel, sizeToWrite);
    return sizeToWrite;
  }

  @Override
  public List<MessageInfo> getMessageSetInfo() {
    return streamInfo;
  }
}
