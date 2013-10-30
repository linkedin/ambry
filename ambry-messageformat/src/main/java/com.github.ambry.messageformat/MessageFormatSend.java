package com.github.ambry.messageformat;


import com.github.ambry.network.Send;
import com.github.ambry.store.MessageReadSet;
import com.github.ambry.utils.ByteBufferOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;

/**
 * Created with IntelliJ IDEA.
 * User: srsubram
 * Date: 10/27/13
 * Time: 9:48 AM
 * To change this template use File | Settings | File Templates.
 */

enum MessageFormatFlags {
  SystemMetadata,
  UserMetadata,
  MessageHeader,
  Data,
  All
}

public class MessageFormatSend implements Send {

  private MessageReadSet readSet;
  private MessageFormatFlags flag;
  private ArrayList<SendInfo> infoList;
  private long totalSizeToWrite;
  private long sizeWritten;
  private int currentWriteIndex;
  private long sizeWrittenFromCurrentIndex;
  private Logger logger = LoggerFactory.getLogger(getClass());

  class SendInfo {
    private long relativeOffset;
    private long sizeToSend;

    public SendInfo(long relativeOffset, long sizeToSend) {
      this.relativeOffset = relativeOffset;
      this.sizeToSend = sizeToSend;
    }

    public long relativeOffset() {
      return relativeOffset;
    }

    public long sizetoSend() {
      return sizeToSend;
    }
  }

  public MessageFormatSend(MessageReadSet readSet, MessageFormatFlags flag) throws IOException {
    this.readSet = readSet;
    this.flag = flag;
    totalSizeToWrite = 0;
    calculateOffsets();
    sizeWritten = 0;
    currentWriteIndex = 0;
    sizeWrittenFromCurrentIndex = 0;
  }

  private void calculateOffsets() throws IOException {
    // get size
    int messageCount = readSet.count();
    // for each message, determine the offset and size that needs to be sent based on the flag
    infoList = new ArrayList<SendInfo>(messageCount);
    for (int i = 0; i < messageCount; i++) {
      if (flag == MessageFormatFlags.All) {
        // just copy over the total size and use relative offset to be 0
        infoList.add(i, new SendInfo(0, readSet.sizeInBytes(i)));
        totalSizeToWrite += readSet.sizeInBytes(i);

      }
      else {
        // read header version
        ByteBuffer headerVersion = ByteBuffer.allocate(2);
        readSet.writeTo(i, Channels.newChannel(new ByteBufferOutputStream(headerVersion)), 0, 2);
        if (headerVersion.getShort() == 0) {

          // read the header
          ByteBuffer header = ByteBuffer.allocate(MessageFormat.MessageHeader_V1.Message_Header_Size_V1);
          readSet.writeTo(i, Channels.newChannel(
                  new ByteBufferOutputStream(header)), 2, MessageFormat.MessageHeader_V1.Message_Header_Size_V1);
          MessageFormat.MessageHeader_V1 headerFormat = new MessageFormat.MessageHeader_V1(header);

          if (flag == MessageFormatFlags.SystemMetadata) {
            int systemMetadataSize = headerFormat.getUserMetadataOffset() - headerFormat.getSystemMetadataOffset() - 6;
            infoList.add(i, new SendInfo(headerFormat.getSystemMetadataOffset() + 6, systemMetadataSize));
            totalSizeToWrite += systemMetadataSize;
            logger.trace("Sending all data for message relativeOffset : {} size : {}",
                    infoList.get(i).relativeOffset(), infoList.get(i).sizetoSend());
          }
          else if (flag == MessageFormatFlags.UserMetadata) {
            int userMetadataSize = headerFormat.getDataOffset() - headerFormat.getUserMetadataOffset() - 6;
            infoList.add(i, new SendInfo(headerFormat.getUserMetadataOffset() + 6, userMetadataSize));
            totalSizeToWrite += userMetadataSize;
            logger.trace("Sending user metadata for message relativeOffset : {} size : {}",
                    infoList.get(i).relativeOffset(), infoList.get(i).sizetoSend());
          }
          else  if (flag == MessageFormatFlags.Data) {
            long dataSize = headerFormat.getSize() - headerFormat.getDataOffset() - 10;
            infoList.add(i, new SendInfo(headerFormat.getDataOffset() + 10, dataSize));
            totalSizeToWrite += dataSize;
            logger.trace("Sending data for message relativeOffset : {} size : {}",
                    infoList.get(i).relativeOffset(), infoList.get(i).sizetoSend());
          }
          else if (flag == MessageFormatFlags.MessageHeader) {
            infoList.add(i, new SendInfo(2, MessageFormat.MessageHeader_V1.Message_Header_Size_V1));
            totalSizeToWrite += MessageFormat.MessageHeader_V1.Message_Header_Size_V1;
            logger.trace("Sending message header relativeOffset : {} size : {}",
                    infoList.get(i).relativeOffset(), infoList.get(i).sizetoSend());
          }
        }
      }
    }
  }

  @Override
  public void writeTo(WritableByteChannel channel) throws IOException {
    if (!isComplete()) {
      long written = readSet.writeTo(currentWriteIndex, channel,
              infoList.get(currentWriteIndex).relativeOffset() + sizeWrittenFromCurrentIndex,
              infoList.get(currentWriteIndex).sizetoSend() - sizeWrittenFromCurrentIndex);
      sizeWritten += written;
      sizeWrittenFromCurrentIndex += written;
      logger.trace("size written in this loop : {} size written till now : {}", written, sizeWritten);
      if (sizeWrittenFromCurrentIndex == infoList.get(currentWriteIndex).sizetoSend()) {
        currentWriteIndex++;
        sizeWrittenFromCurrentIndex = 0;
      }
    }
  }

  @Override
  public boolean isComplete() {
    return totalSizeToWrite == sizeWritten;
  }

  @Override
  public long sizeInBytes() {
    return totalSizeToWrite;
  }
}
