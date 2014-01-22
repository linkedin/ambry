package com.github.ambry.messageformat;


import com.github.ambry.network.Send;
import com.github.ambry.store.MessageReadSet;
import com.github.ambry.utils.ByteBufferOutputStream;
import com.github.ambry.utils.SystemTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;

/**
 * A send object for the message format to send data from the underlying store
 * to the network channel
 */

public class MessageFormatSend implements Send {

  private MessageReadSet readSet;
  private MessageFormatFlags flag;
  private ArrayList<SendInfo> infoList;
  private long totalSizeToWrite;
  private long sizeWritten;
  private int currentWriteIndex;
  private long sizeWrittenFromCurrentIndex;
  private Logger logger = LoggerFactory.getLogger(getClass());

  private class SendInfo {
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

  public MessageFormatSend(MessageReadSet readSet, MessageFormatFlags flag, MessageFormatMetrics metrics) throws IOException, MessageFormatException {
    this.readSet = readSet;
    this.flag = flag;
    totalSizeToWrite = 0;
    long startTime = SystemTime.getInstance().milliseconds();
    calculateOffsets();
    metrics.calculateOffsetMessageSendTime.update(SystemTime.getInstance().milliseconds() - startTime);
    sizeWritten = 0;
    currentWriteIndex = 0;
    sizeWrittenFromCurrentIndex = 0;
  }

  // calculates the offsets from the MessageReadSet that needs to be sent over the network
  // based on the type of data requested as indicated by the flags
  private void calculateOffsets() throws IOException, MessageFormatException {
    // get size
    int messageCount = readSet.count();
    // for each message, determine the offset and size that needs to be sent based on the flag
    infoList = new ArrayList<SendInfo>(messageCount);
    for (int i = 0; i < messageCount; i++) {
      if (flag == MessageFormatFlags.All) {
        // just copy over the total size and use relative offset to be 0
        // We do not have to check any version in this case as we dont
        // have to read any data to deserialize anything.
        infoList.add(i, new SendInfo(0, readSet.sizeInBytes(i)));
        totalSizeToWrite += readSet.sizeInBytes(i);

      }
      else {
        // read header version
        ByteBuffer headerVersion = ByteBuffer.allocate(MessageFormatRecord.Version_Field_Size_In_Bytes);
        readSet.writeTo(i, Channels.newChannel(new ByteBufferOutputStream(headerVersion)), 0,
                                               MessageFormatRecord.Version_Field_Size_In_Bytes);
        headerVersion.flip();
        switch (headerVersion.getShort()) {
          case MessageFormatRecord.Message_Header_Version_V1:

            // read the header
            ByteBuffer header = ByteBuffer.allocate(MessageFormatRecord.MessageHeader_Format_V1.getHeaderSize());
            headerVersion.clear();
            header.putShort(headerVersion.getShort());
            readSet.writeTo(i, Channels.newChannel(new ByteBufferOutputStream(header)),
                            MessageFormatRecord.Version_Field_Size_In_Bytes,
                            MessageFormatRecord.MessageHeader_Format_V1.getHeaderSize() - MessageFormatRecord.Version_Field_Size_In_Bytes);
            header.flip();
            MessageFormatRecord.MessageHeader_Format_V1 headerFormat = new MessageFormatRecord.MessageHeader_Format_V1(header);
            headerFormat.verifyHeader();

            if (flag == MessageFormatFlags.BlobProperties) {
              int blobPropertyRecordSize = headerFormat.getUserMetadataRecordRelativeOffset() -
                                           headerFormat.getBlobPropertyRecordRelativeOffset();

              infoList.add(i, new SendInfo(headerFormat.getBlobPropertyRecordRelativeOffset(), blobPropertyRecordSize));
              totalSizeToWrite += blobPropertyRecordSize;
              logger.trace("Sending blob properties for message relativeOffset : {} size : {}",
                           infoList.get(i).relativeOffset(), infoList.get(i).sizetoSend());
            }
            else if (flag == MessageFormatFlags.BlobUserMetadata) {
              int userMetadataRecordSize = headerFormat.getBlobRecordRelativeOffset() -
                                     headerFormat.getUserMetadataRecordRelativeOffset();

              infoList.add(i, new SendInfo(headerFormat.getUserMetadataRecordRelativeOffset(), userMetadataRecordSize));
              totalSizeToWrite += userMetadataRecordSize;
              logger.trace("Sending user metadata for message relativeOffset : {} size : {}",
                           infoList.get(i).relativeOffset(), infoList.get(i).sizetoSend());
            }
            else  if (flag == MessageFormatFlags.Blob) {
              long blobRecordSize = headerFormat.getMessageSize() -
                      (headerFormat.getBlobRecordRelativeOffset() - headerFormat.getBlobPropertyRecordRelativeOffset());
              infoList.add(i, new SendInfo(headerFormat.getBlobRecordRelativeOffset(), blobRecordSize));
              totalSizeToWrite += blobRecordSize;
              logger.trace("Sending data for message relativeOffset : {} size : {}",
                           infoList.get(i).relativeOffset(), infoList.get(i).sizetoSend());
            }
            else { //just return the header
              int messageHeaderSize = MessageFormatRecord.MessageHeader_Format_V1.getHeaderSize() +
                                      MessageFormatRecord.Version_Field_Size_In_Bytes;
              infoList.add(i, new SendInfo(0, messageHeaderSize));
              totalSizeToWrite += messageHeaderSize;
              logger.trace("Sending message header relativeOffset : {} size : {}",
                           infoList.get(i).relativeOffset(), infoList.get(i).sizetoSend());
            }
            break;
          default:
            throw new MessageFormatException("Version not known while reading message - " + headerVersion.getShort(),
                                             MessageFormatErrorCodes.Unknown_Format_Version);
        }
      }
    }
  }

  @Override
  public void writeTo(WritableByteChannel channel) throws IOException {
    if (!isSendComplete()) {
      long written = readSet.writeTo(currentWriteIndex, channel,
                                     infoList.get(currentWriteIndex).relativeOffset() + sizeWrittenFromCurrentIndex,
                                     infoList.get(currentWriteIndex).sizetoSend() - sizeWrittenFromCurrentIndex);
      logger.trace("writeindex {} relativeOffset {} maxSize {} written {}",
                   currentWriteIndex,
                   infoList.get(currentWriteIndex).relativeOffset() + sizeWrittenFromCurrentIndex,
                   infoList.get(currentWriteIndex).sizetoSend() - sizeWrittenFromCurrentIndex, written);
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
  public boolean isSendComplete() {
    return totalSizeToWrite == sizeWritten;
  }

  @Override
  public long sizeInBytes() {
    return totalSizeToWrite;
  }
}
