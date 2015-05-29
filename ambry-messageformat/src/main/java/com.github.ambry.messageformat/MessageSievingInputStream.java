package com.github.ambry.messageformat;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.SystemTime;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * InputStream that skips invalid blobs based on some validation criteria.
 * For now, the check only supports detection of message corruption
 */
public class MessageSievingInputStream extends InputStream {
  private int validSize;
  private final Logger logger;
  private ByteBuffer byteBuffer;
  private boolean hasInvalidMessages;
  private List<MessageInfo> validMessageInfoList;

  //metrics
  public Histogram messageFormatValidationTime;
  public Histogram messageFormatBatchValidationTime;

  /**
   * @param stream The stream from which bytes need to be read. If the underlying stream is SocketInputStream, it needs
   *               to be blocking
   * @param messageInfoList List of MessageInfo which contains details about the messages in the stream
   * @param storeKeyFactory factory which is used to read the key from the stream
   * @param metricRegistry Metric register to register metrics
   * @throws java.io.IOException
   */
  public MessageSievingInputStream(InputStream stream, List<MessageInfo> messageInfoList,
      StoreKeyFactory storeKeyFactory, MetricRegistry metricRegistry)
      throws IOException {
    this.logger = LoggerFactory.getLogger(getClass());
    messageFormatValidationTime =
        metricRegistry.histogram(MetricRegistry.name(MessageSievingInputStream.class, "MessageFormatValidationTime"));
    messageFormatBatchValidationTime = metricRegistry
        .histogram(MetricRegistry.name(MessageSievingInputStream.class, "MessageFormatBatchValidationTime"));
    validSize = 0;
    hasInvalidMessages = false;
    validMessageInfoList = new ArrayList<MessageInfo>();

    // check for empty list
    if (messageInfoList.size() == 0) {
      byteBuffer = ByteBuffer.allocate(0);
      return;
    }

    int totalMessageListSize = 0;
    for (MessageInfo info : messageInfoList) {
      totalMessageListSize += info.getSize();
    }

    byte[] data = new byte[totalMessageListSize];
    long startTime = SystemTime.getInstance().milliseconds();
    for (MessageInfo msgInfo : messageInfoList) {
      int msgSize = (int) msgInfo.getSize();
      stream.read(data, validSize, msgSize);
      ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data, validSize, msgSize);
      if (checkForMessageValidity(byteArrayInputStream, validSize, msgSize, storeKeyFactory)) {
        validSize += msgSize;
        validMessageInfoList.add(msgInfo);
      } else {
        hasInvalidMessages = true;
      }
    }
    messageFormatBatchValidationTime.update(SystemTime.getInstance().milliseconds() - startTime);
    byteBuffer = ByteBuffer.wrap(data, 0, validSize);
  }

  /**
   * Returns the total size of all valid messages that could be read from the stream
   * @return validSize
   */
  public int getSize() {
    return validSize;
  }

  @Override
  public int read()
      throws IOException {
    if (!byteBuffer.hasRemaining()) {
      return -1;
    }
    return byteBuffer.get() & 0xFF;
  }

  @Override
  public int read(byte[] bytes, int offset, int length)
      throws IOException {
    if (bytes == null) {
      throw new NullPointerException();
    } else if (offset < 0 || length < 0 || length > bytes.length - offset) {
      throw new IndexOutOfBoundsException();
    } else if (length == 0) {
      return 0;
    }
    int count = Math.min(byteBuffer.remaining(), length);
    if (count == 0) {
      return -1;
    }
    byteBuffer.get(bytes, offset, count);
    return count;
  }

  /**
   * Whether the stream has invalid messages or not
   * @return
   */
  public boolean hasInvalidMessages() {
    return hasInvalidMessages;
  }

  public List<MessageInfo> getValidMessageInfoList() {
    return validMessageInfoList;
  }

  /**
   * Ensures blob validity in the given input stream. For now, blobs are checked for message corruption
   * @param byteArrayInputStream stream against which validation has to be done
   * @param size total size of the message expected
   * @param currentOffset Current offset at which the data has to be read from the given byte array
   * @param storeKeyFactory StoreKeyFactory used to get store key
   * @return true if message is valid and false otherwise
   * @throws IOException
   */
  private boolean checkForMessageValidity(ByteArrayInputStream byteArrayInputStream, int currentOffset, long size,
      StoreKeyFactory storeKeyFactory)
      throws IOException {
    boolean isValid = false;
    int startOffset = currentOffset;
    BlobProperties props = null;
    ByteBuffer metadata = null;
    BlobOutput output = null;
    long startTime = SystemTime.getInstance().milliseconds();
    try {
      int availableBeforeParsing = byteArrayInputStream.available();
      byte[] headerVersionInBytes = new byte[MessageFormatRecord.Version_Field_Size_In_Bytes];
      byteArrayInputStream.read(headerVersionInBytes, 0, MessageFormatRecord.Version_Field_Size_In_Bytes);
      ByteBuffer headerVersion = ByteBuffer.wrap(headerVersionInBytes);
      short version = headerVersion.getShort();
      if (version == 1) {
        ByteBuffer headerBuffer = ByteBuffer.allocate(MessageFormatRecord.MessageHeader_Format_V1.getHeaderSize());
        headerBuffer.putShort(version);
        byteArrayInputStream.read(headerBuffer.array(), 2, headerBuffer.capacity() - 2);
        headerBuffer.position(headerBuffer.capacity());
        headerBuffer.flip();
        MessageFormatRecord.MessageHeader_Format_V1 header =
            new MessageFormatRecord.MessageHeader_Format_V1(headerBuffer);
        StoreKey storeKey = storeKeyFactory.getStoreKey(new DataInputStream(byteArrayInputStream));

        if (header.getBlobPropertiesRecordRelativeOffset()
            != MessageFormatRecord.Message_Header_Invalid_Relative_Offset) {
          props = MessageFormatRecord.deserializeBlobProperties(byteArrayInputStream);
          metadata = MessageFormatRecord.deserializeUserMetadata(byteArrayInputStream);
          output = MessageFormatRecord.deserializeBlob(byteArrayInputStream);
        } else {
          throw new IllegalStateException("Message cannot be a deleted record ");
        }
        if (byteArrayInputStream.available() != 0) {
          logger.error("Parsed message size " + (availableBeforeParsing + byteArrayInputStream.available())
              + " is not equivalent to the size in message info " + availableBeforeParsing);
          isValid = false;
        }
        if (logger.isTraceEnabled()) {
          logger.trace("Message Successfully read");
          logger.trace(
              "Header - version {} Message Size {} Starting offset of the blob {} BlobPropertiesRelativeOffset {}"
                  + " UserMetadataRelativeOffset {} DataRelativeOffset {} DeleteRecordRelativeOffset {} Crc {}",
              header.getVersion(), header.getMessageSize(), startOffset, header.getBlobPropertiesRecordRelativeOffset(),
              header.getUserMetadataRecordRelativeOffset(), header.getBlobRecordRelativeOffset(),
              header.getDeleteRecordRelativeOffset(), header.getCrc());
          logger.trace("Id {} Blob Properties - blobSize {} Metadata - size {} Blob - size {} ", storeKey.getID(),
              props.getBlobSize(), metadata.capacity(), output.getSize());
        }
        isValid = true;
      } else {
        throw new MessageFormatException("Header version not supported " + version,
            MessageFormatErrorCodes.Data_Corrupt);
      }
    } catch (MessageFormatException e) {
      logger.error("MessageFormat exception thrown for a blob starting at offset " + startOffset + " with exception: ",
          e);
    } finally {
      messageFormatValidationTime.update(SystemTime.getInstance().milliseconds() - startTime);
    }
    return isValid;
  }
}

