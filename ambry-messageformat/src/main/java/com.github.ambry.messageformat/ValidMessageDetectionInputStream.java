package com.github.ambry.messageformat;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.SystemTime;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * InputStream that skips invalid blobs based on a validation criteria
 * For now Corrupt blobs are skipped when read from the stream
 * Non-corrupt blobs should have the format:  | header | blobId | blob properties | user metadata | blob |
 */
public class ValidMessageDetectionInputStream extends InputStream {
  private int validSize;
  private final Logger logger;
  private ByteBuffer byteBuffer;
  private boolean hasInvalidMessage;
  private final MetricRegistry metricRegistry;

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
  public ValidMessageDetectionInputStream(InputStream stream, List<MessageInfo> messageInfoList,
      StoreKeyFactory storeKeyFactory, MetricRegistry metricRegistry)
      throws IOException {
    this.logger = LoggerFactory.getLogger(getClass());
    this.metricRegistry = metricRegistry;
    this.initializeMetrics();

    // check for empty list
    if (messageInfoList.size() == 0) {
      validSize = 0;
      byteBuffer = ByteBuffer.allocate(0);
      hasInvalidMessage = false;
      return;
    }

    int size = 0;
    for (MessageInfo info : messageInfoList) {
      size += info.getSize();
    }

    byte[] data = new byte[size];
    int offset = 0;
    long startTime = SystemTime.getInstance().milliseconds();
    for (MessageInfo msgInfo : messageInfoList) {
      int msgSize = (int) msgInfo.getSize();
      stream.read(data, offset, msgSize);
      InputStream inputStream = new ByteArrayInputStream(data, offset, msgSize);
      if (isValid(inputStream, msgSize, offset, storeKeyFactory)) {
        validSize += msgSize;
        offset += msgSize;
      } else {
        hasInvalidMessage = true;
      }
    }
    messageFormatBatchValidationTime.update(SystemTime.getInstance().milliseconds() - startTime);
    byteBuffer = ByteBuffer.wrap(data, 0, offset);
  }

  public void initializeMetrics() {
    messageFormatValidationTime = metricRegistry
        .histogram(MetricRegistry.name(ValidMessageDetectionInputStream.class, "MessageFormatValidationTime"));
    messageFormatBatchValidationTime = metricRegistry
        .histogram(MetricRegistry.name(ValidMessageDetectionInputStream.class, "MessageFormatBatchValidationTime"));
  }

  /**
   * Returns the total Size which could be read from the stream.
   * Passed in size in the constructor gives the total size of the stream. This method is responsible for
   * getting the total size of valid messages which could be read from the stream.
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
    int count = Math.min(byteBuffer.remaining(), length);
    if (count == 0) {
      return -1;
    }
    byteBuffer.get(bytes, offset, count);
    return count;
  }

  public boolean hasInvalidMessages() {
    return hasInvalidMessage;
  }

  /**
   * Ensures blob validity for all the blobs in the given input stream
   * For now, we check for blob corruption
   * Expected format for a single blob : | header | blob id | blob property | user metadata | blob |
   * @param inputStream InputStream for which the check is to be done
   * @param size total size of the message expected
   * @param currentOffset Current offset at which the stream was read from the buffer
   * @param storeKeyFactory StoreKeyFactory used to get store key
   * @return true if message was corrupt and false otherwise
   * @throws IOException
   */
  private boolean isValid(InputStream inputStream, long size, int currentOffset, StoreKeyFactory storeKeyFactory)
      throws IOException {
    StringBuilder strBuilder = new StringBuilder();
    boolean isValid = false;
    long startTime = SystemTime.getInstance().milliseconds();
    try {
      int availableBeforeParsing = inputStream.available();
      byte[] headerVersionInBytes = new byte[MessageFormatRecord.Version_Field_Size_In_Bytes];
      inputStream.read(headerVersionInBytes, 0, MessageFormatRecord.Version_Field_Size_In_Bytes);
      ByteBuffer headerVersion = ByteBuffer.wrap(headerVersionInBytes);
      short version = headerVersion.getShort();
      if (version == 1) {
        ByteBuffer buffer = ByteBuffer.allocate(MessageFormatRecord.MessageHeader_Format_V1.getHeaderSize());
        buffer.putShort(version);
        inputStream.read(buffer.array(), 2, buffer.capacity() - 2);
        buffer.position(buffer.capacity());
        buffer.flip();
        MessageFormatRecord.MessageHeader_Format_V1 header = new MessageFormatRecord.MessageHeader_Format_V1(buffer);
        strBuilder.append("Header - version ").append(header.getVersion()).append(" messagesize ").
            append(header.getMessageSize()).append(" currentOffset ").append(currentOffset)
            .append(" blobPropertiesRelativeOffset ").append(header.getBlobPropertiesRecordRelativeOffset())
            .append(" userMetadataRelativeOffset ").append(header.getUserMetadataRecordRelativeOffset())
            .append(" dataRelativeOffset ").append(header.getBlobRecordRelativeOffset())
            .append(" crc " + header.getCrc()).append("\n");

        StoreKey storeKey = storeKeyFactory.getStoreKey(new DataInputStream(inputStream));
        strBuilder.append(" Id - ").append(storeKey.getID());
        if (header.getBlobPropertiesRecordRelativeOffset()
            != MessageFormatRecord.Message_Header_Invalid_Relative_Offset) {
          BlobProperties props = MessageFormatRecord.deserializeBlobProperties(inputStream);
          strBuilder.append(" Blob properties - blobSize  ").append(props.getBlobSize()).append(" serviceId ")
              .append(props.getServiceId()).append("\n");
          ByteBuffer metadata = MessageFormatRecord.deserializeUserMetadata(inputStream);
          strBuilder.append(" Metadata - size ").append(metadata.capacity()).append("\n");
          BlobOutput output = MessageFormatRecord.deserializeBlob(inputStream);
          strBuilder.append("Blob - size ").append(output.getSize()).append("\n");
        } else {
          throw new IllegalStateException("Message cannot be a deleted record ");
        }
        logger.trace(strBuilder.toString());
        if (availableBeforeParsing - inputStream.available() != size) {
          logger.error(
              "Parsed message size " + (inputStream.available() - availableBeforeParsing) + " is not equivalent " +
                  "to the size in message info " + size);
          isValid = false;
        }
        logger.trace("Message successfully read {} ", strBuilder);
        isValid = true;
      } else {
        throw new IllegalStateException("Header version not supported " + version);
      }
    } catch (IllegalArgumentException e) {
      logger.error("Illegal arg exception thrown at " + currentOffset + " and exception: ", e);
    } catch (IllegalStateException e) {
      logger.error("Illegal state exception thrown at " + currentOffset + " and exception: ", e);
    } catch (MessageFormatException e) {
      logger.error("MessageFormat exception thrown at " + currentOffset + " and exception: ", e);
    } catch (EOFException e) {
      logger.error("EOFException thrown at " + currentOffset, e);
      throw e;
    } finally {
      messageFormatValidationTime.update(SystemTime.getInstance().milliseconds() - startTime);
    }
    return isValid;
  }

  class MessageInfoByteBufferPair {
    private MessageInfo msgInfo;
    private ByteBuffer byteBuffer;
    private int absoluteStartOffset;

    public MessageInfoByteBufferPair(MessageInfo msgInfo, ByteBuffer byteBuffer, int absoluteStartOffset) {
      this.msgInfo = msgInfo;
      this.byteBuffer = byteBuffer;
      this.absoluteStartOffset = absoluteStartOffset;
    }

    MessageInfo getMsgInfo() {
      return msgInfo;
    }

    ByteBuffer getByteBuffer() {
      return byteBuffer;
    }

    int getAbsoluteStartOffset() {
      return absoluteStartOffset;
    }
  }
}

