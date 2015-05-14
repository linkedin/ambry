package com.github.ambry.messageformat;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.SystemTime;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class represents an Inputstream containing blobs
 * Invalid blobs are are skipped based on validation criteria. For now Corrupt blobs are skipped when read from the stream
 * Non-corrupt blobs should have the format:  | header | blobId | blob properties | user metadata | blob |
 * This class is similar to ByteBufferInputStream which MessageFormatWriteSet uses, just that invalid blobs are skipped
 * during read
 */
public class ValidMessageFormatInputStream extends InputStream {
  private int mark;
  private int readLimit;
  private int validSize;
  private int sizeLeftToRead;
  private Iterator<MessageInfoByteBufferPair> messageInfoByteBufferPairIterator;
  private MessageInfoByteBufferPair currentMessageInfoByteBufferPair;
  private Logger logger;
  private StoreKeyFactory storeKeyFactory;
  private List<MessageInfo> messageInfoList;
  private List<MessageInfoByteBufferPair> messageInfoByteBufferPairList;
  private int validMessageInfoCount;
  private final boolean validateMessageStream;
  private MetricRegistry metricRegistry;

  //metrics
  public Histogram messageFormatValidationTime;
  public Histogram messageFormatGlobalValidationTime;

  public ValidMessageFormatInputStream(List<MessageInfoByteBufferPair> messageInfoByteBufferPairList,
      List<MessageInfo> messageInfoList, int validSize, StoreKeyFactory storeKeyFactory,
      final boolean validateMessageStream, int validMessageInfoCount, MetricRegistry metricRegistry) {
    this.mark = -1;
    this.readLimit = -1;
    this.storeKeyFactory = storeKeyFactory;
    this.validSize = validSize;
    this.sizeLeftToRead = validSize;
    this.messageInfoList = messageInfoList;
    this.messageInfoByteBufferPairList = messageInfoByteBufferPairList;
    this.messageInfoByteBufferPairIterator = messageInfoByteBufferPairList.iterator();
    this.currentMessageInfoByteBufferPair = messageInfoByteBufferPairIterator.next();
    this.validateMessageStream = validateMessageStream;
    this.validMessageInfoCount = validMessageInfoCount;
    this.metricRegistry = metricRegistry;
    this.logger = LoggerFactory.getLogger(getClass());
    initializeMetrics();
  }

  /**
   * @param stream The stream from which bytes need to be read. If the underlying stream is SocketInputStream, it needs
   *               to be blocking
   * @param messageInfoList List of MessageInfo which contains details about the messages in the stream
   * @param storeKeyFactory factory which is used to read the key from the stream
   * @param validateMessageStream whether the stream should be checked for validity or not
   * @param metricRegistry Metric register to register metrics
   * @throws java.io.IOException
   */
  public ValidMessageFormatInputStream(InputStream stream, List<MessageInfo> messageInfoList,
      StoreKeyFactory storeKeyFactory, final boolean validateMessageStream, MetricRegistry metricRegistry)
      throws IOException {
    this.messageInfoList = messageInfoList;
    this.storeKeyFactory = storeKeyFactory;
    this.logger = LoggerFactory.getLogger(getClass());
    this.validateMessageStream = validateMessageStream;
    this.metricRegistry = metricRegistry;
    messageInfoByteBufferPairList = new ArrayList<MessageInfoByteBufferPair>();
    this.initializeMetrics();

    // check for empty list
    if (messageInfoList.size() == 0) {
      sizeLeftToRead = validSize = 0;
      this.mark = -1;
      this.readLimit = -1;
      validMessageInfoCount = 0;
      return;
    }

    int size = 0;
    for (MessageInfo info : messageInfoList) {
      size += info.getSize();
    }

    ReadableByteChannel readableByteChannel = Channels.newChannel(stream);
    int totalRead = 0;
    int absoluteStartOffset = 0;
    long startTime = SystemTime.getInstance().milliseconds();
    for (int i = 0; i < messageInfoList.size(); i++) {
      int read = 0;
      int sizeToBeRead = (int) messageInfoList.get(i).getSize();
      ByteBuffer byteBuffer = ByteBuffer.allocate(sizeToBeRead);
      while (read < sizeToBeRead) {
        int sizeRead = readableByteChannel.read(byteBuffer);
        if (sizeRead == 0 || sizeRead == -1) {
          throw new IOException("Total size read " + (totalRead + read) + " is less than the size to be read " + size);
        }
        read += sizeRead;
      }
      totalRead += sizeToBeRead;
      byteBuffer.flip();
      if (!validateMessageStream || isValid(new ByteBufferInputStream(byteBuffer), sizeToBeRead,
          totalRead - sizeToBeRead, storeKeyFactory)) {
        byteBuffer.flip();
        messageInfoByteBufferPairList
            .add(new MessageInfoByteBufferPair(messageInfoList.get(i), byteBuffer, absoluteStartOffset));
        validSize += sizeToBeRead;
        validMessageInfoCount++;
      }
      absoluteStartOffset += sizeToBeRead;
    }
    messageFormatGlobalValidationTime.update(SystemTime.getInstance().milliseconds() - startTime);
    messageInfoByteBufferPairIterator = messageInfoByteBufferPairList.iterator();
    currentMessageInfoByteBufferPair = messageInfoByteBufferPairIterator.next();
    sizeLeftToRead = validSize;
  }

  public void initializeMetrics() {
    messageFormatValidationTime = metricRegistry
        .histogram(MetricRegistry.name(ValidMessageFormatInputStream.class, "MessageFormatValidationTime"));
    messageFormatGlobalValidationTime = metricRegistry
        .histogram(MetricRegistry.name(ValidMessageFormatInputStream.class, "MessageFormatGlobalValidationTime"));
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
    if (sizeLeftToRead == 0) {
      return -1;
    }
    if (currentMessageInfoByteBufferPair.getByteBuffer().position() == currentMessageInfoByteBufferPair.getMsgInfo()
        .getSize()) {
      if (!messageInfoByteBufferPairIterator.hasNext()) {
        return -1;
      } else {
        currentMessageInfoByteBufferPair = messageInfoByteBufferPairIterator.next();
      }
    }
    sizeLeftToRead--;
    return currentMessageInfoByteBufferPair.getByteBuffer().get() & 0xFF;
  }

  @Override
  public int read(byte[] bytes, int offset, int length)
      throws IOException {
    if (bytes == null) {
      throw new IllegalArgumentException("Passed in byte array cannot be null ");
    } else if (length < 0) {
      throw new IllegalArgumentException("length cannot be < 0 ");
    } else if (offset < 0 || offset > bytes.length) {
      throw new IndexOutOfBoundsException();
    } else if (length == 0) {
      return 0;
    }

    int count = Math.min(sizeLeftToRead, Math.min(bytes.length - offset, length));
    if (count == 0) {
      return -1;
    }

    int read = 0;
    int sizeYetToRead = count;

    do {
      if (currentMessageInfoByteBufferPair.getByteBuffer().remaining() == 0) {
        currentMessageInfoByteBufferPair = messageInfoByteBufferPairIterator.next();
      }
      int sizeToReadFromCurrentBuffer =
          Math.min(currentMessageInfoByteBufferPair.getByteBuffer().remaining(), sizeYetToRead);
      currentMessageInfoByteBufferPair.getByteBuffer().get(bytes, offset, sizeToReadFromCurrentBuffer);
      read += sizeToReadFromCurrentBuffer;
      sizeYetToRead -= sizeToReadFromCurrentBuffer;
      offset += sizeToReadFromCurrentBuffer;
    } while (read < count);
    return count;
  }

  @Override
  public int available()
      throws IOException {
    return sizeLeftToRead;
  }

  @Override
  public synchronized void reset()
      throws IOException {
    if (readLimit == -1 || mark == -1) {
      throw new IOException("Mark not set before reset invoked.");
    }
    if (currentMessageInfoByteBufferPair.getAbsoluteStartOffset() + currentMessageInfoByteBufferPair.getByteBuffer()
        .position() - mark > readLimit) {
      throw new IOException("Read limit exceeded before reset invoked.");
    }
    // walk until you reach the marked bytebuffer
    Iterator<MessageInfoByteBufferPair> tempIterator = messageInfoByteBufferPairList.iterator();
    while (tempIterator.hasNext()) {
      MessageInfoByteBufferPair messageInfoByteBuffer = tempIterator.next();
      if (messageInfoByteBuffer.getAbsoluteStartOffset() + messageInfoByteBuffer.getMsgInfo().getSize() > mark) {
        messageInfoByteBuffer.getByteBuffer().reset();
        break;
      }
    }
    // reset/flip all bytebuffers until current bytebuffer (where reset is called) is reached
    while (tempIterator.hasNext()) {
      MessageInfoByteBufferPair messageInfoByteBuffer = tempIterator.next();
      if (messageInfoByteBuffer.getAbsoluteStartOffset() == currentMessageInfoByteBufferPair.getAbsoluteStartOffset()) {
        messageInfoByteBuffer.getByteBuffer().flip();
        break;
      } else {
        messageInfoByteBuffer.getByteBuffer().flip();
      }
    }

    sizeLeftToRead = validSize;
    messageInfoByteBufferPairIterator = messageInfoByteBufferPairList.iterator();
    // make currentMessageInfoByteBufferPair refer to the msginfobytebufferpair when mark was called
    while (messageInfoByteBufferPairIterator.hasNext()) {
      currentMessageInfoByteBufferPair = messageInfoByteBufferPairIterator.next();
      if (currentMessageInfoByteBufferPair.getAbsoluteStartOffset() + currentMessageInfoByteBufferPair.getMsgInfo()
          .getSize() > mark) {
        break;
      } else {
        sizeLeftToRead -= currentMessageInfoByteBufferPair.getMsgInfo().getSize();
      }
    }
  }

  @Override
  public synchronized void mark(int readLimit) {
    this.mark =
        currentMessageInfoByteBufferPair.getAbsoluteStartOffset() + currentMessageInfoByteBufferPair.getByteBuffer()
            .position();
    this.readLimit = readLimit;
    currentMessageInfoByteBufferPair.getByteBuffer().mark();
  }

  @Override
  public boolean markSupported() {
    return true;
  }

  public ValidMessageFormatInputStream duplicate() {
    return new ValidMessageFormatInputStream(messageInfoByteBufferPairList, messageInfoList, validSize, storeKeyFactory,
        validateMessageStream, validMessageInfoCount, metricRegistry);
  }

  public boolean hasInvalidMessages() {
    if (messageInfoList.size() != validMessageInfoCount) {
      return false;
    }
    return true;
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
          throw new IllegalStateException("Message cannot not be a deleted record ");
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

