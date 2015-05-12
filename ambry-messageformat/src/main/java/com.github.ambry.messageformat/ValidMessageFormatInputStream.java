package com.github.ambry.messageformat;

import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.ByteBufferInputStream;
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
  private Iterator<MessageInfoByteBuffer> messageInfoByteBufferIterator;
  private MessageInfoByteBuffer currentMessageInfoByteBuffer;
  private Logger logger;
  private StoreKeyFactory storeKeyFactory;
  private List<MessageInfo> messageInfoList;
  private List<MessageInfoByteBuffer> messageInfoByteBufferList;
  private int validMessageInfoCount;
  private final boolean validateMessageStream;

  public ValidMessageFormatInputStream(List<MessageInfoByteBuffer> messageInfoByteBufferList,
      List<MessageInfo> messageInfoList, int validSize, StoreKeyFactory storeKeyFactory, Logger logger,
      final boolean validateMessageStream) {
    this.mark = -1;
    this.readLimit = -1;
    this.storeKeyFactory = storeKeyFactory;
    this.validSize = validSize;
    this.sizeLeftToRead = validSize;
    this.messageInfoList = messageInfoList;
    this.logger = logger;
    this.messageInfoByteBufferList = messageInfoByteBufferList;
    this.messageInfoByteBufferIterator = messageInfoByteBufferList.iterator();
    this.currentMessageInfoByteBuffer = messageInfoByteBufferIterator.next();
    this.validateMessageStream = validateMessageStream;
  }

  /**
   * @param stream The stream from which bytes need to be read. If the underlying stream is SocketInputStream, it needs
   *               to be blocking
   * @param messageInfoList List of MessageInfo which contains details about the messages in the stream
   * @param storeKeyFactory factory which is used to read the key from the stream
   * @param logger used for logging
   * @param validateMessageStream whether the stream should be checked for validity or not
   * @throws java.io.IOException
   */
  public ValidMessageFormatInputStream(InputStream stream, List<MessageInfo> messageInfoList,
      StoreKeyFactory storeKeyFactory, Logger logger, final boolean validateMessageStream)
      throws IOException {
    this.messageInfoList = messageInfoList;
    this.storeKeyFactory = storeKeyFactory;
    this.logger = logger;
    this.validateMessageStream = validateMessageStream;

    // check for empty list
    if (messageInfoList.size() == 0) {
      sizeLeftToRead = validSize = 0;
      this.mark = -1;
      this.readLimit = -1;
      return;
    }

    int size = 0;
    for (MessageInfo info : messageInfoList) {
      size += info.getSize();
    }
    ReadableByteChannel readableByteChannel = Channels.newChannel(stream);
    messageInfoByteBufferList = new ArrayList<MessageInfoByteBuffer>();
    int totalRead = 0;
    int absoluteStartOffset = 0;
    for (int i = 0; i < messageInfoList.size(); i++) {
      int read = 0;
      int sizeToBeRead = (int) messageInfoList.get(i).getSize();
      ByteBuffer byteBufferCurrentMessage = ByteBuffer.allocate(sizeToBeRead);
      while (read < sizeToBeRead) {
        int sizeRead = readableByteChannel.read(byteBufferCurrentMessage);
        if (sizeRead == 0 || sizeRead == -1) {
          throw new IOException("Total size read " + (totalRead + read) + " is less than the size to be read " + size);
        }
        read += sizeRead;
      }
      totalRead += sizeToBeRead;
      byteBufferCurrentMessage.flip();
      if (!validateMessageStream || isValid(new ByteBufferInputStream(byteBufferCurrentMessage), sizeToBeRead,
          totalRead - sizeToBeRead, storeKeyFactory)) {
        byteBufferCurrentMessage.flip();
        messageInfoByteBufferList
            .add(new MessageInfoByteBuffer(messageInfoList.get(i), byteBufferCurrentMessage, absoluteStartOffset));
        validSize += sizeToBeRead;
        validMessageInfoCount++;
      }
      absoluteStartOffset += sizeToBeRead;
    }
    messageInfoByteBufferIterator = messageInfoByteBufferList.iterator();
    currentMessageInfoByteBuffer = messageInfoByteBufferIterator.next();
    sizeLeftToRead = validSize;
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
    if (currentMessageInfoByteBuffer.getByteBuffer().position() == currentMessageInfoByteBuffer.getMsgInfo()
        .getSize()) {
      if (!messageInfoByteBufferIterator.hasNext()) {
        return -1;
      } else {
        currentMessageInfoByteBuffer = messageInfoByteBufferIterator.next();
      }
    }
    sizeLeftToRead--;
    return currentMessageInfoByteBuffer.getByteBuffer().get() & 0xFF;
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
      if (currentMessageInfoByteBuffer.getByteBuffer().remaining() == 0) {
        currentMessageInfoByteBuffer = messageInfoByteBufferIterator.next();
      }
      int sizeToReadFromCurrentBuffer =
          Math.min(currentMessageInfoByteBuffer.getByteBuffer().remaining(), sizeYetToRead);
      currentMessageInfoByteBuffer.getByteBuffer().get(bytes, offset, sizeToReadFromCurrentBuffer);
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
    if (currentMessageInfoByteBuffer.getAbsoluteStartOffset() + currentMessageInfoByteBuffer.getByteBuffer().position()
        - mark > readLimit) {
      throw new IOException("Read limit exceeded before reset invoked.");
    }
    // walk until you reach the marked bytebuffer
    Iterator<MessageInfoByteBuffer> tempIterator = messageInfoByteBufferList.iterator();
    while (tempIterator.hasNext()) {
      MessageInfoByteBuffer messageInfoByteBuffer = tempIterator.next();
      if (messageInfoByteBuffer.getAbsoluteStartOffset() + messageInfoByteBuffer.getMsgInfo().getSize() > mark) {
        messageInfoByteBuffer.getByteBuffer().reset();
        break;
      }
    }
    // reset/flip all bytebuffers until current bytebuffer where reset is called
    while (tempIterator.hasNext()) {
      MessageInfoByteBuffer messageInfoByteBuffer = tempIterator.next();
      if (messageInfoByteBuffer.getAbsoluteStartOffset() == currentMessageInfoByteBuffer.getAbsoluteStartOffset()) {
        messageInfoByteBuffer.getByteBuffer().flip();
        break;
      } else {
        messageInfoByteBuffer.getByteBuffer().flip();
      }
    }

    sizeLeftToRead = validSize;
    messageInfoByteBufferIterator = messageInfoByteBufferList.iterator();
    // make currentMessageInfoByteBuffer refer to the msginfobytebuffer when mark was called
    while (messageInfoByteBufferIterator.hasNext()) {
      currentMessageInfoByteBuffer = messageInfoByteBufferIterator.next();
      if (currentMessageInfoByteBuffer.getAbsoluteStartOffset() + currentMessageInfoByteBuffer.getMsgInfo().getSize()
          > mark) {
        break;
      } else {
        sizeLeftToRead -= currentMessageInfoByteBuffer.getMsgInfo().getSize();
      }
    }
  }

  @Override
  public synchronized void mark(int readLimit) {
    this.mark =
        currentMessageInfoByteBuffer.getAbsoluteStartOffset() + currentMessageInfoByteBuffer.getByteBuffer().position();
    this.readLimit = readLimit;
    currentMessageInfoByteBuffer.getByteBuffer().mark();
  }

  @Override
  public boolean markSupported() {
    return true;
  }

  public ValidMessageFormatInputStream duplicate() {
    return new ValidMessageFormatInputStream(messageInfoByteBufferList, messageInfoList, validSize, storeKeyFactory,
        logger, validateMessageStream);
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
    }
    return isValid;
  }

  class MessageInfoByteBuffer {
    private MessageInfo msgInfo;
    private ByteBuffer byteBuffer;
    private int absoluteStartOffset;

    public MessageInfoByteBuffer(MessageInfo msgInfo, ByteBuffer byteBuffer, int absoluteStartOffset) {
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

