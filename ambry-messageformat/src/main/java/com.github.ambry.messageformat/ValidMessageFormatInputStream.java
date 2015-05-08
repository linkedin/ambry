package com.github.ambry.messageformat;

import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.ByteBufferInputStream;
import java.io.ByteArrayInputStream;
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
 * Class represents Inputstream containing blobs in its entirety(one complete message stream with header, blobId,
 * (blob properties + user metadata + blob) / delete record.
 * Blobs are skipped based on certain criteria. For now Corrupt blobs are skipped when read from the stream
 * This class is similar to ByteBufferInputStream which MessageFormatWriteSetUses, just that some blobs are skipped
 * during read
 */
public class ValidMessageFormatInputStream extends InputStream {
  private ByteBuffer byteBuffer;
  private int mark;
  private int readLimit;
  private int validSize;
  private int position;
  private int sizeLeftToRead;
  private int validMessageInfoCount;
  private List<MessageInfoStatus> msgInfoStatusList;
  private Iterator<MessageInfoStatus> iterator;
  private MessageInfoStatus currentMsgInfoStatus;
  private Logger logger;
  private StoreKeyFactory storeKeyFactory;
  private List<MessageInfo> messageInfoList;

  public ValidMessageFormatInputStream(ByteBuffer byteBuffer, List<MessageInfoStatus> msgInfoStatusList, int validSize,
      StoreKeyFactory storeKeyFactory, Logger logger) {
    this.byteBuffer = byteBuffer;
    this.mark = -1;
    this.readLimit = -1;
    this.msgInfoStatusList = msgInfoStatusList;
    this.iterator = msgInfoStatusList.iterator();
    this.storeKeyFactory = storeKeyFactory;
    this.validSize = validSize;
    this.sizeLeftToRead = validSize;
    this.position = 0;
    this.logger = logger;
  }

  /**
   * @param stream The stream from which bytes need to be read. If the underlying stream is SocketInputStream, it needs
   *               to be blocking
   * @param messageInfoList List of MessageInfo which contains details about the messages in the stream
   * @param storeKeyFactory factory which is used to read the key from the stream
   * @param logger used for logging
   * @throws java.io.IOException
   */
  public ValidMessageFormatInputStream(InputStream stream, List<MessageInfo> messageInfoList,
      StoreKeyFactory storeKeyFactory, Logger logger)
      throws IOException {
    int size = 0;
    for (MessageInfo info : messageInfoList) {
      size += info.getSize();
    }
    this.messageInfoList = messageInfoList;
    this.storeKeyFactory = storeKeyFactory;
    // read the entire stream to bytebuffer
    if (messageInfoList.size() == 0) {
      position = sizeLeftToRead = validSize = 0;
      this.mark = -1;
      this.readLimit = -1;
      return;
    }
    this.byteBuffer = ByteBuffer.allocate(size);
    this.logger = logger;
    int read = 0;
    ReadableByteChannel readableByteChannel = Channels.newChannel(stream);
    while (read < size) {
      int sizeRead = readableByteChannel.read(byteBuffer);
      if (sizeRead == 0 || sizeRead == -1) {
        throw new IOException("Total size read " + read + " is less than the size to be read " + size);
      }
      read += sizeRead;
    }
    byteBuffer.flip();
    // validate message stream and generate metadata about each blob
    validateMessageStream();
  }

  private void validateMessageStream()
      throws IOException {
    msgInfoStatusList = new ArrayList<MessageInfoStatus>(messageInfoList.size());
    int currentOffset = 0;
    byteBuffer.mark();
    ByteBufferInputStream byteBufferInputStream = new ByteBufferInputStream(byteBuffer);
    for (MessageInfo messageInfo : messageInfoList) {
      byte[] bytes = new byte[(int) messageInfo.getSize()];
      byteBuffer.position(currentOffset);
      byteBuffer.get(bytes, 0, (int) messageInfo.getSize());
      ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
      boolean isValid = isValid(byteArrayInputStream, messageInfo.getSize(), currentOffset, storeKeyFactory);
      MessageInfoStatus messageInfoStatus = new MessageInfoStatus(messageInfo, isValid, currentOffset);
      msgInfoStatusList.add(messageInfoStatus);
      if (isValid) {
        validMessageInfoCount++;
        validSize += messageInfo.getSize();
      } else {
        logger.error("Corrupt blob reported for blob with messageInfo " + messageInfo);
      }
      currentOffset += messageInfo.getSize();
    }
    sizeLeftToRead = validSize;
    byteBuffer.flip();
    position = 0;
    this.mark = -1;
    this.readLimit = -1;
    iterator = msgInfoStatusList.iterator();
    this.currentMsgInfoStatus = iterator.next();
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
    if (!byteBuffer.hasRemaining() || sizeLeftToRead == 0) {
      return -1;
    }
    if ((currentMsgInfoStatus.getStartOffset() + currentMsgInfoStatus.getMsgInfo().getSize() == (position))) {
      iterateToNextValidMsg();
      byteBuffer.position(position);
    }
    position++;
    sizeLeftToRead--;
    return byteBuffer.get() & 0xFF;
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
    int sizeRead = 0;
    while (sizeRead < count) {
      int currentMsgSizeYetToBeRead =
          (int) (currentMsgInfoStatus.getStartOffset() + currentMsgInfoStatus.getMsgInfo().getSize() - position);
      if (currentMsgSizeYetToBeRead == 0) {
        iterateToNextValidMsg();
        byteBuffer.position(position);
        currentMsgSizeYetToBeRead =
            (int) (currentMsgInfoStatus.getStartOffset() + currentMsgInfoStatus.getMsgInfo().getSize() - position);
      }
      if (sizeRead + currentMsgSizeYetToBeRead < count) { // current msg has less bytes than required
        readBytesFromBuffer(bytes, offset, currentMsgSizeYetToBeRead);
        sizeRead += currentMsgSizeYetToBeRead;
        offset += currentMsgSizeYetToBeRead;
        iterateToNextValidMsg();
        byteBuffer.position(position);
      } else {
        //current msg has more byes than required
        readBytesFromBuffer(bytes, offset, count - sizeRead);
        offset += count - sizeRead;
        sizeRead += (count - sizeRead);
        break;
      }
    }
    return sizeRead;
  }

  private void iterateToNextValidMsg()
      throws IllegalArgumentException {
    currentMsgInfoStatus = null;
    do {
      currentMsgInfoStatus = iterator.next();
    } while (!currentMsgInfoStatus.isValid());
    position = currentMsgInfoStatus.getStartOffset();
  }

  private void readBytesFromBuffer(byte[] bytes, int offset, int length) {
    byteBuffer.get(bytes, offset, length);
    position = byteBuffer.position();
    sizeLeftToRead -= length;
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
    if (byteBuffer.position() - mark > readLimit) {
      throw new IOException("Read limit exceeded before reset invoked.");
    }
    position = 0;
    sizeLeftToRead = validSize;
    iterator = msgInfoStatusList.iterator();
    currentMsgInfoStatus = iterator.next();
    byteBuffer.reset();
  }

  @Override
  public synchronized void mark(int readLimit) {
    this.mark = byteBuffer.position();
    this.readLimit = readLimit;
    byteBuffer.mark();
  }

  @Override
  public boolean markSupported() {
    return true;
  }

  public ValidMessageFormatInputStream duplicate() {
    return new ValidMessageFormatInputStream(byteBuffer.duplicate(), msgInfoStatusList, validSize, storeKeyFactory,
        logger);
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

  class MessageInfoStatus {
    private MessageInfo msgInfo;
    private boolean isValid;
    private int startOffset;

    public MessageInfoStatus(MessageInfo msgInfo, boolean isValid, int startOffset) {
      this.msgInfo = msgInfo;
      this.isValid = isValid;
      this.startOffset = startOffset;
    }

    MessageInfo getMsgInfo() {
      return msgInfo;
    }

    boolean isValid() {
      return isValid;
    }

    int getStartOffset() {
      return startOffset;
    }
  }
}

