package com.github.ambry.messageformat;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreKeyFactory;
import com.github.ambry.utils.Utils;
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
 * Class represents Inputstream containing blobs in its entirety.
 * Corrupt blobs are skipped when read from the stream
 */
public class MessageFormatByteBufferInputStream extends InputStream {
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
  private ClusterMap clusterMap;
  private Logger logger;

  public MessageFormatByteBufferInputStream(ByteBuffer byteBuffer, List<MessageInfoStatus> msgInfoStatusList,
      int validSize, ClusterMap clusterMap, Logger logger) {
    this.byteBuffer = byteBuffer;
    this.mark = -1;
    this.readLimit = -1;
    this.msgInfoStatusList = msgInfoStatusList;
    this.iterator = msgInfoStatusList.iterator();
    this.clusterMap = clusterMap;
    this.validSize = validSize;
    this.sizeLeftToRead = validSize;
    this.position = 0;
    this.logger = logger;
  }

  /**
   * Reads 'size' amount of bytes from the stream into the buffer.
   * @param stream The stream from which bytes need to be read. If the underlying stream is SocketInputStream, it needs
   *               to be blocking
   * @param size The size that needs to be read from the stream
   * @throws java.io.IOException
   */
  public MessageFormatByteBufferInputStream(InputStream stream, int size, List<MessageInfo> messageInfoList,
      ClusterMap clusterMap, Logger logger)
      throws IOException {
    if (messageInfoList.size() < 1) {
      // TODO:
    }
    this.clusterMap = clusterMap;
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
    init(messageInfoList);
  }

  private void init(List<MessageInfo> messageInfoList)
      throws IOException {

    StoreKeyFactory storeKeyFactory = null;
    try {
      storeKeyFactory = Utils.getObj("com.github.ambry.commons.BlobIdFactory", clusterMap);
    } catch (Exception e) {
      logger.error("Error creating StoreKeyFactory ");
      throw new IOException("Error creating StoreKeyFactory " + e);
    }

    msgInfoStatusList = new ArrayList<MessageInfoStatus>(messageInfoList.size());
    int currentOffset = 0;
    byteBuffer.mark();
    for (MessageInfo messageInfo : messageInfoList) {
      byte[] bytes = new byte[(int) messageInfo.getSize()];
      byteBuffer.position(currentOffset);
      byteBuffer.get(bytes, 0, (int) messageInfo.getSize());
      ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
      boolean isCorrupt = isCorrupt(byteArrayInputStream, messageInfo.getSize(), currentOffset, storeKeyFactory);
      MessageInfoStatus messageInfoStatus = new MessageInfoStatus(messageInfo, isCorrupt, currentOffset);
      msgInfoStatusList.add(messageInfoStatus);
      if (!isCorrupt) {
        validMessageInfoCount++;
        validSize += messageInfo.getSize();
      }
      else{
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

  @Override
  public int read()
      throws IOException {
    if (!byteBuffer.hasRemaining() || sizeLeftToRead == 0) {
      return -1;
    }
    if ((currentMsgInfoStatus.getStartOffset() + currentMsgInfoStatus.getMsgInfo().getSize() == (position))) {
      iterateToNextNonCorruptMsg();
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
      throw new NullPointerException();
    } else if (offset < 0 || length < 0 || length > bytes.length - offset) {
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
      if (sizeRead + currentMsgSizeYetToBeRead < count) { // current msg has less bytes than required
        readBytesFromBuffer(bytes, offset, currentMsgSizeYetToBeRead);
        sizeRead += currentMsgSizeYetToBeRead;
        offset += currentMsgSizeYetToBeRead;
        iterateToNextNonCorruptMsg();
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

  private void iterateToNextMessage() {
    if (iterator.hasNext()) {
      currentMsgInfoStatus = iterator.next();
    } else {
      throw new IllegalStateException("No more messages to iterate ");
    }
  }

  private void iterateToNextNonCorruptMsg()
      throws IllegalArgumentException {
    if (sizeLeftToRead == 0) {
      throw new IllegalArgumentException("No more bytes to read ");
    } else if (!iterator.hasNext()) {
      throw new IllegalArgumentException("No more message info to iterate ");
    }
    currentMsgInfoStatus = iterator.next();
    position = currentMsgInfoStatus.getStartOffset();
    while (currentMsgInfoStatus.isCorrupt()) {
      iterateToNextMessage();
      position = currentMsgInfoStatus.getStartOffset();
    }
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
    iterateToNextMessage();
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

  public MessageFormatByteBufferInputStream duplicate() {
    return new MessageFormatByteBufferInputStream(byteBuffer.duplicate(), msgInfoStatusList, validSize, clusterMap,
        logger);
  }

  public int getTotalValidMessageInfos(){
    return validMessageInfoCount;
  }

  /**
   * To check if the Inputstream contains an entire blob in proper message format
   * @param inputStream InputStream for which the check is to be done
   * @param size total size of the message expected
   * @param currentOffset Current offset at which the stream was read from the buffer
   * @param storeKeyFactory StoreKeyFactory used to get store key
   * @return true if message was corrupt and false otherwise
   * @throws IOException
   */
  private boolean isCorrupt(InputStream inputStream, long size, int currentOffset, StoreKeyFactory storeKeyFactory)
      throws IOException {
    String messageheader = null;
    String blobId = null;
    String blobProperty = null;
    String usermetadata = null;
    String blobOutput = null;
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
        messageheader = " Header - version " + header.getVersion() + " messagesize " + header.getMessageSize() +
            " currentOffset " + currentOffset +
            " blobPropertiesRelativeOffset " + header.getBlobPropertiesRecordRelativeOffset() +
            " userMetadataRelativeOffset " + header.getUserMetadataRecordRelativeOffset() +
            " dataRelativeOffset " + header.getBlobRecordRelativeOffset() +
            " crc " + header.getCrc();

        StoreKey storeKey = storeKeyFactory.getStoreKey(new DataInputStream(inputStream));
        blobId = "Id - " + storeKey.getID();
        boolean isDeleted = false;
        if (header.getBlobPropertiesRecordRelativeOffset()
            != MessageFormatRecord.Message_Header_Invalid_Relative_Offset) {
          BlobProperties props = MessageFormatRecord.deserializeBlobProperties(inputStream);
          blobProperty = " Blob properties - blobSize  " + props.getBlobSize() +
              " serviceId " + props.getServiceId();
          ByteBuffer metadata = MessageFormatRecord.deserializeUserMetadata(inputStream);
          usermetadata = " Metadata - size " + metadata.capacity();
          BlobOutput output = MessageFormatRecord.deserializeBlob(inputStream);
          blobOutput = "Blob - size " + output.getSize();
        } else {
          throw new IllegalStateException("Message cannot not be a deleted record ");
        }
        if (!isDeleted) {
          logger.trace(messageheader + "\n " + blobId + "\n" + blobProperty + "\n" + usermetadata + "\n" + blobOutput);
          return false;
        }
        if (inputStream.available() - availableBeforeParsing != size) {
          logger.error(
              "Parsed message size " + (inputStream.available() - availableBeforeParsing) + " is not equivalent " +
                  "to the size in message info " + size);
          return false;
        }
        logger.trace(
            "Message successfully read : " + messageheader + " " + blobId + " " + blobProperty + " " + usermetadata
                + " " + blobOutput);
      } else {
        throw new IllegalStateException("Header version not supported " + version);
      }
    } catch (IllegalArgumentException e) {
      logger.error("Illegal arg exception thrown at " + currentOffset + " and exception: " + e);
    } catch (MessageFormatException e) {
      logger.error("MessageFormat exception thrown at " + currentOffset + " and exception: " + e);
    } catch (EOFException e) {
      logger.error("EOFException thrown at " + currentOffset);
      e.printStackTrace();
      throw e;
    }
    return true;
  }

  class MessageInfoStatus {
    MessageInfo msgInfo;
    boolean isCorrupt;
    private int startOffset;

    public MessageInfoStatus(MessageInfo msgInfo, boolean isCorrupt, int startOffset) {
      this.msgInfo = msgInfo;
      this.isCorrupt = isCorrupt;
      this.startOffset = startOffset;
    }

    MessageInfo getMsgInfo() {
      return msgInfo;
    }

    boolean isCorrupt() {
      return isCorrupt;
    }

    int getStartOffset() {
      return startOffset;
    }
  }
}

