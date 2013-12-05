package com.github.ambry.shared;

import com.github.ambry.network.Send;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.utils.Utils;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * Response to GetRequest to fetch data
 */
public class GetResponse extends RequestOrResponse {

  private final List<MessageInfo> messageInfoList;
  private Send toSend = null;
  private InputStream stream = null;
  private final int messageInfoListSize;

  private int getMessageInfoListSize() {
    int size = 0;
    int keySize = 2;
    int fieldSize = 8;
    int listcountSize = 4;
    size += listcountSize;
    for (MessageInfo messageInfo : messageInfoList) {
      size += keySize;
      size += messageInfo.getStoreKey().sizeInBytes();
      size += fieldSize;
      size += fieldSize;
    }
    return size;
  }

  private static void serializeMessageInfoList(ByteBuffer outputBuffer, List<MessageInfo> messageInfoList) {
    outputBuffer.putInt(messageInfoList.size());
    for (MessageInfo messageInfo : messageInfoList) {
      outputBuffer.put(messageInfo.getStoreKey().toBytes());
      outputBuffer.putLong(messageInfo.getSize());
      outputBuffer.putLong(messageInfo.getTimeToLiveInMs());
    }
  }

  private static List<MessageInfo> deserializeMessageInfoList(DataInputStream stream) throws IOException {
    int messageInfoListCount = stream.readInt();
    ArrayList<MessageInfo> messageListInfo = new ArrayList<MessageInfo>(messageInfoListCount);
    for (int i = 0; i < messageInfoListCount; i++) {
      BlobId id = new BlobId(stream);
      long size = stream.readLong();
      long ttl = stream.readLong();
      messageListInfo.add(new MessageInfo(id, size, ttl));
    }
    return messageListInfo;
  }

  public GetResponse(int correlationId, String clientId, List<MessageInfo> messageInfoList, Send send) {
    super(RequestResponseType.GetResponse, Request_Response_Version, correlationId, clientId);
    this.messageInfoList = messageInfoList;
    this.messageInfoListSize = getMessageInfoListSize();
    this.toSend = send;
  }

  public GetResponse(int correlationId, String clientId, List<MessageInfo> messageInfoList, InputStream stream) {
    super(RequestResponseType.GetResponse, Request_Response_Version, correlationId, clientId);
    this.messageInfoList = messageInfoList;
    this.messageInfoListSize = getMessageInfoListSize();
    this.stream = stream;
  }

  public InputStream getInputStream() {
    return stream;
  }

  public List<MessageInfo> getMessageInfoList() {
    return messageInfoList;
  }

  public static GetResponse readFrom(DataInputStream stream) throws IOException {
    short typeval = stream.readShort();
    RequestResponseType type = RequestResponseType.values()[typeval];
    if (type != RequestResponseType.GetResponse) {
      throw new IllegalArgumentException("The type of request response is not compatible");
    }
    Short versionId  = stream.readShort();
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    List<MessageInfo> messageInfoList = deserializeMessageInfoList(stream);
    // ignoring version for now
    return new GetResponse(correlationId, clientId, messageInfoList, stream);
  }

  @Override
  public void writeTo(WritableByteChannel channel) throws IOException {
    if (bufferToSend == null) {
      bufferToSend = ByteBuffer.allocate((int) super.sizeInBytes() + messageInfoListSize);
      writeHeader();
      serializeMessageInfoList(bufferToSend, messageInfoList);
      bufferToSend.flip();
    }
    if (bufferToSend.remaining() > 0) {
      channel.write(bufferToSend);
    }
    if (bufferToSend.remaining() == 0 && !toSend.isSendComplete()) {
      toSend.writeTo(channel);
    }
  }

  @Override
  public boolean isSendComplete() {
    return bufferToSend.remaining() == 0 && toSend.isSendComplete();
  }

  @Override
  public long sizeInBytes() {
    return super.sizeInBytes() + messageInfoListSize + toSend.sizeInBytes();
  }
}
