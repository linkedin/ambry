package com.github.ambry.shared;

import com.github.ambry.clustermap.ClusterMap;
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
public class GetResponse extends Response {

  private final List<MessageInfo> messageInfoList;
  private Send toSend = null;
  private InputStream stream = null;
  private final int messageInfoListSize;

  private int getMessageInfoListSize() {
    int listcountSize = 4;
    if(messageInfoList == null) {
      return listcountSize;
    }
    int size = 0;
    int keySize = 2;
    int fieldSize = 8;
    size += listcountSize;
    if (messageInfoList != null) {
      for (MessageInfo messageInfo : messageInfoList) {
        size += keySize;
        size += messageInfo.getStoreKey().sizeInBytes();
        size += fieldSize;
        size += fieldSize;
      }
    }
    return size;
  }

  private static void serializeMessageInfoList(ByteBuffer outputBuffer, List<MessageInfo> messageInfoList) {
    outputBuffer.putInt(messageInfoList == null ? 0 : messageInfoList.size());
    if (messageInfoList != null) {
      for (MessageInfo messageInfo : messageInfoList) {
        outputBuffer.put(messageInfo.getStoreKey().toBytes());
        outputBuffer.putLong(messageInfo.getSize());
        outputBuffer.putLong(messageInfo.getTimeToLiveInMs());
      }
    }
  }

  private static List<MessageInfo> deserializeMessageInfoList(DataInputStream stream,
                                                              ClusterMap map) throws IOException {
    int messageInfoListCount = stream.readInt();
    ArrayList<MessageInfo> messageListInfo = new ArrayList<MessageInfo>(messageInfoListCount);
    for (int i = 0; i < messageInfoListCount; i++) {
      BlobId id = new BlobId(stream, map);
      long size = stream.readLong();
      long ttl = stream.readLong();
      messageListInfo.add(new MessageInfo(id, size, ttl));
    }
    return messageListInfo;
  }

  public GetResponse(int correlationId,
                     String clientId,
                     List<MessageInfo> messageInfoList,
                     Send send,
                     ServerErrorCode error) {
    super(RequestResponseType.GetResponse, Request_Response_Version, correlationId, clientId, error);
    this.messageInfoList = messageInfoList;
    this.messageInfoListSize = getMessageInfoListSize();
    this.toSend = send;
  }

  public GetResponse(int correlationId,
                     String clientId,
                     List<MessageInfo> messageInfoList,
                     InputStream stream,
                     ServerErrorCode error) {
    super(RequestResponseType.GetResponse, Request_Response_Version, correlationId, clientId, error);
    this.messageInfoList = messageInfoList;
    this.messageInfoListSize = getMessageInfoListSize();
    this.stream = stream;
  }

  public GetResponse(int correlationId,
                     String clientId,
                     ServerErrorCode error) {
    super(RequestResponseType.GetResponse, Request_Response_Version, correlationId, clientId, error);
    this.messageInfoList = null;
    this.messageInfoListSize = getMessageInfoListSize();
  }

  public ServerErrorCode getError() {
    return error;
  }

  public InputStream getInputStream() {
    return stream;
  }

  public List<MessageInfo> getMessageInfoList() {
    return messageInfoList;
  }

  public static GetResponse readFrom(DataInputStream stream, ClusterMap map) throws IOException {
    short typeval = stream.readShort();
    RequestResponseType type = RequestResponseType.values()[typeval];
    if (type != RequestResponseType.GetResponse) {
      throw new IllegalArgumentException("The type of request response is not compatible");
    }
    Short versionId = stream.readShort();
    // ignore version for now
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    ServerErrorCode error = ServerErrorCode.values()[stream.readShort()];
    List<MessageInfo> messageInfoList = deserializeMessageInfoList(stream, map);
    if (error != ServerErrorCode.No_Error)
      return new GetResponse(correlationId, clientId, error);
    else
      return new GetResponse(correlationId, clientId, messageInfoList, stream, error);
  }

  @Override
  public void writeTo(WritableByteChannel channel) throws IOException {
    if (bufferToSend == null) {
      bufferToSend = ByteBuffer.allocate((int)super.sizeInBytes() + messageInfoListSize);
      writeHeader();
      serializeMessageInfoList(bufferToSend, messageInfoList);
      bufferToSend.flip();
    }
    if (bufferToSend.remaining() > 0) {
      channel.write(bufferToSend);
    }
    if (bufferToSend.remaining() == 0 && toSend != null && !toSend.isSendComplete()) {
      toSend.writeTo(channel);
    }
  }

  @Override
  public boolean isSendComplete() {
    return (super.isSendComplete()) && (toSend == null || toSend.isSendComplete());
  }

  @Override
  public long sizeInBytes() {
    return super.sizeInBytes() + messageInfoListSize + ((toSend == null) ? 0 : toSend.sizeInBytes());
  }
}
