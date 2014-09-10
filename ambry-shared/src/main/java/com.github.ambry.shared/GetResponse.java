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
import java.util.List;


/**
 * Response to GetRequest to fetch data
 */
public class GetResponse extends Response {

  private Send toSend = null;
  private InputStream stream = null;
  private final int messageInfoListSize;
  private final MessageInfoListSerde messageInfoListSerDe;

  public GetResponse(int correlationId, String clientId, List<MessageInfo> messageInfoList, Send send,
      ServerErrorCode error) {
    super(RequestOrResponseType.GetResponse, Request_Response_Version, correlationId, clientId, error);
    this.messageInfoListSerDe = new MessageInfoListSerde(messageInfoList);
    this.messageInfoListSize = messageInfoListSerDe.getMessageInfoListSize();
    this.toSend = send;
  }

  public GetResponse(int correlationId, String clientId, List<MessageInfo> messageInfoList, InputStream stream,
      ServerErrorCode error) {
    super(RequestOrResponseType.GetResponse, Request_Response_Version, correlationId, clientId, error);
    this.messageInfoListSerDe = new MessageInfoListSerde(messageInfoList);
    this.messageInfoListSize = messageInfoListSerDe.getMessageInfoListSize();
    this.stream = stream;
  }

  public GetResponse(int correlationId, String clientId, ServerErrorCode error) {
    super(RequestOrResponseType.GetResponse, Request_Response_Version, correlationId, clientId, error);
    this.messageInfoListSerDe = new MessageInfoListSerde(null);
    this.messageInfoListSize = messageInfoListSerDe.getMessageInfoListSize();
  }

  public InputStream getInputStream() {
    return stream;
  }

  public List<MessageInfo> getMessageInfoList() {
    return messageInfoListSerDe.getMessageInfoList();
  }

  public static GetResponse readFrom(DataInputStream stream, ClusterMap map)
      throws IOException {
    short typeval = stream.readShort();
    RequestOrResponseType type = RequestOrResponseType.values()[typeval];
    if (type != RequestOrResponseType.GetResponse) {
      throw new IllegalArgumentException("The type of request response is not compatible");
    }
    Short versionId = stream.readShort();
    // ignore version for now
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    ServerErrorCode error = ServerErrorCode.values()[stream.readShort()];
    List<MessageInfo> messageInfoList = MessageInfoListSerde.deserializeMessageInfoList(stream, map);
    if (error != ServerErrorCode.No_Error) {
      return new GetResponse(correlationId, clientId, error);
    } else {
      return new GetResponse(correlationId, clientId, messageInfoList, stream, error);
    }
  }

  @Override
  public void writeTo(WritableByteChannel channel)
      throws IOException {
    if (bufferToSend == null) {
      bufferToSend = ByteBuffer.allocate((int) super.sizeInBytes() + messageInfoListSize);
      writeHeader();
      messageInfoListSerDe.serializeMessageInfoList(bufferToSend);
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

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("GetResponse[");
    sb.append("ServerErrorCode=").append(getError());
    sb.append("]");
    return sb.toString();
  }
}
