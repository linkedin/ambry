package com.github.ambry.shared;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.store.FindTokenFactory;
import com.github.ambry.store.FindToken;
import com.github.ambry.utils.Utils;
import com.github.ambry.store.MessageInfo;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.List;


/**
 * The response for a ReplicaMetadataRequest. This returns the new entries found
 * and the new token that could be used for future searches
 */
public class ReplicaMetadataResponse extends Response {

  private FindToken token;
  private MessageInfoListSerde messageInfoListSerDe;
  private final int messageInfoListSize;

  public ReplicaMetadataResponse(int correlationId, String clientId, ServerErrorCode error, FindToken token,
      List<MessageInfo> messageInfoList) {
    super(RequestOrResponseType.ReplicaMetadataResponse, Request_Response_Version, correlationId, clientId, error);
    if (token == null || messageInfoList == null) {
      throw new IllegalArgumentException("Invalid token or message info list");
    }
    this.token = token;
    this.messageInfoListSerDe = new MessageInfoListSerde(messageInfoList);
    this.messageInfoListSize = messageInfoListSerDe.getMessageInfoListSize();
  }

  public ReplicaMetadataResponse(int correlationId, String clientId, ServerErrorCode error) {
    super(RequestOrResponseType.ReplicaMetadataResponse, Request_Response_Version, correlationId, clientId, error);
    token = null;
    this.messageInfoListSerDe = new MessageInfoListSerde(null);
    this.messageInfoListSize = messageInfoListSerDe.getMessageInfoListSize();
  }

  public List<MessageInfo> getMessageInfoList() {
    return messageInfoListSerDe.getMessageInfoList();
  }

  public FindToken getFindToken() {
    return token;
  }

  public static ReplicaMetadataResponse readFrom(DataInputStream stream, FindTokenFactory factory,
      ClusterMap clusterMap)
      throws IOException {
    RequestOrResponseType type = RequestOrResponseType.values()[stream.readShort()];
    if (type != RequestOrResponseType.ReplicaMetadataResponse) {
      throw new IllegalArgumentException("The type of request response is not compatible");
    }
    Short versionId = stream.readShort();
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    ServerErrorCode error = ServerErrorCode.values()[stream.readShort()];
    FindToken token = factory.getFindToken(stream);
    List<MessageInfo> messageInfoList = MessageInfoListSerde.deserializeMessageInfoList(stream, clusterMap);

    // ignore version for now
    return new ReplicaMetadataResponse(correlationId, clientId, error, token, messageInfoList);
  }

  @Override
  public void writeTo(WritableByteChannel channel)
      throws IOException {
    if (bufferToSend == null) {
      bufferToSend = ByteBuffer.allocate((int) sizeInBytes());
      writeHeader();
      if (token != null) {
        bufferToSend.put(token.toBytes());
        messageInfoListSerDe.serializeMessageInfoList(bufferToSend);
      }
      bufferToSend.flip();
    }
    if (bufferToSend.remaining() > 0) {
      channel.write(bufferToSend);
    }
  }

  @Override
  public boolean isSendComplete() {
    return bufferToSend != null && bufferToSend.remaining() == 0;
  }

  @Override
  public long sizeInBytes() {
    return super.sizeInBytes() + messageInfoListSize + (token == null ? 0 : token.toBytes().length);
  }
}
