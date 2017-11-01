/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.router;

import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.messageformat.MessageMetadata;
import com.github.ambry.network.BoundedByteBufferReceive;
import com.github.ambry.network.ByteBufferSend;
import com.github.ambry.network.Send;
import com.github.ambry.protocol.DeleteRequest;
import com.github.ambry.protocol.DeleteResponse;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PartitionRequestInfo;
import com.github.ambry.protocol.PartitionResponseInfo;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.PutResponse;
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.protocol.RequestOrResponseType;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.ByteBufferChannel;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.Crc32;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import static com.github.ambry.messageformat.MessageFormatRecord.*;


/**
 * A class that mocks the server (data node) and provides methods for sending requests and setting error states.
 */
class MockServer {
  private ServerErrorCode hardError = null;
  private LinkedList<ServerErrorCode> serverErrors = new LinkedList<ServerErrorCode>();
  private final Map<String, StoredBlob> blobs = new ConcurrentHashMap<>();
  private boolean shouldRespond = true;
  private short blobFormatVersion = MessageFormatRecord.Blob_Version_V2;
  private boolean getErrorOnDataBlobOnly = false;
  private final ClusterMap clusterMap;
  private final String dataCenter;
  private final ConcurrentHashMap<RequestOrResponseType, LongAdder> requestCounts = new ConcurrentHashMap<>();

  MockServer(ClusterMap clusterMap, String dataCenter) {
    this.clusterMap = clusterMap;
    this.dataCenter = dataCenter;
  }

  /**
   * Take in a request in the form of {@link Send} and return a response in the form of a
   * {@link BoundedByteBufferReceive}.
   * @param send the request.
   * @return the response.
   * @throws IOException if there was an error in interpreting the request.
   */
  public BoundedByteBufferReceive send(Send send) throws IOException {
    if (!shouldRespond) {
      return null;
    }
    ServerErrorCode serverError =
        hardError != null ? hardError : serverErrors.size() > 0 ? serverErrors.poll() : ServerErrorCode.No_Error;
    RequestOrResponseType type = ((RequestOrResponse) send).getRequestType();
    RequestOrResponse response;
    requestCounts.computeIfAbsent(type, k -> new LongAdder()).increment();
    switch (type) {
      case PutRequest:
        response = makePutResponse((PutRequest) send, serverError);
        break;
      case GetRequest:
        response = makeGetResponse((GetRequest) send, serverError);
        break;
      case DeleteRequest:
        response = makeDeleteResponse((DeleteRequest) send, serverError);
        break;
      default:
        throw new IOException("Unknown request type received");
    }
    ByteBufferChannel channel = new ByteBufferChannel(ByteBuffer.allocate((int) response.sizeInBytes()));
    response.writeTo(channel);
    ByteBuffer payload = channel.getBuffer();
    payload.flip();
    BoundedByteBufferReceive boundedByteBufferReceive = new BoundedByteBufferReceive();
    boundedByteBufferReceive.readFrom(Channels.newChannel(new ByteBufferInputStream(payload)));
    return boundedByteBufferReceive;
  }

  /**
   * Make a {@link PutResponse} for the given {@link PutRequest} for which the given {@link ServerErrorCode} was
   * encountered.
   * @param putRequest the {@link PutRequest} for which the response is being constructed.
   * @param putError the {@link ServerErrorCode} that was encountered.
   * @return the created {@link PutResponse}
   * @throws IOException if there was an error constructing the response.
   */
  PutResponse makePutResponse(PutRequest putRequest, ServerErrorCode putError) throws IOException {
    if (putError == ServerErrorCode.No_Error) {
      updateBlobMap(putRequest);
    }
    return new PutResponse(putRequest.getCorrelationId(), putRequest.getClientId(), putError);
  }

  /**
   * Make a {@link GetResponse} for the given {@link GetRequest} for which the given {@link ServerErrorCode} was
   * encountered. The request could be for BlobInfo or for Blob (the only two options that the router would request
   * for).
   * @param getRequest the {@link GetRequest} for which the response is being constructed.
   * @param getError the {@link ServerErrorCode} that was encountered.
   * @return the constructed {@link GetResponse}
   * @throws IOException if there was an error constructing the response.
   */
  GetResponse makeGetResponse(GetRequest getRequest, ServerErrorCode getError) throws IOException {
    GetResponse getResponse;
    if (getError == ServerErrorCode.No_Error) {
      List<PartitionRequestInfo> infos = getRequest.getPartitionInfoList();
      if (infos.size() != 1 || infos.get(0).getBlobIds().size() != 1) {
        getError = ServerErrorCode.Unknown_Error;
      }
    }

    ServerErrorCode serverError;
    ServerErrorCode partitionError;
    boolean isDataBlob = false;
    try {
      String id = getRequest.getPartitionInfoList().get(0).getBlobIds().get(0).getID();
      isDataBlob = blobs.get(id).type == BlobType.DataBlob;
    } catch (Exception ignored) {
    }

    if (!getErrorOnDataBlobOnly || isDataBlob) {
      // getError could be at the server level or the partition level. For partition level errors,
      // set it in the partitionResponseInfo
      if (getError == ServerErrorCode.No_Error || getError == ServerErrorCode.Blob_Expired
          || getError == ServerErrorCode.Blob_Deleted || getError == ServerErrorCode.Blob_Not_Found) {
        partitionError = getError;
        serverError = ServerErrorCode.No_Error;
      } else {
        serverError = getError;
        // does not matter - this will not be checked if serverError is not No_Error.
        partitionError = ServerErrorCode.No_Error;
      }
    } else {
      serverError = ServerErrorCode.No_Error;
      partitionError = ServerErrorCode.No_Error;
    }

    if (serverError == ServerErrorCode.No_Error) {
      int byteBufferSize;
      ByteBuffer byteBuffer;
      StoreKey key = getRequest.getPartitionInfoList().get(0).getBlobIds().get(0);
      short accountId = Account.UNKNOWN_ACCOUNT_ID;
      short containerId = Container.UNKNOWN_CONTAINER_ID;
      long operationTimeMs = Utils.Infinite_Time;
      StoredBlob blob = blobs.get(key.getID());
      ServerErrorCode processedError = errorForGet(blob, getRequest);
      MessageMetadata msgMetadata = null;
      if (processedError == ServerErrorCode.No_Error) {
        ByteBuffer buf = blobs.get(key.getID()).serializedSentPutRequest.duplicate();
        // read off the size
        buf.getLong();
        // read off the type.
        buf.getShort();
        PutRequest.ReceivedPutRequest originalBlobPutReq =
            PutRequest.readFrom(new DataInputStream(new ByteBufferInputStream(buf)), clusterMap);
        switch (getRequest.getMessageFormatFlag()) {
          case BlobInfo:
            BlobProperties blobProperties = originalBlobPutReq.getBlobProperties();
            accountId = blobProperties.getAccountId();
            containerId = blobProperties.getContainerId();
            operationTimeMs = blobProperties.getCreationTimeInMs();
            ByteBuffer userMetadata = originalBlobPutReq.getUsermetadata();
            byteBufferSize = MessageFormatRecord.BlobProperties_Format_V1.getBlobPropertiesRecordSize(blobProperties)
                + MessageFormatRecord.UserMetadata_Format_V1.getUserMetadataSize(userMetadata);
            byteBuffer = ByteBuffer.allocate(byteBufferSize);
            if (originalBlobPutReq.getBlobEncryptionKey() != null) {
              msgMetadata = new MessageMetadata(originalBlobPutReq.getBlobEncryptionKey().duplicate());
            }
            MessageFormatRecord.BlobProperties_Format_V1.serializeBlobPropertiesRecord(byteBuffer, blobProperties);
            MessageFormatRecord.UserMetadata_Format_V1.serializeUserMetadataRecord(byteBuffer, userMetadata);
            break;
          case Blob:
            switch (blobFormatVersion) {
              case MessageFormatRecord.Blob_Version_V2:
                if (originalBlobPutReq.getBlobEncryptionKey() != null) {
                  msgMetadata = new MessageMetadata(originalBlobPutReq.getBlobEncryptionKey().duplicate());
                }
                byteBufferSize =
                    (int) MessageFormatRecord.Blob_Format_V2.getBlobRecordSize((int) originalBlobPutReq.getBlobSize());
                byteBuffer = ByteBuffer.allocate(byteBufferSize);
                MessageFormatRecord.Blob_Format_V2.serializePartialBlobRecord(byteBuffer,
                    (int) originalBlobPutReq.getBlobSize(), originalBlobPutReq.getBlobType());
                break;
              case MessageFormatRecord.Blob_Version_V1:
                byteBufferSize =
                    (int) MessageFormatRecord.Blob_Format_V1.getBlobRecordSize((int) originalBlobPutReq.getBlobSize());
                byteBuffer = ByteBuffer.allocate(byteBufferSize);
                MessageFormatRecord.Blob_Format_V1.serializePartialBlobRecord(byteBuffer,
                    (int) originalBlobPutReq.getBlobSize());
                break;
              default:
                throw new IllegalStateException("Blob format version " + blobFormatVersion + " not supported.");
            }
            byteBuffer.put(
                Utils.readBytesFromStream(originalBlobPutReq.getBlobStream(), (int) originalBlobPutReq.getBlobSize()));
            Crc32 crc = new Crc32();
            crc.update(byteBuffer.array(), 0, byteBuffer.position());
            byteBuffer.putLong(crc.getValue());
            break;
          case All:
            blobProperties = originalBlobPutReq.getBlobProperties();
            accountId = blobProperties.getAccountId();
            containerId = blobProperties.getContainerId();
            userMetadata = originalBlobPutReq.getUsermetadata();
            operationTimeMs = originalBlobPutReq.getBlobProperties().getCreationTimeInMs();
            int blobHeaderSize = MessageFormatRecord.MessageHeader_Format_V2.getHeaderSize();
            int blobEncryptionRecordSize = originalBlobPutReq.getBlobEncryptionKey() != null
                ? MessageFormatRecord.BlobEncryptionKey_Format_V1.getBlobEncryptionKeyRecordSize(
                originalBlobPutReq.getBlobEncryptionKey().duplicate()) : 0;
            int blobPropertiesSize =
                MessageFormatRecord.BlobProperties_Format_V1.getBlobPropertiesRecordSize(blobProperties);
            int userMetadataSize = MessageFormatRecord.UserMetadata_Format_V1.getUserMetadataSize(userMetadata);
            int blobInfoSize = blobPropertiesSize + userMetadataSize;
            int blobRecordSize;
            switch (blobFormatVersion) {
              case MessageFormatRecord.Blob_Version_V2:
                blobRecordSize =
                    (int) MessageFormatRecord.Blob_Format_V2.getBlobRecordSize((int) originalBlobPutReq.getBlobSize());
                break;
              case MessageFormatRecord.Blob_Version_V1:
                blobRecordSize =
                    (int) MessageFormatRecord.Blob_Format_V1.getBlobRecordSize((int) originalBlobPutReq.getBlobSize());
                break;
              default:
                throw new IllegalStateException("Blob format version " + blobFormatVersion + " not supported.");
            }
            byteBufferSize =
                blobHeaderSize + key.sizeInBytes() + blobEncryptionRecordSize + blobInfoSize + blobRecordSize;
            byteBuffer = ByteBuffer.allocate(byteBufferSize);
            try {
              MessageFormatRecord.MessageHeader_Format_V2.serializeHeader(byteBuffer,
                  blobEncryptionRecordSize + blobInfoSize + blobRecordSize,
                  originalBlobPutReq.getBlobEncryptionKey() == null ? Message_Header_Invalid_Relative_Offset
                      : blobHeaderSize + key.sizeInBytes(),
                  blobHeaderSize + key.sizeInBytes() + blobEncryptionRecordSize, Message_Header_Invalid_Relative_Offset,
                  blobHeaderSize + key.sizeInBytes() + blobEncryptionRecordSize + blobPropertiesSize,
                  blobHeaderSize + key.sizeInBytes() + blobEncryptionRecordSize + blobInfoSize);
            } catch (MessageFormatException e) {
              e.printStackTrace();
            }
            byteBuffer.put(key.toBytes());
            if (originalBlobPutReq.getBlobEncryptionKey() != null) {
              MessageFormatRecord.BlobEncryptionKey_Format_V1.serializeBlobEncryptionKeyRecord(byteBuffer,
                  originalBlobPutReq.getBlobEncryptionKey().duplicate());
              msgMetadata = new MessageMetadata(originalBlobPutReq.getBlobEncryptionKey().duplicate());
            }
            MessageFormatRecord.BlobProperties_Format_V1.serializeBlobPropertiesRecord(byteBuffer, blobProperties);
            MessageFormatRecord.UserMetadata_Format_V1.serializeUserMetadataRecord(byteBuffer, userMetadata);
            int blobRecordStart = byteBuffer.position();
            switch (blobFormatVersion) {
              case MessageFormatRecord.Blob_Version_V2:
                MessageFormatRecord.Blob_Format_V2.serializePartialBlobRecord(byteBuffer,
                    (int) originalBlobPutReq.getBlobSize(), originalBlobPutReq.getBlobType());
                break;
              case MessageFormatRecord.Blob_Version_V1:
                MessageFormatRecord.Blob_Format_V1.serializePartialBlobRecord(byteBuffer,
                    (int) originalBlobPutReq.getBlobSize());
                break;
              default:
                throw new IllegalStateException("Blob format version " + blobFormatVersion + " not supported.");
            }
            byteBuffer.put(
                Utils.readBytesFromStream(originalBlobPutReq.getBlobStream(), (int) originalBlobPutReq.getBlobSize()));
            crc = new Crc32();
            crc.update(byteBuffer.array(), blobRecordStart, blobRecordSize - MessageFormatRecord.Crc_Size);
            byteBuffer.putLong(crc.getValue());
            break;
          default:
            throw new IOException("GetRequest flag is not supported: " + getRequest.getMessageFormatFlag());
        }
      } else if (processedError == ServerErrorCode.Blob_Deleted) {
        if (partitionError == ServerErrorCode.No_Error) {
          partitionError = ServerErrorCode.Blob_Deleted;
        }
        byteBuffer = ByteBuffer.allocate(0);
        byteBufferSize = 0;
      } else if (processedError == ServerErrorCode.Blob_Expired) {
        if (partitionError == ServerErrorCode.No_Error) {
          partitionError = ServerErrorCode.Blob_Expired;
        }
        byteBuffer = ByteBuffer.allocate(0);
        byteBufferSize = 0;
      } else {
        if (partitionError == ServerErrorCode.No_Error) {
          partitionError = ServerErrorCode.Blob_Not_Found;
        }
        byteBuffer = ByteBuffer.allocate(0);
        byteBufferSize = 0;
      }

      byteBuffer.flip();
      ByteBufferSend responseSend = new ByteBufferSend(byteBuffer);
      List<MessageInfo> messageInfoList = new ArrayList<>(1);
      List<MessageMetadata> messageMetadataList = new ArrayList<>(1);
      List<PartitionResponseInfo> partitionResponseInfoList = new ArrayList<PartitionResponseInfo>();
      messageInfoList.add(new MessageInfo(key, byteBufferSize, accountId, containerId, operationTimeMs));
      messageMetadataList.add(msgMetadata);
      PartitionResponseInfo partitionResponseInfo =
          partitionError == ServerErrorCode.No_Error ? new PartitionResponseInfo(
              getRequest.getPartitionInfoList().get(0).getPartition(), messageInfoList, messageMetadataList)
              : new PartitionResponseInfo(getRequest.getPartitionInfoList().get(0).getPartition(), partitionError);
      partitionResponseInfoList.add(partitionResponseInfo);
      getResponse = new GetResponse(getRequest.getCorrelationId(), getRequest.getClientId(), partitionResponseInfoList,
          responseSend, serverError);
    } else {
      getResponse = new GetResponse(getRequest.getCorrelationId(), getRequest.getClientId(),
          new ArrayList<PartitionResponseInfo>(), new ByteBufferSend(ByteBuffer.allocate(0)), serverError);
    }
    return getResponse;
  }

  ServerErrorCode errorForGet(StoredBlob blob, GetRequest getRequest) {
    ServerErrorCode retCode = ServerErrorCode.No_Error;
    if (blob == null) {
      retCode = ServerErrorCode.Blob_Not_Found;
    } else if (blob.isDeleted() && !getRequest.getGetOption().equals(GetOption.Include_All)
        && !getRequest.getGetOption().equals(GetOption.Include_Deleted_Blobs)) {
      retCode = ServerErrorCode.Blob_Deleted;
    } else if (blob.hasExpired() && !getRequest.getGetOption().equals(GetOption.Include_All)
        && !getRequest.getGetOption().equals(GetOption.Include_Expired_Blobs)) {
      retCode = ServerErrorCode.Blob_Expired;
    }
    return retCode;
  }

  /**
   *
   * Make a {@link DeleteResponse} for the given {@link DeleteRequest} for which the given {@link ServerErrorCode} was
   * encountered.
   * @param deleteRequest the {@link DeleteRequest} for which the response is being constructed.
   * @param deleteError the {@link ServerErrorCode} that was encountered.
   * @return the constructed {@link DeleteResponse}
   * @throws IOException if there was an error constructing the response.
   */
  DeleteResponse makeDeleteResponse(DeleteRequest deleteRequest, ServerErrorCode deleteError) throws IOException {
    if (deleteError == ServerErrorCode.No_Error) {
      updateBlobMap(deleteRequest);
    }
    return new DeleteResponse(deleteRequest.getCorrelationId(), deleteRequest.getClientId(), deleteError);
  }

  /**
   * Serialize contents of the PutRequest and update the blob map with the serialized content.
   * @param putRequest the PutRequest
   * @throws IOException if there was an error reading the contents of the given PutRequest.
   */
  private void updateBlobMap(PutRequest putRequest) throws IOException {
    StoredBlob blob = new StoredBlob(putRequest, clusterMap);
    blobs.put(blob.id, blob);
  }

  private void updateBlobMap(DeleteRequest deleteRequest) throws IOException {
    StoredBlob blob = blobs.get(deleteRequest.getBlobId().getID());
    if (blob != null) {
      blob.markAsDeleted();
    }
  }

  /**
   * @return the blobs that were put on this server.
   */
  Map<String, StoredBlob> getBlobs() {
    return blobs;
  }

  /**
   * Return the datacenter name of this server.
   * @return the datacenter name.
   */
  String getDataCenter() {
    return dataCenter;
  }

  /**
   * Set the error for each request from this point onwards that affects subsequent requests sent to this node
   * (until/unless the next set or reset error method is invoked).
   * Each request from the list is used exactly once and in order. So, if the list contains {No_Error, Unknown_Error,
   * Disk_Error}, then the first, second and third requests would receive No_Error,
   * Unknown_Error and Disk_Error respectively. Once the errors are exhausted, the default No_Error is assumed for
   * all further requests until the next call to this method.
   * @param serverErrors the list of errors that affects subsequent PutRequests.
   */
  public void setServerErrors(List<ServerErrorCode> serverErrors) {
    this.serverErrors.clear();
    this.serverErrors.addAll(serverErrors);
  }

  /**
   * Set the error to be set in the responses for all requests from this point onwards (until/unless another set or
   * reset method for errors is invoked).
   * @param serverError the error to set from this point onwards.
   */
  public void setServerErrorForAllRequests(ServerErrorCode serverError) {
    this.hardError = serverError;
  }

  /**
   * Set whether the mock server should set the error code for all blobs or just data blobs on get requests.
   * @param getErrorOnDataBlobOnly {@code true} if the preset error code should be set only on data blobs in a get
   *                               request, {@code false} otherwise
   */
  public void setGetErrorOnDataBlobOnly(boolean getErrorOnDataBlobOnly) {
    this.getErrorOnDataBlobOnly = getErrorOnDataBlobOnly;
  }

  /**
   * Clear the error for subsequent requests. That is all responses from this point onwards will be successful
   * ({@link ServerErrorCode#No_Error}) until/unless another set error method is invoked.
   */
  public void resetServerErrors() {
    this.serverErrors.clear();
    this.hardError = null;
    this.getErrorOnDataBlobOnly = false;
  }

  /**
   * Set whether or not the server would send response back.
   * @param shouldRespond {@code true} if the server responds.
   */
  public void setShouldRespond(boolean shouldRespond) {
    this.shouldRespond = shouldRespond;
  }

  /**
   * Set the blob format version that the server should respond to get requests with.
   * @param blobFormatVersion The blob version to use.
   */
  public void setBlobFormatVersion(short blobFormatVersion) {
    this.blobFormatVersion = blobFormatVersion;
  }

  /**
   * Get the count of requests of the given type.
   * @param type the type of request
   * @return the count of requests this server has received of the given type.
   */
  public int getCount(RequestOrResponseType type) {
    return requestCounts.getOrDefault(type, new LongAdder()).intValue();
  }
}

class StoredBlob {
  final String id;
  final BlobType type;
  final BlobProperties properties;
  final ByteBuffer userMetadata;
  final ByteBuffer blobEncryptionKey;
  final ByteBuffer serializedSentPutRequest;
  final PutRequest.ReceivedPutRequest receivedPutRequest;
  private final long expiresAt;
  private boolean deleted = false;

  StoredBlob(PutRequest putRequest, ClusterMap clusterMap) throws IOException {
    serializedSentPutRequest = ByteBuffer.allocate((int) putRequest.sizeInBytes());
    ByteBufferChannel bufChannel = new ByteBufferChannel(serializedSentPutRequest);
    putRequest.writeTo(bufChannel);
    serializedSentPutRequest.flip();
    DataInputStream receivedStream =
        new DataInputStream(new ByteBufferInputStream(serializedSentPutRequest.duplicate()));
    // read off the size
    receivedStream.readLong();
    // read of the RequestResponse type.
    receivedStream.readShort();
    receivedPutRequest = PutRequest.readFrom(receivedStream, clusterMap);
    id = receivedPutRequest.getBlobId().getID();
    type = receivedPutRequest.getBlobType();
    properties = receivedPutRequest.getBlobProperties();
    userMetadata = receivedPutRequest.getUsermetadata();
    blobEncryptionKey = receivedPutRequest.getBlobEncryptionKey();
    expiresAt = properties.getTimeToLiveInSeconds() == Utils.Infinite_Time ? Utils.Infinite_Time
        : SystemTime.getInstance().milliseconds() + properties.getTimeToLiveInSeconds();
  }

  boolean isDeleted() {
    return deleted;
  }

  boolean hasExpired() {
    return expiresAt != Utils.Infinite_Time && expiresAt <= SystemTime.getInstance().milliseconds();
  }

  void markAsDeleted() {
    deleted = true;
  }
}
