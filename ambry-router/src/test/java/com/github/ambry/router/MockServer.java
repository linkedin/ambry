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
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.BlobType;
import com.github.ambry.messageformat.MessageFormatException;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.messageformat.MessageMetadata;
import com.github.ambry.network.BoundedNettyByteBufReceive;
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
import com.github.ambry.protocol.TtlUpdateRequest;
import com.github.ambry.protocol.TtlUpdateResponse;
import com.github.ambry.protocol.UndeleteRequest;
import com.github.ambry.protocol.UndeleteResponse;
import com.github.ambry.server.ServerErrorCode;
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
import java.util.HashMap;
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
  private final Map<String, ServerErrorCode> errorCodeForBlobs = new HashMap<>();

  MockServer(ClusterMap clusterMap, String dataCenter) {
    this.clusterMap = clusterMap;
    this.dataCenter = dataCenter;
  }

  /**
   * Take in a request in the form of {@link Send} and return a response in the form of a
   * {@link BoundedNettyByteBufReceive}.
   * @param send the request.
   * @return the response.
   * @throws IOException if there was an error in interpreting the request.
   */
  public BoundedNettyByteBufReceive send(Send send) throws IOException {
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
      case TtlUpdateRequest:
        response = makeTtlUpdateResponse((TtlUpdateRequest) send, serverError);
        break;
      case UndeleteRequest:
        response = makeUndeleteResponse((UndeleteRequest) send, serverError);
        break;
      default:
        throw new IOException("Unknown request type received");
    }
    ByteBufferChannel channel = new ByteBufferChannel(ByteBuffer.allocate((int) response.sizeInBytes()));
    response.writeTo(channel);
    ByteBuffer payload = channel.getBuffer();
    payload.flip();
    BoundedNettyByteBufReceive receive = new BoundedNettyByteBufReceive();
    receive.readFrom(Channels.newChannel(new ByteBufferInputStream(payload)));
    return receive;
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
    } else {
      // we have to read the put request out so that the blob in put request can be released.
      new StoredBlob(putRequest, clusterMap);
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
          || getError == ServerErrorCode.Blob_Deleted || getError == ServerErrorCode.Blob_Not_Found
          || getError == ServerErrorCode.Blob_Authorization_Failure || getError == ServerErrorCode.Disk_Unavailable) {
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
      ServerErrorCode processedError = errorForGet(key.getID(), blob, getRequest);
      MessageMetadata msgMetadata = null;
      if (processedError == ServerErrorCode.No_Error) {
        ByteBuffer buf = blobs.get(key.getID()).serializedSentPutRequest.duplicate();
        // read off the size
        buf.getLong();
        // read off the type.
        buf.getShort();
        PutRequest originalBlobPutReq =
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
      } else if (processedError == ServerErrorCode.Blob_Authorization_Failure) {
        if (partitionError == ServerErrorCode.No_Error) {
          partitionError = ServerErrorCode.Blob_Authorization_Failure;
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
      List<MessageInfo> messageInfoList = new ArrayList<>();
      List<MessageMetadata> messageMetadataList = new ArrayList<>();
      List<PartitionResponseInfo> partitionResponseInfoList = new ArrayList<PartitionResponseInfo>();
      if (partitionError == ServerErrorCode.No_Error) {
        messageInfoList.add(
            new MessageInfo(key, byteBufferSize, false, blob.isTtlUpdated(), blob.isUndeleted(), blob.expiresAt, null,
                accountId, containerId, operationTimeMs, blob.lifeVersion));
        messageMetadataList.add(msgMetadata);
      }
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

  private ServerErrorCode errorForGet(String blobId, StoredBlob blob, GetRequest getRequest) {
    ServerErrorCode retCode = errorCodeForBlobs.getOrDefault(blobId, ServerErrorCode.No_Error);
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
   * Make a {@link DeleteResponse} for the given {@link DeleteRequest} for which the given {@link ServerErrorCode} was
   * encountered.
   * @param deleteRequest the {@link DeleteRequest} for which the response is being constructed.
   * @param deleteError the {@link ServerErrorCode} that was encountered.
   * @return the constructed {@link DeleteResponse}
   */
  DeleteResponse makeDeleteResponse(DeleteRequest deleteRequest, ServerErrorCode deleteError) {
    if (deleteError == ServerErrorCode.No_Error) {
      deleteError = errorCodeForBlobs.getOrDefault(deleteRequest.getBlobId().getID(), ServerErrorCode.No_Error);
    }
    if (deleteError == ServerErrorCode.No_Error) {
      updateBlobMap(deleteRequest);
    }
    return new DeleteResponse(deleteRequest.getCorrelationId(), deleteRequest.getClientId(), deleteError);
  }

  /**
   * Make a {@link TtlUpdateResponse} for the given {@link TtlUpdateRequest} for which the given {@link ServerErrorCode}
   * was encountered.
   * @param ttlUpdateRequest the {@link TtlUpdateRequest} for which the response is being constructed.
   * @param ttlUpdateError the {@link ServerErrorCode} that was encountered.
   * @return the constructed {@link TtlUpdateResponse}
   */
  private TtlUpdateResponse makeTtlUpdateResponse(TtlUpdateRequest ttlUpdateRequest, ServerErrorCode ttlUpdateError) {
    if (ttlUpdateError == ServerErrorCode.No_Error) {
      ttlUpdateError = errorCodeForBlobs.getOrDefault(ttlUpdateRequest.getBlobId().getID(), ServerErrorCode.No_Error);
    }
    if (ttlUpdateError == ServerErrorCode.No_Error) {
      ttlUpdateError = updateBlobMap(ttlUpdateRequest);
    }
    return new TtlUpdateResponse(ttlUpdateRequest.getCorrelationId(), ttlUpdateRequest.getClientId(), ttlUpdateError);
  }

  /**
   * Make a {@link UndeleteResponse} for the given {@link UndeleteRequest} for which the given {@link ServerErrorCode} was
   * encountered.
   * @param undeleteRequest the {@link UndeleteRequest} for which the response is being constructed.
   * @param undeleteError the {@link ServerErrorCode} that was encountered.
   * @return the constructed {@link UndeleteResponse}
   */
  private UndeleteResponse makeUndeleteResponse(UndeleteRequest undeleteRequest, ServerErrorCode undeleteError) {
    if (undeleteError == ServerErrorCode.No_Error) {
      undeleteError = errorCodeForBlobs.getOrDefault(undeleteRequest.getBlobId().getID(), ServerErrorCode.No_Error);
    }
    if (undeleteError == ServerErrorCode.No_Error) {
      undeleteError = updateBlobMap(undeleteRequest);
    }
    if (undeleteError == ServerErrorCode.No_Error) {
      short lifeVersion = blobs.get(undeleteRequest.getBlobId().getID()).lifeVersion;
      return new UndeleteResponse(undeleteRequest.getCorrelationId(), undeleteRequest.getClientId(), lifeVersion);
    } else {
      return new UndeleteResponse(undeleteRequest.getCorrelationId(), undeleteRequest.getClientId(), undeleteError);
    }
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

  /**
   * Updates the map of the blobs to indicate that the blob in question has been deleted
   * @param deleteRequest the {@link DeleteRequest} that describes what needs to be done
   */
  private void updateBlobMap(DeleteRequest deleteRequest) {
    StoredBlob blob = blobs.get(deleteRequest.getBlobId().getID());
    if (blob != null) {
      blob.markAsDeleted(deleteRequest.getDeletionTimeInMs());
    }
  }

  /**
   * Updates the blob map based on the {@code ttlUpdateRequest}.
   * @param ttlUpdateRequest the {@link TtlUpdateRequest} that needs to be processed
   */
  private ServerErrorCode updateBlobMap(TtlUpdateRequest ttlUpdateRequest) {
    ServerErrorCode errorCode = ServerErrorCode.No_Error;
    StoredBlob blob = blobs.get(ttlUpdateRequest.getBlobId().getID());
    if (blob != null && !blob.isDeleted() && !blob.hasExpired() && !blob.isTtlUpdated()) {
      blob.updateExpiry(ttlUpdateRequest.getExpiresAtMs());
    } else if (blob == null) {
      errorCode = ServerErrorCode.Blob_Not_Found;
    } else if (blob.isDeleted()) {
      errorCode = ServerErrorCode.Blob_Deleted;
    } else if (blob.hasExpired()) {
      errorCode = ServerErrorCode.Blob_Expired;
    } else if (blob.isTtlUpdated()) {
      errorCode = ServerErrorCode.Blob_Already_Updated;
    } else {
      throw new IllegalStateException("Could not recognize blob state");
    }
    return errorCode;
  }

  /**
   * Updates the blob map based on the {@code undeleteRequest}.
   * @param undeleteRequest the {@link TtlUpdateRequest} that needs to be processed
   */
  private ServerErrorCode updateBlobMap(UndeleteRequest undeleteRequest) {
    ServerErrorCode errorCode = ServerErrorCode.No_Error;
    StoredBlob blob = blobs.get(undeleteRequest.getBlobId().getID());
    if (blob == null) {
      errorCode = ServerErrorCode.Blob_Not_Found;
    } else if (blob.isUndeleted()) {
      errorCode = ServerErrorCode.Blob_Already_Undeleted;
    } else if (!blob.isDeleted()) {
      errorCode = ServerErrorCode.Blob_Not_Deleted;
    } else if (blob.hasExpired()) {
      errorCode = ServerErrorCode.Blob_Expired;
    } else {
      blob.updateLifeVersion();
    }
    return errorCode;
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

  /**
   * Sets up this {@link MockServer} such that it returns the given {@code errorCode} for the given {@code blobId} for
   * get, ttl update and delete (not put).
   * @param blobId the blob id for which the error code must apply
   * @param errorCode the error code to apply
   */
  public void setErrorCodeForBlob(String blobId, ServerErrorCode errorCode) {
    if (errorCode == null) {
      errorCodeForBlobs.remove(blobId);
    } else {
      errorCodeForBlobs.put(blobId, errorCode);
    }
  }
}

class StoredBlob {
  final String id;
  final BlobType type;
  final BlobProperties properties;
  final ByteBuffer serializedSentPutRequest;
  long expiresAt;
  long deleteAt;
  short lifeVersion = (short) 0;
  private boolean deleted = false;
  private boolean ttlUpdated = false;
  private boolean undeleted = false;

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
    PutRequest receivedPutRequest = PutRequest.readFrom(receivedStream, clusterMap);
    id = receivedPutRequest.getBlobId().getID();
    type = receivedPutRequest.getBlobType();
    properties = receivedPutRequest.getBlobProperties();
    expiresAt = Utils.addSecondsToEpochTime(properties.getCreationTimeInMs(), properties.getTimeToLiveInSeconds());
  }

  boolean isDeleted() {
    return deleted;
  }

  boolean isTtlUpdated() {
    return ttlUpdated;
  }

  boolean isUndeleted() {
    return undeleted;
  }

  boolean hasExpired() {
    return expiresAt != Utils.Infinite_Time && SystemTime.getInstance().milliseconds() > expiresAt;
  }

  void markAsDeleted(long deleteAt) {
    deleted = true;
    undeleted = false;
    this.deleteAt = deleteAt;
  }

  void updateExpiry(long expiresAtMs) {
    ttlUpdated = true;
    expiresAt = expiresAtMs;
  }

  void updateLifeVersion() {
    deleted = false;
    undeleted = true;
    lifeVersion++;
  }
}
