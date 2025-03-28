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
import com.github.ambry.commons.BlobId;
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
import com.github.ambry.protocol.ReplicateBlobRequest;
import com.github.ambry.protocol.ReplicateBlobResponse;
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
import java.util.zip.CRC32;

import static com.github.ambry.messageformat.MessageFormatRecord.*;


/**
 * A class that mocks the server (data node) and provides methods for sending requests and setting error states.
 */
class MockServer {
  private ServerErrorCode hardError = null;
  private LinkedList<ServerErrorCode> serverErrors = new LinkedList<ServerErrorCode>();
  private final HashMap<RequestOrResponseType, LinkedList<ServerErrorCode>> serverErrorsByType = new HashMap<>();
  private final Map<String, StoredBlob> blobs = new ConcurrentHashMap<>();
  private boolean shouldRespond = true;
  private short blobFormatVersion = MessageFormatRecord.Blob_Version_V3;
  private boolean getErrorOnDataBlobOnly = false;
  private final ClusterMap clusterMap;
  private final String dataCenter;
  // hostName and hostPort are used to identify this Mock Server.
  private final String hostName;
  private final Integer hostPort;
  // With layout, can communicate with other MockServers.
  private final MockServerLayout layout;
  private final ConcurrentHashMap<RequestOrResponseType, LongAdder> requestCounts = new ConcurrentHashMap<>();
  private final Map<String, ServerErrorCode> errorCodeForBlobs = new HashMap<>();

  MockServer(ClusterMap clusterMap, String dataCenter) {
    this(clusterMap, dataCenter, null, null, null);
  }

  MockServer(ClusterMap clusterMap, String dataCenter, String hostName, Integer port, MockServerLayout layout) {
    this.clusterMap = clusterMap;
    this.dataCenter = dataCenter;
    this.hostName = hostName;
    this.hostPort = port;
    this.layout = layout;
    this.serverErrorsByType.put(RequestOrResponseType.PutRequest, new LinkedList<>());
    this.serverErrorsByType.put(RequestOrResponseType.GetRequest, new LinkedList<>());
    this.serverErrorsByType.put(RequestOrResponseType.DeleteRequest, new LinkedList<>());
    this.serverErrorsByType.put(RequestOrResponseType.TtlUpdateRequest, new LinkedList<>());
    this.serverErrorsByType.put(RequestOrResponseType.UndeleteRequest, new LinkedList<>());
    this.serverErrorsByType.put(RequestOrResponseType.ReplicaMetadataRequest, new LinkedList<>());
    this.serverErrorsByType.put(RequestOrResponseType.ReplicateBlobRequest, new LinkedList<>());
    this.serverErrorsByType.put(RequestOrResponseType.PutRequest, new LinkedList<>());
    this.serverErrorsByType.put(RequestOrResponseType.AdminRequest, new LinkedList<>());
  }

  public String getHostName() {
    return hostName;
  }

  public Integer getHostPort() {
    return hostPort;
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
    // Error order:
    // 1. hardError
    // 2. if No_Error, use serverErrorsByType
    // 3. if No_Error, use serverErrors
    RequestOrResponseType type = ((RequestOrResponse) send).getRequestType();
    LinkedList<ServerErrorCode> errorsByType = serverErrorsByType.get(type);
    ServerErrorCode serverError =
        hardError != null ? hardError : errorsByType.size() > 0 ? errorsByType.poll() : ServerErrorCode.NoError;
    if (serverError == ServerErrorCode.NoError) {
      serverError = serverErrors.size() > 0 ? serverErrors.poll() : ServerErrorCode.NoError;
    }
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
      case ReplicateBlobRequest:
        response = makeReplicateBlobResponse((ReplicateBlobRequest) send, serverError);
        break;
      default:
        throw new IOException("Unknown request type received");
    }
    ByteBufferChannel channel = new ByteBufferChannel(ByteBuffer.allocate((int) response.sizeInBytes()));
    response.writeTo(channel);
    response.release();
    ByteBuffer payload = channel.getBuffer();
    payload.flip();
    BoundedNettyByteBufReceive receive = new BoundedNettyByteBufReceive(100 * 1024 * 1024);
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
    if (putError == ServerErrorCode.NoError) {
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
    if (getError == ServerErrorCode.NoError) {
      List<PartitionRequestInfo> infos = getRequest.getPartitionInfoList();
      if (infos.size() != 1 || infos.get(0).getBlobIds().size() != 1) {
        getError = ServerErrorCode.UnknownError;
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
      if (getError == ServerErrorCode.NoError || getError == ServerErrorCode.BlobExpired
          || getError == ServerErrorCode.BlobDeleted || getError == ServerErrorCode.BlobNotFound
          || getError == ServerErrorCode.BlobAuthorizationFailure || getError == ServerErrorCode.DiskUnavailable) {
        partitionError = getError;
        serverError = ServerErrorCode.NoError;
      } else {
        serverError = getError;
        // does not matter - this will not be checked if serverError is not No_Error.
        partitionError = ServerErrorCode.NoError;
      }
    } else {
      serverError = ServerErrorCode.NoError;
      partitionError = ServerErrorCode.NoError;
    }

    if (serverError == ServerErrorCode.NoError) {
      int byteBufferSize;
      ByteBuffer byteBuffer;
      StoreKey key = getRequest.getPartitionInfoList().get(0).getBlobIds().get(0);
      short accountId = Account.UNKNOWN_ACCOUNT_ID;
      short containerId = Container.UNKNOWN_CONTAINER_ID;
      long operationTimeMs = Utils.Infinite_Time;
      StoredBlob blob = blobs.get(key.getID());
      ServerErrorCode processedError = errorForGet(key.getID(), blob, getRequest);
      MessageMetadata msgMetadata = null;
      if (processedError == ServerErrorCode.NoError) {
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
              case MessageFormatRecord.Blob_Version_V3:
                if (originalBlobPutReq.getBlobEncryptionKey() != null) {
                  msgMetadata = new MessageMetadata(originalBlobPutReq.getBlobEncryptionKey().duplicate());
                }
                byteBufferSize =
                    (int) MessageFormatRecord.Blob_Format_V3.getBlobRecordSize((int) originalBlobPutReq.getBlobSize());
                byteBuffer = ByteBuffer.allocate(byteBufferSize);
                MessageFormatRecord.Blob_Format_V3.serializePartialBlobRecord(byteBuffer,
                    (int) originalBlobPutReq.getBlobSize(), originalBlobPutReq.getBlobType(),
                    originalBlobPutReq.isCompressed());
                break;
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
            CRC32 crc = new CRC32();
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
              case MessageFormatRecord.Blob_Version_V3:
                blobRecordSize =
                    (int) MessageFormatRecord.Blob_Format_V3.getBlobRecordSize((int) originalBlobPutReq.getBlobSize());
                break;
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
              case MessageFormatRecord.Blob_Version_V3:
                MessageFormatRecord.Blob_Format_V3.serializePartialBlobRecord(byteBuffer,
                    (int) originalBlobPutReq.getBlobSize(), originalBlobPutReq.getBlobType(), originalBlobPutReq.isCompressed());
                break;
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
            crc = new CRC32();
            crc.update(byteBuffer.array(), blobRecordStart, blobRecordSize - MessageFormatRecord.Crc_Size);
            byteBuffer.putLong(crc.getValue());
            break;
          default:
            throw new IOException("GetRequest flag is not supported: " + getRequest.getMessageFormatFlag());
        }
      } else if (processedError == ServerErrorCode.BlobDeleted) {
        if (partitionError == ServerErrorCode.NoError) {
          partitionError = ServerErrorCode.BlobDeleted;
        }
        byteBuffer = ByteBuffer.allocate(0);
        byteBufferSize = 0;
      } else if (processedError == ServerErrorCode.BlobExpired) {
        if (partitionError == ServerErrorCode.NoError) {
          partitionError = ServerErrorCode.BlobExpired;
        }
        byteBuffer = ByteBuffer.allocate(0);
        byteBufferSize = 0;
      } else if (processedError == ServerErrorCode.BlobAuthorizationFailure) {
        if (partitionError == ServerErrorCode.NoError) {
          partitionError = ServerErrorCode.BlobAuthorizationFailure;
        }
        byteBuffer = ByteBuffer.allocate(0);
        byteBufferSize = 0;
      } else if (processedError == ServerErrorCode.ReplicaUnavailable) {
        if (partitionError == ServerErrorCode.NoError) {
          partitionError = ServerErrorCode.ReplicaUnavailable;
        }
        byteBuffer = ByteBuffer.allocate(0);
        byteBufferSize = 0;
      } else {
        if (partitionError == ServerErrorCode.NoError) {
          partitionError = ServerErrorCode.BlobNotFound;
        }
        byteBuffer = ByteBuffer.allocate(0);
        byteBufferSize = 0;
      }

      byteBuffer.flip();
      ByteBufferSend responseSend = new ByteBufferSend(byteBuffer);
      List<MessageInfo> messageInfoList = new ArrayList<>();
      List<MessageMetadata> messageMetadataList = new ArrayList<>();
      List<PartitionResponseInfo> partitionResponseInfoList = new ArrayList<PartitionResponseInfo>();
      if (partitionError == ServerErrorCode.NoError) {
        messageInfoList.add(
            new MessageInfo(key, byteBufferSize, false, blob.isTtlUpdated(), blob.isUndeleted(), blob.expiresAt, null,
                accountId, containerId, operationTimeMs, blob.lifeVersion));
        messageMetadataList.add(msgMetadata);
      }
      PartitionResponseInfo partitionResponseInfo =
          partitionError == ServerErrorCode.NoError ? new PartitionResponseInfo(
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
    ServerErrorCode retCode = errorCodeForBlobs.getOrDefault(blobId, ServerErrorCode.NoError);
    if (blob == null) {
      retCode = ServerErrorCode.BlobNotFound;
    } else if (blob.isDeleted() && !getRequest.getGetOption().equals(GetOption.Include_All)
        && !getRequest.getGetOption().equals(GetOption.Include_Deleted_Blobs)) {
      retCode = ServerErrorCode.BlobDeleted;
    } else if (blob.hasExpired() && !getRequest.getGetOption().equals(GetOption.Include_All)
        && !getRequest.getGetOption().equals(GetOption.Include_Expired_Blobs)) {
      retCode = ServerErrorCode.BlobExpired;
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
    if (deleteError == ServerErrorCode.NoError) {
      deleteError = errorCodeForBlobs.getOrDefault(deleteRequest.getBlobId().getID(), ServerErrorCode.NoError);
    }
    if (deleteError == ServerErrorCode.NoError) {
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
    if (ttlUpdateError == ServerErrorCode.NoError) {
      ttlUpdateError = errorCodeForBlobs.getOrDefault(ttlUpdateRequest.getBlobId().getID(), ServerErrorCode.NoError);
    }
    if (ttlUpdateError == ServerErrorCode.NoError) {
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
    if (undeleteError == ServerErrorCode.NoError) {
      undeleteError = errorCodeForBlobs.getOrDefault(undeleteRequest.getBlobId().getID(), ServerErrorCode.NoError);
    }
    if (undeleteError == ServerErrorCode.NoError) {
      undeleteError = updateBlobMap(undeleteRequest);
    }
    if (undeleteError == ServerErrorCode.NoError || undeleteError == ServerErrorCode.BlobAlreadyUndeleted) {
      short lifeVersion = blobs.get(undeleteRequest.getBlobId().getID()).lifeVersion;
      return new UndeleteResponse(undeleteRequest.getCorrelationId(), undeleteRequest.getClientId(), lifeVersion,
          undeleteError);
    } else {
      return new UndeleteResponse(undeleteRequest.getCorrelationId(), undeleteRequest.getClientId(), undeleteError);
    }
  }

  /**
   * Make a {@link ReplicateBlobResponse} for the given {@link ReplicateBlobRequest} for which the given {@link ServerErrorCode} was
   * encountered.
   * *
   * @return the constructed {@link ReplicateBlobResponse}
   */
  ReplicateBlobResponse makeReplicateBlobResponse(ReplicateBlobRequest replicateBlobRequest,
      ServerErrorCode replicateBlobError) {
    if (replicateBlobError == ServerErrorCode.NoError) {
      replicateBlobError =
          errorCodeForBlobs.getOrDefault(replicateBlobRequest.getBlobId().getID(), ServerErrorCode.NoError);
    }
    if (replicateBlobError == ServerErrorCode.NoError) {
      updateBlobMap(replicateBlobRequest);
    }
    return new ReplicateBlobResponse(replicateBlobRequest.getCorrelationId(), replicateBlobRequest.getClientId(),
        replicateBlobError);
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
   * Serialize contents of the ReplicateBlobRequest and update the blob map with the serialized content.
   * @param replicateBlobRequest the ReplicateBlobRequest
   */
  private void updateBlobMap(ReplicateBlobRequest replicateBlobRequest) {
    String sourceHostName = replicateBlobRequest.getSourceHostName();
    int sourceHostPort = replicateBlobRequest.getSourceHostPort();
    BlobId blobId = replicateBlobRequest.getBlobId();
    // local store has the Blob already
    if (blobs.containsKey(blobId.getID())) {
      return;
    }

    // replicating the Blob from the source Data node.
    for (MockServer server : layout.getMockServers()) {
      // find the source Data node.
      if (server.getHostName().equals(sourceHostName) && server.getHostPort().equals(sourceHostPort)) {
        Map<String, StoredBlob> remoteBlobs = server.getBlobs();
        if (remoteBlobs.containsKey(blobId.getID())) {
          // write to the local store
          this.blobs.put(blobId.getID(), remoteBlobs.get(blobId.getID()));
        }
        break;
      }
    }
  }

  /**
   * Updates the map of the blobs to indicate that the blob in question has been deleted
   * @param deleteRequest the {@link DeleteRequest} that describes what needs to be done
   */
  private void updateBlobMap(DeleteRequest deleteRequest) {
    StoredBlob blob = blobs.get(deleteRequest.getBlobId().getID());
    if (blob != null) {
      blob.markAsDeleted(deleteRequest.getDeletionTimeInMs());
    } else {
      if (deleteRequest.shouldForceDelete()) {
        blobs.put(deleteRequest.getBlobId().getID(), new StoredBlob(deleteRequest));
      }
    }
  }

  /**
   * Updates the blob map based on the {@code ttlUpdateRequest}.
   * @param ttlUpdateRequest the {@link TtlUpdateRequest} that needs to be processed
   */
  private ServerErrorCode updateBlobMap(TtlUpdateRequest ttlUpdateRequest) {
    ServerErrorCode errorCode = ServerErrorCode.NoError;
    StoredBlob blob = blobs.get(ttlUpdateRequest.getBlobId().getID());
    if (blob != null && !blob.isDeleted() && !blob.hasExpired() && !blob.isTtlUpdated()) {
      blob.updateExpiry(ttlUpdateRequest.getExpiresAtMs());
    } else if (blob == null) {
      errorCode = ServerErrorCode.BlobNotFound;
    } else if (blob.isDeleted()) {
      errorCode = ServerErrorCode.BlobDeleted;
    } else if (blob.hasExpired()) {
      errorCode = ServerErrorCode.BlobExpired;
    } else if (blob.isTtlUpdated()) {
      errorCode = ServerErrorCode.BlobAlreadyUpdated;
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
    ServerErrorCode errorCode = ServerErrorCode.NoError;
    StoredBlob blob = blobs.get(undeleteRequest.getBlobId().getID());
    if (blob == null) {
      errorCode = ServerErrorCode.BlobNotFound;
    } else if (blob.isUndeleted()) {
      errorCode = ServerErrorCode.BlobAlreadyUndeleted;
    } else if (!blob.isDeleted()) {
      errorCode = ServerErrorCode.BlobNotDeleted;
    } else if (blob.hasExpired()) {
      errorCode = ServerErrorCode.BlobExpired;
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

  public void setServerErrorsByType(RequestOrResponseType type, List<ServerErrorCode> serverErrors) {
    this.serverErrorsByType.get(type).clear();
    this.serverErrorsByType.get(type).addAll(serverErrors);
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
   * ({@link ServerErrorCode#NoError}) until/unless another set error method is invoked.
   */
  public void resetServerErrors() {
    this.serverErrors.clear();
    this.hardError = null;
    this.getErrorOnDataBlobOnly = false;
    this.serverErrorsByType.get(RequestOrResponseType.PutRequest).clear();
    this.serverErrorsByType.get(RequestOrResponseType.GetRequest).clear();
    this.serverErrorsByType.get(RequestOrResponseType.DeleteRequest).clear();
    this.serverErrorsByType.get(RequestOrResponseType.TtlUpdateRequest).clear();
    this.serverErrorsByType.get(RequestOrResponseType.UndeleteRequest).clear();
    this.serverErrorsByType.get(RequestOrResponseType.ReplicaMetadataRequest).clear();
    this.serverErrorsByType.get(RequestOrResponseType.ReplicateBlobRequest).clear();
    this.serverErrorsByType.get(RequestOrResponseType.PutRequest).clear();
    this.serverErrorsByType.get(RequestOrResponseType.AdminRequest).clear();
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
  private boolean isDeleteTombstone = false;

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

  StoredBlob(DeleteRequest deleteRequest) {
    id = deleteRequest.getBlobId().getID();
    type = BlobType.DataBlob; // fake type
    properties = null;
    serializedSentPutRequest = null;
    deleteAt = deleteRequest.getDeletionTimeInMs();
    deleted = true;
    isDeleteTombstone = true;
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

  boolean isDeleteTombstone() {
    return isDeleteTombstone;
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
