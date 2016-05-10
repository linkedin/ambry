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

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.network.BoundedByteBufferReceive;
import com.github.ambry.network.ByteBufferSend;
import com.github.ambry.network.Send;
import com.github.ambry.protocol.DeleteRequest;
import com.github.ambry.protocol.DeleteResponse;
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


/**
 * A class that mocks the server (data node) and provides methods for sending requests and setting error states.
 */
class MockServer {
  private ServerErrorCode hardError = null;
  private LinkedList<ServerErrorCode> serverErrors = new LinkedList<ServerErrorCode>();
  private final Map<String, ByteBuffer> blobs = new ConcurrentHashMap<String, ByteBuffer>();
  private final HashMap<String, ServerErrorCode> blobIdToServerErrorCode = new HashMap<String, ServerErrorCode>();
  private boolean shouldRespond = true;
  private final ClusterMap clusterMap;
  private final String dataCenter;

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
  public BoundedByteBufferReceive send(Send send)
      throws IOException {
    if (!shouldRespond) {
      return null;
    }
    ServerErrorCode serverError =
        hardError != null ? hardError : serverErrors.size() > 0 ? serverErrors.poll() : ServerErrorCode.No_Error;
    RequestOrResponseType type = ((RequestOrResponse) send).getRequestType();
    RequestOrResponse response;
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
  PutResponse makePutResponse(PutRequest putRequest, ServerErrorCode putError)
      throws IOException {
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
  GetResponse makeGetResponse(GetRequest getRequest, ServerErrorCode getError)
      throws IOException {
    GetResponse getResponse;
    if (getError == ServerErrorCode.No_Error) {
      List<PartitionRequestInfo> infos = getRequest.getPartitionInfoList();
      if (infos.size() != 1 || infos.get(0).getBlobIds().size() != 1) {
        getError = ServerErrorCode.Unknown_Error;
      }
    }

    ServerErrorCode serverError;
    ServerErrorCode partitionError;
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

    if (serverError == ServerErrorCode.No_Error) {
      int byteBufferSize;
      ByteBuffer byteBuffer;
      StoreKey key = getRequest.getPartitionInfoList().get(0).getBlobIds().get(0);
      if (blobs.containsKey(key.getID())) {
        ByteBuffer buf = blobs.get(key.getID()).duplicate();
        // read off the size
        buf.getLong();
        // read off the type.
        buf.getShort();
        PutRequest originalBlobPutReq =
            PutRequest.readFrom(new DataInputStream(new ByteBufferInputStream(buf)), clusterMap);
        switch (getRequest.getMessageFormatFlag()) {
          case BlobInfo:
            BlobProperties blobProperties = originalBlobPutReq.getBlobProperties();
            ByteBuffer userMetadata = originalBlobPutReq.getUsermetadata();
            byteBufferSize = MessageFormatRecord.BlobProperties_Format_V1.getBlobPropertiesRecordSize(blobProperties)
                + MessageFormatRecord.UserMetadata_Format_V1.getUserMetadataSize(userMetadata);
            byteBuffer = ByteBuffer.allocate(byteBufferSize);
            MessageFormatRecord.BlobProperties_Format_V1.serializeBlobPropertiesRecord(byteBuffer, blobProperties);
            MessageFormatRecord.UserMetadata_Format_V1.serializeUserMetadataRecord(byteBuffer, userMetadata);
            break;
          case Blob:
            byteBufferSize =
                (int) MessageFormatRecord.Blob_Format_V2.getBlobRecordSize((int) originalBlobPutReq.getBlobSize());
            byteBuffer = ByteBuffer.allocate(byteBufferSize);
            MessageFormatRecord.Blob_Format_V2
                .serializePartialBlobRecord(byteBuffer, (int) originalBlobPutReq.getBlobSize(),
                    originalBlobPutReq.getBlobType());
            byteBuffer.put(
                Utils.readBytesFromStream(originalBlobPutReq.getBlobStream(), (int) originalBlobPutReq.getBlobSize()));
            Crc32 crc = new Crc32();
            crc.update(byteBuffer.array(), 0, byteBuffer.position());
            byteBuffer.putLong(crc.getValue());
            break;
          default:
            throw new IOException("GetRequest flag is not supported: " + getRequest.getMessageFormatFlag());
        }
      } else {
        if (partitionError == ServerErrorCode.No_Error) {
          partitionError = ServerErrorCode.Blob_Not_Found;
        }
        byteBuffer = ByteBuffer.allocate(0);
        byteBufferSize = 0;
      }

      byteBuffer.flip();
      ByteBufferSend responseSend = new ByteBufferSend(byteBuffer);
      List<MessageInfo> messageInfoList = new ArrayList<MessageInfo>(1);
      List<PartitionResponseInfo> partitionResponseInfoList = new ArrayList<PartitionResponseInfo>();
      messageInfoList.add(new MessageInfo(key, byteBufferSize));
      PartitionResponseInfo partitionResponseInfo =
          partitionError == ServerErrorCode.No_Error ? new PartitionResponseInfo(
              getRequest.getPartitionInfoList().get(0).getPartition(), messageInfoList)
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

  /**
   *
   * Make a {@link DeleteResponse} for the given {@link DeleteRequest} for which the given {@link ServerErrorCode} was
   * encountered.
   * @param deleteRequest the {@link DeleteRequest} for which the response is being constructed.
   * @param deleteError the {@link ServerErrorCode} that was encountered.
   * @return the constructed {@link DeleteResponse}
   * @throws IOException if there was an error constructing the response.
   */
  DeleteResponse makeDeleteResponse(DeleteRequest deleteRequest, ServerErrorCode deleteError)
      throws IOException {
    String blobIdString = deleteRequest.getBlobId().getID();
    if (deleteError == ServerErrorCode.No_Error) {
      deleteError = getErrorFromBlobIdStr(blobIdString);
    }
    return new DeleteResponse(deleteRequest.getCorrelationId(), deleteRequest.getClientId(), deleteError);
  }

  /**
   * Serialize contents of the PutRequest and update the blob map with the serialized content.
   * @param putRequest the PutRequest
   * @throws IOException if there was an error reading the contents of the given PutRequest.
   */
  private void updateBlobMap(PutRequest putRequest)
      throws IOException {
    String id = putRequest.getBlobId().getID();
    ByteBuffer buf = ByteBuffer.allocate((int) putRequest.sizeInBytes());
    ByteBufferChannel bufChannel = new ByteBufferChannel(buf);
    putRequest.writeTo(bufChannel);
    buf.flip();
    blobs.put(id, buf);
  }

  /**
   * @return the blobs that were put on this server.
   */
  Map<String, ByteBuffer> getBlobs() {
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
   * Clear the error for subsequent requests. That is all responses from this point onwards will be successful
   * ({@link ServerErrorCode#No_Error}) until/unless another set error method is invoked.
   */
  public void resetServerErrors() {
    this.serverErrors.clear();
    this.hardError = null;
  }

  /**
   * Set whether or not the server would send response back.
   * @param shouldRespond {@code true} if the server responds.
   */
  public void setShouldRespond(boolean shouldRespond) {
    this.shouldRespond = shouldRespond;
  }

  /**
   * Get the pre-defined {@link ServerErrorCode} that this server should return for a given {@code blobIdString}.
   * @param blobIdString The blob for which a {@link ServerErrorCode} needs to be returned.
   * @return A {@code ServerErrorCode} if it is present. Otherwise {@code ServerErrorCode.Blob_Not_Found}.
   */
  public ServerErrorCode getErrorFromBlobIdStr(String blobIdString) {
    return blobIdToServerErrorCode.containsKey(blobIdString) ? blobIdToServerErrorCode.get(blobIdString)
        : ServerErrorCode.Blob_Not_Found;
  }

  /**
   * Set the mapping relationship between a {@code blobIdString} and the {@link ServerErrorCode} this server should
   * return.
   * @param blobIdString The key in this mapping relation.
   * @param code The {@link ServerErrorCode} for the {@code blobIdString}.
   */
  public void setBlobIdToServerErrorCode(String blobIdString, ServerErrorCode code) {
    blobIdToServerErrorCode.put(blobIdString, code);
  }
}

