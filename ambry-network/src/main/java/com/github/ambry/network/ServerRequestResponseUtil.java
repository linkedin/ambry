/**
 * Copyright 2023 LinkedIn Corp. All rights reserved.
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

package com.github.ambry.network;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.protocol.AdminRequest;
import com.github.ambry.protocol.AdminResponse;
import com.github.ambry.protocol.DeleteRequest;
import com.github.ambry.protocol.DeleteResponse;
import com.github.ambry.protocol.GetRequest;
import com.github.ambry.protocol.GetResponse;
import com.github.ambry.protocol.PutRequest;
import com.github.ambry.protocol.PutResponse;
import com.github.ambry.protocol.ReplicaMetadataRequest;
import com.github.ambry.protocol.ReplicaMetadataResponse;
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.protocol.RequestOrResponseType;
import com.github.ambry.protocol.Response;
import com.github.ambry.protocol.TtlUpdateRequest;
import com.github.ambry.protocol.TtlUpdateResponse;
import com.github.ambry.protocol.UndeleteRequest;
import com.github.ambry.protocol.UndeleteResponse;
import com.github.ambry.replication.FindTokenHelper;
import com.github.ambry.server.ServerErrorCode;
import io.netty.buffer.ByteBufInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;


/**
 * Helper class to decode requests coming to Ambry server
 */
public class ServerRequestResponseUtil {

  private final ClusterMap clusterMap;
  private final FindTokenHelper findTokenHelper;

  public ServerRequestResponseUtil(ClusterMap clusterMap, FindTokenHelper findTokenHelper) {
    this.clusterMap = clusterMap;
    this.findTokenHelper = findTokenHelper;
  }

  /**
   * Get deserialized request
   * @param networkRequest incoming network request
   * @return deserialized request
   */
  public RequestOrResponse getDecodedRequest(NetworkRequest networkRequest) throws IOException {
    RequestOrResponse request;
    if (!(networkRequest instanceof LocalRequestResponseChannel.LocalChannelRequest)) {
      InputStream is = networkRequest.getInputStream();
      DataInputStream dis = is instanceof DataInputStream ? (DataInputStream) is : new DataInputStream(is);
      RequestOrResponseType requestType = RequestOrResponseType.values()[dis.readShort()];
      switch (requestType) {
        case PutRequest:
          request = PutRequest.readFrom(dis, clusterMap);
          break;
        case GetRequest:
          request = GetRequest.readFrom(dis, clusterMap);
          break;
        case DeleteRequest:
          request = DeleteRequest.readFrom(dis, clusterMap);
          break;
        case TtlUpdateRequest:
          request = TtlUpdateRequest.readFrom(dis, clusterMap);
          break;
        case UndeleteRequest:
          request = UndeleteRequest.readFrom(dis, clusterMap);
          break;
        case ReplicaMetadataRequest:
          request = ReplicaMetadataRequest.readFrom(dis, clusterMap, findTokenHelper);
          break;
        case AdminRequest:
          request = AdminRequest.readFrom(dis, clusterMap);
          break;
        default:
          throw new UnsupportedOperationException("Request type not supported");
      }
    } else {
      request = (RequestOrResponse) ((LocalRequestResponseChannel.LocalChannelRequest) networkRequest).getRequestInfo()
          .getRequest();
      if (request.getRequestType() == RequestOrResponseType.PutRequest) {
        // For PutRequest sent via local channel, we have to create a new PutRequest object to wrap the data buffer as a
        // stream. Also, we need to set the crc value to null since the request is not coming over network.
        PutRequest receivedPutRequest = ((PutRequest) request);
        request = new PutRequest(receivedPutRequest.getCorrelationId(), receivedPutRequest.getClientId(),
            receivedPutRequest.getBlobId(), receivedPutRequest.getBlobProperties(),
            receivedPutRequest.getUsermetadata(), receivedPutRequest.getBlobSize(), receivedPutRequest.getBlobType(),
            receivedPutRequest.getBlobEncryptionKey(), new ByteBufInputStream(receivedPutRequest.getBlob()), null);
      }
    }
    return request;
  }

  /**
   * Creates {@link Response} for errors.
   * @param request incoming request
   * @param serverErrorCode error code
   * @return {@link Response} with provided error code
   */
  public Response createErrorResponse(RequestOrResponse request, ServerErrorCode serverErrorCode) {
    RequestOrResponseType type = request.getRequestType();
    Response response;
    switch (type) {
      case PutRequest:
        response = new PutResponse(request.getCorrelationId(), request.getClientId(), serverErrorCode);
        break;
      case GetRequest:
        response = new GetResponse(request.getCorrelationId(), request.getClientId(), serverErrorCode);
        break;
      case DeleteRequest:
        response = new DeleteResponse(request.getCorrelationId(), request.getClientId(), serverErrorCode);
        break;
      case TtlUpdateRequest:
        response = new TtlUpdateResponse(request.getCorrelationId(), request.getClientId(), serverErrorCode);
        break;
      case UndeleteRequest:
        response = new UndeleteResponse(request.getCorrelationId(), request.getClientId(), serverErrorCode);
        break;
      case ReplicaMetadataRequest:
        response = new ReplicaMetadataResponse(request.getCorrelationId(), request.getClientId(), serverErrorCode,
            ReplicaMetadataResponse.getCompatibleResponseVersion(request.getVersionId()));
        break;
      case AdminRequest:
        response = new AdminResponse(request.getCorrelationId(), request.getClientId(), serverErrorCode);
        break;
      default:
        throw new UnsupportedOperationException("Request type not supported");
    }
    return response;
  }
}
