package com.github.ambry.server;

import java.io.DataInputStream;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.messageformat.*;
import com.github.ambry.network.RequestResponseChannel;
import com.github.ambry.shared.*;
import com.github.ambry.network.Request;
import com.github.ambry.network.Send;
import com.github.ambry.store.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The main request implementation class. All requests to the server are
 * handled by this class
 */

public class AmbryRequests {

  private StoreManager storeManager;
  private final RequestResponseChannel requestResponseChannel;
  private Logger logger = LoggerFactory.getLogger(getClass());
  private final ClusterMap clusterMap;

  public AmbryRequests(StoreManager storeManager,
                       RequestResponseChannel requestResponseChannel,
                       ClusterMap clusterMap) {
    this.storeManager = storeManager;
    this.requestResponseChannel = requestResponseChannel;
    this.clusterMap = clusterMap;
  }

  public void handleRequests(Request request) throws InterruptedException {
    try {
      // log
      DataInputStream stream = new DataInputStream(request.getInputStream());
      RequestResponseType type = RequestResponseType.values()[stream.readShort()];
      switch (type) {
        case PutRequest:
          handlePutRequest(request);
          break;
        case GetRequest:
          handleGetRequest(request);
          break;
        case DeleteRequest:
          handleDeleteRequest(request);
          break;
        case TTLRequest:
          handleTTLRequest(request);
          break;
        default: throw new UnsupportedOperationException("Request type not supported");
      }
    }
    catch (Exception e) {
      requestResponseChannel.closeConnection(request);
    }
  }

  private void handlePutRequest(Request request) throws IOException, InterruptedException {
    PutRequest putRequest = PutRequest.readFrom(new DataInputStream(request.getInputStream()), clusterMap);
    try {
      MessageFormatInputStream stream = new PutMessageFormatInputStream(putRequest.getBlobId(),
                                                                        putRequest.getBlobProperties(),
                                                                        putRequest.getUsermetadata(),
                                                                        putRequest.getData(),
                                                                        putRequest.getBlobProperties().getBlobSize());
      MessageInfo info = new MessageInfo(putRequest.getBlobId(),
                                         stream.getSize(),
                                         putRequest.getBlobProperties().getTimeToLiveInMs());
      ArrayList<MessageInfo> infoList = new ArrayList<MessageInfo>();
      infoList.add(info);
      MessageFormatWriteSet writeset = new MessageFormatWriteSet(stream, infoList);
      Store storeToPut = storeManager.getStore(putRequest.getBlobId().getPartition());
      storeToPut.put(writeset);
      PutResponse response = new PutResponse(putRequest.getCorrelationId(),
                                             putRequest.getClientId(),
                                             ServerErrorCode.No_Error);
      requestResponseChannel.sendResponse(response, request);
    }
    catch (StoreException e) {
      logger.error("Store exception on a put with error code {} and exception {}",e.getErrorCode(), e);
      PutResponse response = new PutResponse(putRequest.getCorrelationId(),
                                             putRequest.getClientId(),
                                             ErrorMapping.getStoreErrorMapping(e.getErrorCode()));
      requestResponseChannel.sendResponse(response, request);
    }
    catch (Exception e) {
      logger.error("Unknown exception on a put {} ", e);
      PutResponse response = new PutResponse(putRequest.getCorrelationId(),
                                             putRequest.getClientId(),
                                             ServerErrorCode.Unknown_Error);
      requestResponseChannel.sendResponse(response, request);
    }
  }

  private void handleGetRequest(Request request) throws IOException, InterruptedException {
    GetRequest getRequest = GetRequest.readFrom(new DataInputStream(request.getInputStream()), clusterMap);
    try {
      Store storeToGet = storeManager.getStore(getRequest.getPartition());
      StoreInfo info = storeToGet.get(getRequest.getBlobIds());
      Send blobsToSend = new MessageFormatSend(info.getMessageReadSet(), getRequest.getMessageFormatFlag());
      GetResponse response = new GetResponse(getRequest.getCorrelationId(),
                                             getRequest.getClientId(),
                                             info.getMessageReadSetInfo(),
                                             blobsToSend,
                                             ServerErrorCode.No_Error);
      requestResponseChannel.sendResponse(response, request);
    }
    catch (StoreException e) {
      logger.error("Store exception on a get with error code {} and exception {}", e.getErrorCode(), e);
      GetResponse response = new GetResponse(getRequest.getCorrelationId(),
                                             getRequest.getClientId(),
                                             ErrorMapping.getStoreErrorMapping(e.getErrorCode()));
      requestResponseChannel.sendResponse(response, request);
    }
    catch (MessageFormatException e) {
      logger.error("Message format exception on a get with error code {} and exception {}", e.getErrorCode(), e);
      GetResponse response = new GetResponse(getRequest.getCorrelationId(),
                                             getRequest.getClientId(),
                                             ErrorMapping.getMessageFormatErrorMapping(e.getErrorCode()));
      requestResponseChannel.sendResponse(response, request);
    }
    catch (Exception e) {
      logger.error("Unknown exception on a get {}", e);
      GetResponse response = new GetResponse(getRequest.getCorrelationId(),
                                             getRequest.getClientId(),
                                             ServerErrorCode.Unknown_Error);
      requestResponseChannel.sendResponse(response, request);
    }
  }

  private void handleDeleteRequest(Request request) throws IOException, InterruptedException {
    DeleteRequest deleteRequest = DeleteRequest.readFrom(new DataInputStream(request.getInputStream()), clusterMap);
    try {
      MessageFormatInputStream stream = new DeleteMessageFormatInputStream(deleteRequest.getBlobId());
      MessageInfo info = new MessageInfo(deleteRequest.getBlobId(), stream.getSize());
      ArrayList<MessageInfo> infoList = new ArrayList<MessageInfo>();
      infoList.add(info);
      MessageFormatWriteSet writeset = new MessageFormatWriteSet(stream, infoList);
      Store storeToDelete = storeManager.getStore(deleteRequest.getBlobId().getPartition());
      storeToDelete.delete(writeset);
      DeleteResponse response = new DeleteResponse(deleteRequest.getCorrelationId(),
                                                   deleteRequest.getClientId(),
                                                   ServerErrorCode.No_Error);
      requestResponseChannel.sendResponse(response, request);
    }
    catch (StoreException e) {
      logger.error("Store exception on a put with error code {} and exception {}",e.getErrorCode(), e);
      DeleteResponse response = new DeleteResponse(deleteRequest.getCorrelationId(),
                                                   deleteRequest.getClientId(),
                                                   ErrorMapping.getStoreErrorMapping(e.getErrorCode()));
      requestResponseChannel.sendResponse(response, request);
    }
    catch (Exception e) {
      logger.error("Unknown exception on delete {}", e);
      DeleteResponse response = new DeleteResponse(deleteRequest.getCorrelationId(),
                                                   deleteRequest.getClientId(),
                                                   ServerErrorCode.Unknown_Error);
      requestResponseChannel.sendResponse(response, request);
    }
  }

  private void handleTTLRequest(Request request) throws IOException, InterruptedException {
    TTLRequest ttlRequest = TTLRequest.readFrom(new DataInputStream(request.getInputStream()), clusterMap);
    try {
      MessageFormatInputStream stream = new TTLMessageFormatInputStream(ttlRequest.getBlobId(), ttlRequest.getNewTTL());
      MessageInfo info = new MessageInfo(ttlRequest.getBlobId(), stream.getSize(), ttlRequest.getNewTTL());
      ArrayList<MessageInfo> infoList = new ArrayList<MessageInfo>();
      infoList.add(info);
      MessageFormatWriteSet writeset = new MessageFormatWriteSet(stream, infoList);
      Store storeToUpdateTTL = storeManager.getStore(ttlRequest.getBlobId().getPartition());
      storeToUpdateTTL.updateTTL(writeset);
      TTLResponse response = new TTLResponse(ttlRequest.getCorrelationId(),
                                             ttlRequest.getClientId(),
                                             ServerErrorCode.No_Error);
      requestResponseChannel.sendResponse(response, request);
    }
    catch (StoreException e) {
      logger.error("Store exception on a put with error code {} and exception {}",e.getErrorCode(), e);
      TTLResponse response = new TTLResponse(ttlRequest.getCorrelationId(),
                                             ttlRequest.getClientId(),
                                             ErrorMapping.getStoreErrorMapping(e.getErrorCode()));
      requestResponseChannel.sendResponse(response, request);
    }
    catch (Exception e) {
      logger.error("Unknown exception on ttl {}", e);
      TTLResponse response = new TTLResponse(ttlRequest.getCorrelationId(),
                                             ttlRequest.getClientId(),
                                             ServerErrorCode.Unknown_Error);
      requestResponseChannel.sendResponse(response, request);
    }
  }
}