package com.github.ambry.server;

import java.io.DataInputStream;

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

/**
 * The main request implementation class. All requests to the server are
 * handled by this class
 */

public class AmbryRequests {

  private Store blobStore;
  private final RequestResponseChannel requestResponseChannel;
  private Logger logger = LoggerFactory.getLogger(getClass());

  public AmbryRequests(Store blobStore, RequestResponseChannel requestResponseChannel) {
    this.blobStore = blobStore;
    this.requestResponseChannel = requestResponseChannel;
  }

  public void handleRequests(Request request) {
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
        default: // throw exception
      }
    } catch (Exception e) {
      // log and measure time
    }
  }

  private void handlePutRequest(Request request) throws IOException {
    try {
      PutRequest putRequest = PutRequest.readFrom(new DataInputStream(request.getInputStream()));
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
      blobStore.put(writeset);
      PutResponse response = new PutResponse(putRequest.getCorrelationId(), putRequest.getClientId(), (short)0);
      requestResponseChannel.sendResponse(response, request);
    }
    catch (StoreException e) {
      logger.error("Error on doing a put {}", e);
    }
    catch (Exception e) {
      logger.error("Error on doing a put {}", e);
    }
  }

  private void handleGetRequest(Request request) {
    try {
      GetRequest getRequest = GetRequest.readFrom(new DataInputStream(request.getInputStream()));
      StoreInfo info = blobStore.get(getRequest.getBlobIds());
      Send blobsToSend = new MessageFormatSend(info.getMessageReadSet(), getRequest.getMessageFormatFlag());
      GetResponse response = new GetResponse(getRequest.getCorrelationId(), getRequest.getClientId(),
              info.getMessageReadSetInfo(), blobsToSend);
      requestResponseChannel.sendResponse(response, request);
    }
    catch (Exception e) {
      logger.error("Error while doing get request {}", e);
    }
  }

  private void handleDeleteRequest(Request request) {
    try {
      DeleteRequest deleteRequest = DeleteRequest.readFrom(new DataInputStream(request.getInputStream()));
      MessageFormatInputStream stream = new DeleteMessageFormatInputStream(deleteRequest.getBlobId());
      MessageInfo info = new MessageInfo(deleteRequest.getBlobId(), stream.getSize());
      ArrayList<MessageInfo> infoList = new ArrayList<MessageInfo>();
      infoList.add(info);
      MessageFormatWriteSet writeset = new MessageFormatWriteSet(stream, infoList);
      blobStore.delete(writeset);
      DeleteResponse response = new DeleteResponse(deleteRequest.getCorrelationId(),
                                                   deleteRequest.getClientId(),
                                                   (short)0);
      requestResponseChannel.sendResponse(response, request);
    }
    catch (Exception e) {

    }
  }

  private void handleTTLRequest(Request request) {
    try {
      TTLRequest ttlRequest = TTLRequest.readFrom(new DataInputStream(request.getInputStream()));
      MessageFormatInputStream stream = new TTLMessageFormatInputStream(ttlRequest.getBlobId(), ttlRequest.getNewTTL());
      MessageInfo info = new MessageInfo(ttlRequest.getBlobId(), stream.getSize(), ttlRequest.getNewTTL());
      ArrayList<MessageInfo> infoList = new ArrayList<MessageInfo>();
      infoList.add(info);
      MessageFormatWriteSet writeset = new MessageFormatWriteSet(stream, infoList);
      blobStore.updateTTL(writeset);
      TTLResponse response = new TTLResponse(ttlRequest.getCorrelationId(), ttlRequest.getClientId(), (short)0);
      requestResponseChannel.sendResponse(response, request);
    }
    catch (Exception e) {

    }
  }
}