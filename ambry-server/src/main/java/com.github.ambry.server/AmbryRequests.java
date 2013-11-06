package com.github.ambry.server;

import java.io.DataInputStream;

import com.github.ambry.messageformat.MessageFormatInputStream;
import com.github.ambry.network.RequestResponseChannel;
import com.github.ambry.shared.*;
import com.github.ambry.network.Request;
import com.github.ambry.network.Send;
import com.github.ambry.messageformat.MessageFormatSend;
import com.github.ambry.messageformat.MessageFormatWriteSet;
import com.github.ambry.store.*;

import java.io.IOException;
import java.util.ArrayList;

/**
 * The main request implementation class. All requests to the server are
 * handled by this class
 */

public class AmbryRequests {

  private Store blobStore;
  private final RequestResponseChannel requestResponseChannel;

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
        default: // throw exception
      }
    } catch (Exception e) {
      // log and measure time
    }
  }

  private void handlePutRequest(Request request) throws IOException {
    try {
      PutRequest putRequest = PutRequest.readFrom(new DataInputStream(request.getInputStream()));
      MessageFormatInputStream stream = new MessageFormatInputStream(new BlobId(putRequest.getBlobId()),
                                                                     putRequest.getBlobProperties(),
                                                                     putRequest.getUsermetadata(),
                                                                     putRequest.getData(),
                                                                     putRequest.getBlobProperties().getBlobSize());
      MessageInfo info = new MessageInfo(new BlobId(putRequest.getBlobId()),
                                         stream.getSize(),
                                         putRequest.getBlobProperties().getTimeToLive());
      ArrayList<MessageInfo> infoList = new ArrayList<MessageInfo>();
      infoList.add(info);
      MessageFormatWriteSet writeset = new MessageFormatWriteSet(stream, infoList);
      blobStore.put(writeset);
      PutResponse response = new PutResponse((short)1, putRequest.getCorrelationId(), (short)0);
      requestResponseChannel.sendResponse(response, request);
    }
    catch (StoreException e) {

    }
    catch (Exception e) {

    }
  }

  private void handleGetRequest(Request request) {
    try {
      GetRequest getRequest = GetRequest.readFrom(new DataInputStream(request.getInputStream()));
      StoreInfo info = blobStore.get(getRequest.getBlobIds());
      Send blobsToSend = new MessageFormatSend(info.getMessageReadSet(), getRequest.getMessageFormatFlag());
      GetResponse response = new GetResponse((short)0, getRequest.getCorrelationId(), info.getMessageReadSetInfo(), blobsToSend);
      requestResponseChannel.sendResponse(response, request);
    }
    catch (Exception e) {
      // send error response
    }


  }
}