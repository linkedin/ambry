package com.github.ambry.server;

import java.io.DataInputStream;
import com.github.ambry.shared.PutRequest;
import com.github.ambry.shared.GetRequest;
import com.github.ambry.shared.GetResponse;
import com.github.ambry.shared.PutResponse;
import com.github.ambry.network.Request;
import com.github.ambry.network.RequestResponseChannel;

import java.io.IOException;

/**
 * The main request implementation class. All requests to the server are
 * handled by this class
 */

public class AmbryRequests {
  private final RequestResponseChannel requestChannel;
  private static final short PutRequestType = 1;

  public AmbryRequests(RequestResponseChannel requestChannel) {
    this.requestChannel = requestChannel;
  }

  public void handleRequests(Request request) {
    try {
      // log
      DataInputStream stream = (DataInputStream)request.getInputStream();
      switch (stream.readShort()) {
        case PutRequestType:
          handlePutRequest(request);
          break;
        default: // throw exception
      }
    } catch (Exception e) {
      // log and measure time
    }
  }

  private void handlePutRequest(Request request) throws IOException {
    PutRequest putRequest = PutRequest.readFrom((DataInputStream)request.getInputStream());
    // need store manager to get store

    // issue append

    // create put response

    // send to request channel
  }

  private void handleGetRequest(Request request) throws IOException {
    GetRequest getRequest = GetRequest.readFrom((DataInputStream)request.getInputStream());
  }
}