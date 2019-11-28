package com.github.ambry.network;

import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import java.io.InputStream;


// The request at the network layer
public class NettyServerRequest implements NetworkRequest {
  public InputStream inputStream;
  public RestResponseChannel restResponseChannel;
  public RestRequest restRequest;

  public NettyServerRequest(RestRequest restRequest, RestResponseChannel restResponseChannel, InputStream inputStream) {
    this.restRequest = restRequest;
    this.restResponseChannel = restResponseChannel;
    this.inputStream = inputStream;
  }

  @Override
  public InputStream getInputStream() {
    return inputStream;
  }

  @Override
  public long getStartTimeInMs() {
    return 0;
  }

  public RestRequest getRestRequest() {
    return restRequest;
  }

  public RestResponseChannel getRestResponseChannel() {
    return restResponseChannel;
  }
}

