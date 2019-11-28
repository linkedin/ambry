package com.github.ambry.server;

import com.github.ambry.commons.CopyingAsyncWritableChannel;
import com.github.ambry.network.NettyServerRequest;
import com.github.ambry.network.NettyServerRequestResponseChannel;
import com.github.ambry.rest.BlobStorageService;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;


public class ServerBlobStorageService implements BlobStorageService {
  NettyServerRequestResponseChannel requestResponseChannel;

  public ServerBlobStorageService(NettyServerRequestResponseChannel requestResponseChannel) {
    this.requestResponseChannel = requestResponseChannel;
  }

  @Override
  public void start() throws InstantiationException {

  }

  @Override
  public void shutdown() {

  }

  @Override
  public void handleGet(RestRequest restRequest, RestResponseChannel restResponseChannel) {
    System.out.println("get");
  }

  @Override
  public void handlePost(RestRequest restRequest, RestResponseChannel restResponseChannel) {
    System.out.println("post");
    CopyingAsyncWritableChannel asyncWritableChannel = new CopyingAsyncWritableChannel();
    restRequest.readInto(asyncWritableChannel, (result, exception) -> {
      System.out.println("post length " + result);
      requestResponseChannel.sendRequest(
          new NettyServerRequest(restRequest, restResponseChannel, asyncWritableChannel.getContentAsInputStream()));
    });
  }

  @Override
  public void handlePut(RestRequest restRequest, RestResponseChannel restResponseChannel) {
    System.out.println("put");
  }

  @Override
  public void handleDelete(RestRequest restRequest, RestResponseChannel restResponseChannel) {

  }

  @Override
  public void handleHead(RestRequest restRequest, RestResponseChannel restResponseChannel) {

  }

  @Override
  public void handleOptions(RestRequest restRequest, RestResponseChannel restResponseChannel) {

  }
}