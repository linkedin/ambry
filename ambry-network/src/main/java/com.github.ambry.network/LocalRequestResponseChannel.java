/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.utils.ByteBufferChannel;
import com.github.ambry.utils.ByteBufferOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LocalRequestResponseChannel implements RequestResponseChannel {

  private static final Logger logger = LoggerFactory.getLogger(LocalRequestResponseChannel.class);
  private BlockingQueue<Request> requestQueue = new LinkedBlockingQueue<>();
  private Map<Integer, List<ResponseInfo>> responseMap = new ConcurrentHashMap<>();
  // buffer to hold size header that we strip off payloads
  private static final byte[] sizeByteArray = new byte[Long.BYTES];

  @Override
  public void sendResponse(Send payloadToSend, Request originalRequest, ServerNetworkResponseMetrics metrics) {
    try {
      LocalChannelRequest localChannelRequest = (LocalChannelRequest) originalRequest;
      ResponseInfo responseInfo =
          new ResponseInfo(localChannelRequest.requestInfo, null, byteBufferFromPayload(payloadToSend));
      getResponseList(localChannelRequest.processorId).add(responseInfo);
    } catch (IOException ex) {
      logger.error("Could not extract response", ex);
    }
  }

  public List<ResponseInfo> receiveResponses(int processorId) {
    List<ResponseInfo> responseList = getResponseList(processorId);
    synchronized (responseList) {
      List<ResponseInfo> result = new ArrayList<>(responseList);
      responseList.clear();
      return result;
    }
  }

  private List<ResponseInfo> getResponseList(int processorId) {
    return responseMap.computeIfAbsent(processorId, p -> new ArrayList<>());
  }

  @Override
  public void sendRequest(Request request) throws InterruptedException {
    requestQueue.offer(request);
  }

  @Override
  public Request receiveRequest() throws InterruptedException {
    Request request = requestQueue.take();
    // Note: need to read inputstream past size header, to simulate BoundedByteBufferReceive
    /*
    try {
      long requestSize = new DataInputStream(request.getInputStream()).readLong();
      logger.trace("Got request of size {}", requestSize);
    } catch (IOException ex) {
      throw new IllegalStateException("Could not read size from request buffer");
    }*/
    return request;
  }

  @Override
  public void closeConnection(Request request) throws InterruptedException {
  }

  @Override
  public void shutdown() {
  }

  /**
   * Utility to extract a byte buffer from a {@link Send} object, skipping the size header.
   * @param payload the payload whose bytes we want.
   */
  static ByteBuffer byteBufferFromPayload(Send payload) throws IOException {
    int bufferSize = (int) payload.sizeInBytes() - sizeByteArray.length;
    ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
    // Skip the size header
    long bytesWritten = payload.writeTo(new ByteBufferChannel(ByteBuffer.wrap(sizeByteArray)));
    WritableByteChannel byteChannel = Channels.newChannel(new ByteBufferOutputStream(buffer));
    payload.writeTo(byteChannel);
    buffer.rewind();
    return buffer;
  }

  static class LocalChannelRequest implements Request {
    private RequestInfo requestInfo;
    private InputStream input;
    private long startTimeInMs;
    private int processorId;

    LocalChannelRequest(RequestInfo requestInfo, int processorId, InputStream input) {
      this.requestInfo = requestInfo;
      this.processorId = processorId;
      this.input = input;
      startTimeInMs = System.currentTimeMillis();
    }

    @Override
    public InputStream getInputStream() {
      return input;
    }

    @Override
    public long getStartTimeInMs() {
      return startTimeInMs;
    }
  }
}
