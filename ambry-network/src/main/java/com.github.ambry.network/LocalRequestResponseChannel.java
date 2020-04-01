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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An implementation of {@link RequestResponseChannel} that buffers messages in local queues.
 * This class enables the Frontend router to call AmbryRequest methods in process.
 */
public class LocalRequestResponseChannel implements RequestResponseChannel {

  private static final Logger logger = LoggerFactory.getLogger(LocalRequestResponseChannel.class);
  private BlockingQueue<NetworkRequest> requestQueue = new LinkedBlockingQueue<>();
  private Map<Integer, BlockingQueue<ResponseInfo>> responseMap = new ConcurrentHashMap<>();
  // buffer to hold size header that we strip off payloads. Only use this array to discard bytes since it is shared.
  private static final byte[] SIZE_BYTE_ARRAY = new byte[Long.BYTES];
  private static final ResponseInfo WAKEUP_MARKER = new ResponseInfo(null, null, null);

  @Override
  public void sendRequest(NetworkRequest request) {
    requestQueue.offer(request);
    if (request instanceof LocalChannelRequest) {
      LocalChannelRequest localRequest = (LocalChannelRequest) request;
      logger.debug("Added request for {}, queue size now {}", localRequest.processorId, requestQueue.size());
    }
  }

  @Override
  public NetworkRequest receiveRequest() throws InterruptedException {
    NetworkRequest request = requestQueue.take();
    if (request instanceof LocalChannelRequest) {
      LocalChannelRequest localRequest = (LocalChannelRequest) request;
      logger.debug("Removed request for {}, queue size now {}", localRequest.processorId, requestQueue.size());
    }
    return request;
  }

  @Override
  public void sendResponse(Send payloadToSend, NetworkRequest originalRequest, ServerNetworkResponseMetrics metrics) {
    try {
      LocalChannelRequest localRequest = (LocalChannelRequest) originalRequest;
      ResponseInfo responseInfo = new ResponseInfo(localRequest.requestInfo, null, byteBufFromPayload(payloadToSend));
      BlockingQueue<ResponseInfo> responseQueue = getResponseQueue(localRequest.processorId);
      responseQueue.put(responseInfo);
      logger.debug("Added response for {}, size now {}", localRequest.processorId, responseQueue.size());
    } catch (IOException | InterruptedException ex) {
      logger.error("Could not extract response", ex);
    }
  }

  /**
   * Receive all queued responses corresponding to requests matching a processor id.
   * @param processorId the processor id to match.
   * @param pollTimeoutMs the poll timeout in msec.
   * @return the applicable responses.
   */
  public List<ResponseInfo> receiveResponses(int processorId, int pollTimeoutMs) {
    BlockingQueue<ResponseInfo> responseQueue = getResponseQueue(processorId);
    ResponseInfo firstResponse = null;
    try {
      firstResponse = responseQueue.poll(pollTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ie) {
      logger.debug("Interrupted polling responses for {}", processorId);
    }
    if (firstResponse == null) {
      return Collections.emptyList();
    }
    List<ResponseInfo> responseList = new LinkedList<>();
    responseList.add(firstResponse);
    responseQueue.drainTo(responseList);
    // remove any wakeup markers since they are only there to stop a timed poll early
    responseList.removeIf(responseInfo -> responseInfo == WAKEUP_MARKER);
    logger.debug("Returning {} responses for {}", responseList.size(), processorId);
    return responseList;
  }

  /**
   * @return the response list corresponding to a processor id.
   * @param processorId the processor id to match.
   */
  private BlockingQueue<ResponseInfo> getResponseQueue(int processorId) {
    return responseMap.computeIfAbsent(processorId, p -> new LinkedBlockingQueue<>());
  }

  @Override
  public void closeConnection(NetworkRequest request) throws InterruptedException {
  }

  @Override
  public void shutdown() {
  }

  /**
   * Wake up a blocking call to {@link #receiveResponses}. This can be called so that some other entity can do some work
   * on the main event loop even if no responses are received.
   * @param processorId the processor ID to wake up.
   */
  public void wakeup(int processorId) {
    // add a marker object to the queue to guarantee that a timed poll returns before the timeout
    getResponseQueue(processorId).add(WAKEUP_MARKER);
  }

  /**
   * Utility to extract a {@link ByteBuf} from a {@link Send} object, skipping the size header.
   * @param payload the payload whose bytes we want.
   */
  static ByteBuf byteBufFromPayload(Send payload) throws IOException {
    int bufferSize = (int) payload.sizeInBytes() - SIZE_BYTE_ARRAY.length;
    ByteBuf buffer = Unpooled.buffer(bufferSize);
    // Skip the size header
    payload.writeTo(new ByteBufferChannel(ByteBuffer.wrap(SIZE_BYTE_ARRAY)));
    WritableByteChannel byteChannel = Channels.newChannel(new ByteBufOutputStream(buffer));
    payload.writeTo(byteChannel);
    return buffer;
  }

  /**
   * A {@link NetworkRequest} implementation that works with {@link LocalRequestResponseChannel}.
   */
  static class LocalChannelRequest implements NetworkRequest {
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

    @Override
    public String toString() {
      return "LocalChannelRequest{" + "requestInfo=" + requestInfo + ", input=" + input + ", startTimeInMs="
          + startTimeInMs + ", processorId=" + processorId + '}';
    }
  }
}
