package com.github.ambry.network;

import com.github.ambry.metrics.MetricsHistogram;
import com.github.ambry.utils.SystemTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.io.IOException;


// The request at the network layer
class SocketServerRequest implements Request {
  private final int processor;
  private final Object requestKey;
  private final InputStream input;
  private final long startTimeInMs;
  private Logger logger = LoggerFactory.getLogger(getClass());

  public SocketServerRequest(int processor, Object requestKey, InputStream input)
      throws IOException {
    this.processor = processor;
    this.requestKey = requestKey;
    this.input = input;
    this.startTimeInMs = SystemTime.getInstance().milliseconds();
    logger.trace("Processor {} received request : {}", processor, requestKey);
  }

  @Override
  public InputStream getInputStream() {
    return input;
  }

  @Override
  public long getStartTimeInMs() {
    return startTimeInMs;
  }

  public int getProcessor() {
    return processor;
  }

  public Object getRequestKey() {
    return requestKey;
  }
}

// The response at the network layer
class SocketServerResponse implements Response, NetworkSend {

  private final int processor;
  private final Request request;
  private final Send output;
  private final NetworkRequestMetrics metrics;
  private long startQueueTimeInMs;
  private long startSendTimeInMs;

  public SocketServerResponse(Request request, Send output, NetworkRequestMetrics metrics) {
    this.request = request;
    this.output = output;
    this.processor = ((SocketServerRequest) request).getProcessor();
    this.metrics = metrics;
  }

  @Override
  public String getConnectionId() {
    return null;
  }

  public Send getPayload() {
    return output;
  }

  public Request getRequest() {
    return request;
  }

  public int getProcessor() {
    return processor;
  }

  public void onEnqueueIntoResponseQueue() {
    this.startQueueTimeInMs = SystemTime.getInstance().milliseconds();
  }

  public void onSendStart() {
    this.startSendTimeInMs = SystemTime.getInstance().milliseconds();
  }

  public void onDequeueFromResponseQueue() {
    if (metrics != null) {
      metrics.updateResponseQueueTime(SystemTime.getInstance().milliseconds() - startQueueTimeInMs);
    }
  }

  public void onSendComplete() {
    if (metrics != null) {
      metrics.updateResponseSendTime(SystemTime.getInstance().milliseconds() - startSendTimeInMs);
    }
  }

  @Override
  public long getSendStartTimeInMs() {
    return startSendTimeInMs;
  }
}

interface ResponseListener {
  public void onResponse(int processorId);
}

/**
 * RequestResponse channel for socket server
 */
public class SocketRequestResponseChannel implements RequestResponseChannel {
  private final int numProcessors;
  private final int queueSize;
  private final ArrayBlockingQueue<Request> requestQueue;
  private final ArrayList<BlockingQueue<Response>> responseQueues;
  private final ArrayList<ResponseListener> responseListeners;

  public SocketRequestResponseChannel(int numProcessors, int queueSize) {
    this.numProcessors = numProcessors;
    this.queueSize = queueSize;
    this.requestQueue = new ArrayBlockingQueue<Request>(this.queueSize);
    responseQueues = new ArrayList<BlockingQueue<Response>>(this.numProcessors);
    responseListeners = new ArrayList<ResponseListener>();

    for (int i = 0; i < this.numProcessors; i++) {
      responseQueues.add(i, new LinkedBlockingQueue<Response>());
    }
  }

  /** Send a request to be handled, potentially blocking until there is room in the queue for the request */
  @Override
  public void sendRequest(Request request)
      throws InterruptedException {
    requestQueue.put(request);
  }

  /** Send a response back to the socket server to be sent over the network */
  @Override
  public void sendResponse(Send payloadToSend, Request originalRequest, NetworkRequestMetrics metrics)
      throws InterruptedException {
    SocketServerResponse response = new SocketServerResponse(originalRequest, payloadToSend, metrics);
    response.onEnqueueIntoResponseQueue();
    responseQueues.get(response.getProcessor()).put(response);
    for (ResponseListener listener : responseListeners) {
      listener.onResponse(response.getProcessor());
    }
  }

  /**
   * Closes the connection and does not send any response
   */
  @Override
  public void closeConnection(Request originalRequest)
      throws InterruptedException {
    SocketServerResponse response = new SocketServerResponse(originalRequest, null, null);
    responseQueues.get(response.getProcessor()).put(response);
    for (ResponseListener listener : responseListeners) {
      listener.onResponse(response.getProcessor());
    }
  }

  /** Get the next request or block until there is one */
  @Override
  public Request receiveRequest()
      throws InterruptedException {
    return requestQueue.take();
  }

  /** Get a response for the given processor if there is one */
  public Response receiveResponse(int processor)
      throws InterruptedException {
    return responseQueues.get(processor).poll();
  }

  public void addResponseListener(ResponseListener listener) {
    responseListeners.add(listener);
  }

  public int getRequestQueueSize() {
    return requestQueue.size();
  }

  public int getResponseQueueSize(int processor) {
    return responseQueues.get(processor).size();
  }

  public int getNumberOfProcessors() {
    return numProcessors;
  }

  public void shutdown() {
    requestQueue.clear();
  }
}


