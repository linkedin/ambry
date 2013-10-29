package com.github.ambry;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.io.IOException;

class SocketServerRequest implements Request {
  private final int processor;
  private final Object requestKey;
  private final InputStream input;
  private Logger logger = LoggerFactory.getLogger(getClass());

  public SocketServerRequest(int processor, Object requestKey, InputStream input) throws IOException {
    this.processor = processor;
    this.requestKey = requestKey;
    this.input = input;
    logger.trace("Processor {} received request : {}", processor, requestKey);
  }

  public InputStream getInputStream() {
    return input;
  }

  public int getProcessor() {
    return processor;
  }

  public Object getRequestKey() {
    return requestKey;
  }
}

class SocketServerResponse implements Response {

  private final int processor;
  private final Request request;
  private final Send output;

  public SocketServerResponse(Request request, Send output) {
    this.request = request;
    this.output = output;
    this.processor = ((SocketServerRequest)request).getProcessor();
  }

  public Send getOutput() {
    return output;
  }

  public Request getRequest() {
    return request;
  }

  public int getProcessor() {
    return processor;
  }
}

/**
 * RequestResponse channel for socket server
 */
public class SocketRequestResponseChannel implements RequestResponseChannel{
  private final int numProcessors;
  private final int queueSize;
  private final ArrayBlockingQueue<Request> requestQueue;
  private final ArrayList<BlockingQueue<Response>> responseQueues;

  public SocketRequestResponseChannel(int numProcessors, int queueSize) {
    this.numProcessors = numProcessors;
    this.queueSize = queueSize;
    this.requestQueue = new ArrayBlockingQueue<Request>(this.queueSize);
    responseQueues = new ArrayList<BlockingQueue<Response>>(this.numProcessors);

    for(int i = 0; i < this.numProcessors; i++)
      responseQueues.add(i, new LinkedBlockingQueue<Response>());
  }

  @Override
  /** Send a request to be handled, potentially blocking until there is room in the queue for the request */
  public void sendRequest(Request request) throws InterruptedException {
    requestQueue.put(request);
  }

  /** Send a response back to the socket server to be sent over the network */
  public void sendResponse(Response response) throws InterruptedException {
    responseQueues.get(((SocketServerResponse)response).getProcessor()).put(response);
  }

  /** Get the next request or block until there is one */
  public Request receiveRequest() throws InterruptedException {
    return requestQueue.take();
  }

  /** Get a response for the given processor if there is one */
  public Response receiveResponse(int processor) throws InterruptedException {
    return responseQueues.get(processor).poll();
  }

  public void shutdown() {
    requestQueue.clear();
  }
}


