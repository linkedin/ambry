package com.github.ambry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created with IntelliJ IDEA.
 * User: srsubram
 * Date: 10/15/13
 * Time: 12:45 AM
 * To change this template use File | Settings | File Templates.
 */
public class RequestHandler implements Runnable {
  private final int id;
  private final RequestResponseChannel requestChannel;
  private final AmbryRequests requests;
  private Logger logger = LoggerFactory.getLogger(getClass());

  public RequestHandler(int id, RequestResponseChannel requestChannel, AmbryRequests requests) {
    this.id = id;
    this.requestChannel = requestChannel;
    this.requests = requests;
  }

  public void run() {
    while(true) {
      try {
        Request req = requestChannel.receiveRequest();
        if(req.equals(EmptyRequest.getInstance())) {
          logger.debug("Request handler " + id + " received shut down command");
          return;
        }
        requests.handleRequests(req);
        logger.trace("Request handler " + id + " handling request " + req);
      } catch (Exception e) {
        logger.error("Exception when handling request", e);
      }
    }
  }

  public void shutdown() throws InterruptedException {
    requestChannel.sendRequest(EmptyRequest.getInstance());
  }
}

class RequestHandlerPool {

  private Thread[] threads = null;
  private RequestHandler[] handlers = null;
  private Logger logger = LoggerFactory.getLogger(getClass());

  public RequestHandlerPool(int numThreads, RequestResponseChannel requestResponseChannel, AmbryRequests requests) {
    threads = new Thread[numThreads];
    handlers = new RequestHandler[numThreads];
    for(int i = 0; i < numThreads; i++) {
      handlers[i] = new RequestHandler(i, requestResponseChannel, requests);
      threads[i] = Utils.daemonThread("request-handler-" + i, handlers[i]);
      threads[i].start();
    }
  }

  public void shutdown() {
    try {
      logger.info("shutting down");
      for(RequestHandler handler: handlers)
        handler.shutdown();
      for(Thread thread : threads)
        thread.join();
      logger.info("shut down completely");
    } catch (Exception e) {
      logger.error("error when shutting down" + e);
    }
  }
}
