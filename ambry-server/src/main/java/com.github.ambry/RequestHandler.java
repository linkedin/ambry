package com.github.ambry;

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
          // log
          return;
        }
        requests.handleRequests(req);
      } catch (Exception e) {
        // log
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
      for(RequestHandler handler: handlers)
        handler.shutdown();
      for(Thread thread : threads)
        thread.join();
    } catch (Exception e) {
    // log
    }
  }
}
