package com.github.ambry.network;

import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RequestResponseHandler implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(Selector.class);
  private final Requestor requestor;
  private final Selector selector;
  private final NetworkMetrics metrics;
  private Thread requestResponseHandlerThread;
  private boolean isRunning;
  private final CountDownLatch shutDownLatch;
  private final int POLL_TIMEOUT_MS = 1 * Time.MsPerSec; // @todo

  public RequestResponseHandler(Requestor requestor, NetworkMetrics metrics, SSLFactory factory, Time time)
      throws IOException {
    this.requestor = requestor;
    this.selector = new Selector(metrics, time, factory);
    this.isRunning = true;
    this.shutDownLatch = new CountDownLatch(1);
    this.metrics = metrics;
  }

  public void start() {
    requestResponseHandlerThread = Utils.newThread("RequestResponseHandlerThread", this, true);
  }

  public void shutDown()
      throws InterruptedException {
    isRunning = false;
    shutDownLatch.await();
  }

  @Override
  public void run() {
    try {
      while (isRunning) {
        try {
          List<NetworkSend> sends = requestor.poll();
          selector.poll(POLL_TIMEOUT_MS, sends);
          requestor.onResponse(selector.connected(), selector.disconnected(), selector.completedSends(),
              selector.completedReceives());
        } catch (IOException e) {
          logger.error("Encountered IO Exception during poll", e);
        }
      }
    } finally {
      isRunning = false;
      shutDownLatch.countDown();
    }
  }
}
