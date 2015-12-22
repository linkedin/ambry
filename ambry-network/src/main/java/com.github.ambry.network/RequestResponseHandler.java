package com.github.ambry.network;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RequestResponseHandler implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(RequestResponseHandler.class);
  private final Requestor requestor;
  private Selector selector;
  private final MetricRegistry registry;
  private final NetworkMetrics metrics;
  private SSLFactory factory;
  private Time time;
  private Thread requestResponseHandlerThread;
  private boolean isRunning;
  private final CountDownLatch shutDownLatch;
  // @todo: these numbers need to be determined.
  private final int POLL_TIMEOUT_MS = 1 * Time.MsPerSec;
  private static int BUFFER_SIZE = 4 * 1024;

  public RequestResponseHandler(Requestor requestor, MetricRegistry registry, SSLFactory factory, Time time) {
    this.requestor = requestor;
    this.registry = registry;
    this.metrics = new NetworkMetrics(registry);
    this.factory = factory;
    this.time = time;
    this.isRunning = true;
    this.shutDownLatch = new CountDownLatch(1);
  }

  public void start()
      throws IOException {
    this.selector = new Selector(metrics, time, factory);
    requestResponseHandlerThread = Utils.newThread("RequestResponseHandlerThread", this, true);
    requestResponseHandlerThread.start();
  }

  public void shutDown()
      throws InterruptedException {
    isRunning = false;
    shutDownLatch.await();
    selector.close();
  }

  public String connect(String host, Port port)
      throws IOException {
    return selector.connect(new InetSocketAddress(host, port.getPort()), BUFFER_SIZE, BUFFER_SIZE, port.getPortType());
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
          logger.error("Encountered IO Exception during poll, continuing.", e);
        } catch (Exception e) {
          requestor.onException(e);
          break;
        }
      }
    } finally {
      isRunning = false;
      shutDownLatch.countDown();
    }
  }
}
