package com.github.ambry.network;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * RequestResponseHandler that polls a {@link Requestor} for requests that is to be sent across the network. It
 * internally uses a {@link Selector} to create channels, and multiplexes over it to send requests and receive
 * responses.
 */

public class RequestResponseHandler implements Runnable {
  private final Requestor requestor;
  private final Selector selector;
  private final MetricRegistry registry;
  private final NetworkMetrics metrics;
  private final SSLFactory factory;
  private final Time time;
  private final Thread requestResponseHandlerThread;
  private AtomicBoolean isRunning;
  private final CountDownLatch shutDownLatch;
  // @todo: these numbers need to be determined.
  private static final int POLL_TIMEOUT_MS = 1 * Time.MsPerSec;
  private static final int BUFFER_SIZE = 4 * 1024;
  private static final Logger logger = LoggerFactory.getLogger(RequestResponseHandler.class);

  public RequestResponseHandler(Requestor requestor, MetricRegistry registry, SSLFactory factory, Time time)
      throws IOException {
    this.requestor = requestor;
    this.registry = registry;
    this.metrics = new NetworkMetrics(registry);
    this.factory = factory;
    this.time = time;
    this.isRunning = new AtomicBoolean(true);
    this.shutDownLatch = new CountDownLatch(1);
    requestResponseHandlerThread = Utils.newThread("RequestResponseHandlerThread", this, true);
    this.selector = new Selector(metrics, time, factory);
  }

  public void start()
      throws IOException {
    requestResponseHandlerThread.start();
  }

  /**
   * Close the requestResponseHandler.
   * @throws InterruptedException
   */
  public void close()
      throws InterruptedException {
    if (isRunning.compareAndSet(true, false)) {
      shutDownLatch.await();
      selector.close();
    }
  }

  /**
   * Initiate a connection to the given host at the given port.
   * @param host the host to which the connection is to be initiated.
   * @param port the port on the host to which the connection is to be initiated.
   * @return a connection id for the initiated connection.
   * @throws IOException
   */
  public String connect(String host, Port port)
      throws IOException {
    return selector.connect(new InetSocketAddress(host, port.getPort()), BUFFER_SIZE, BUFFER_SIZE, port.getPortType());
  }

  @Override
  public void run() {
    Exception exception = null;
    try {
      while (isRunning.get()) {
        try {
          List<NetworkSend> sends = requestor.poll();
          selector.poll(POLL_TIMEOUT_MS, sends);
          requestor.onResponse(selector.connected(), selector.disconnected(), selector.completedSends(),
              selector.completedReceives());
        } catch (IOException e) {
          logger.error("Encountered IO Exception during poll, continuing.", e);
        } catch (Exception e) {
          exception = e;
          break;
        }
      }
    } finally {
      shutDownLatch.countDown();
      requestor.onClose(exception);
    }
  }
}
