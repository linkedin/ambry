package com.github.ambry.network;

import com.github.ambry.config.NetworkConfig;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
  private final NetworkConfig networkConfig;
  private final NetworkMetrics networkMetrics;
  private final Time time;
  private final Thread requestResponseHandlerThread;
  private final AtomicBoolean isRunning = new AtomicBoolean(true);
  private final CountDownLatch shutDownLatch = new CountDownLatch(1);
  // @todo: these numbers need to be determined.
  private static final int POLL_TIMEOUT_MS = 30;
  private static final int SHUTDOWN_WAIT_MS = 10 * Time.MsPerSec;
  private static final Logger logger = LoggerFactory.getLogger(RequestResponseHandler.class);

  public RequestResponseHandler(Requestor requestor, NetworkConfig networkConfig, NetworkMetrics networkMetrics,
      SSLFactory sslFactory, Time time)
      throws IOException {
    this.requestor = requestor;
    this.networkConfig = networkConfig;
    this.networkMetrics = networkMetrics;
    this.time = time;
    requestResponseHandlerThread = Utils.newThread("RequestResponseHandlerThread", this, true);
    selector = new Selector(networkMetrics, time, sslFactory);
  }

  /**
   * Start the RequestResponseHandler thread.
   */
  public void start() {
    requestResponseHandlerThread.start();
  }

  /**
   * Shut down the requestResponseHandler.
   * @throws InterruptedException
   */
  public void shutDown()
      throws InterruptedException {
    if (isRunning.compareAndSet(true, false)) {
      if (!shutDownLatch.await(SHUTDOWN_WAIT_MS, TimeUnit.MILLISECONDS)) {
        logger.error("RequestResponseHandler did not shut down gracefully, forcing shut down");
      }
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
    return selector.connect(new InetSocketAddress(host, port.getPort()), networkConfig.socketSendBufferBytes,
        networkConfig.socketReceiveBufferBytes, port.getPortType());
  }

  @Override
  public void run() {
    Exception exception = null;
    try {
      while (isRunning.get()) {
        List<NetworkSend> sends = requestor.poll();
        selector.poll(POLL_TIMEOUT_MS, sends);
        requestor.onResponse(selector.connected(), selector.disconnected(), selector.completedSends(),
            selector.completedReceives());
      }
    } catch (Exception e) {
      logger.error("Encountered Exception during poll, continuing.", e);
      exception = e;
    } finally {
      shutDownLatch.countDown();
      requestor.onRequestResponseHandlerShutDown(exception);
    }
  }
}
