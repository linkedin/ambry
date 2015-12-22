package com.github.ambry.network;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Time;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * Tests the functionality of {@link RequestResponseHandler}
 */
public class RequestResponseHandlerTest {
  static List<NetworkSend> requests;
  Object requestListlock = new Object();
  private SocketServer server1 = null;
  private SocketServer server2 = null;
  private static File trustStoreFile = null;
  private static SSLFactory sslFactory;
  private static SSLConfig sslConfig;
  private static SSLConfig serverSSLConfig1;
  private static SSLConfig serverSSLConfig2;
  private CountDownLatch connectComplete;
  private CountDownLatch sendComplete;

  /**
   * Starts up SocketServers to which connections will be made.
   * @throws IOException
   * @throws InterruptedException
   */
  public RequestResponseHandlerTest()
      throws IOException, InterruptedException {
    Properties props = new Properties();
    props.setProperty("port", "6667");
    VerifiableProperties propverify = new VerifiableProperties(props);
    NetworkConfig config = new NetworkConfig(propverify);
    ArrayList<Port> ports = new ArrayList<Port>();
    ports.add(new Port(6667, PortType.PLAINTEXT));
    ports.add(new Port(7667, PortType.SSL));
    server1 = new SocketServer(config, serverSSLConfig1, new MetricRegistry(), ports);
    server1.start();
    props.setProperty("port", "6668");
    propverify = new VerifiableProperties(props);
    config = new NetworkConfig(propverify);
    ports = new ArrayList<Port>();
    ports.add(new Port(6668, PortType.PLAINTEXT));
    ports.add(new Port(7668, PortType.SSL));
    server2 = new SocketServer(config, serverSSLConfig2, new MetricRegistry(), ports);
    server2.start();
  }

  /**
   * Run only once for all tests
   */
  @BeforeClass
  public static void initializeTests()
      throws Exception {
    trustStoreFile = File.createTempFile("truststore", ".jks");
    serverSSLConfig1 = TestSSLUtils.createSSLConfig("DC2,DC3", SSLFactory.Mode.SERVER, trustStoreFile, "server1");
    serverSSLConfig2 = TestSSLUtils.createSSLConfig("DC1,DC3", SSLFactory.Mode.SERVER, trustStoreFile, "server2");
    sslConfig = TestSSLUtils.createSSLConfig("DC1,DC2,DC3", SSLFactory.Mode.CLIENT, trustStoreFile, "client");
    sslFactory = new SSLFactory(sslConfig);
  }

  @Test
  public void testRequestResponseHandler()
      throws IOException, InterruptedException {
    MockRequestor mockRequestor = new MockRequestor();
    RequestResponseHandler requestResponseHandler =
        new RequestResponseHandler(mockRequestor, new MetricRegistry(), sslFactory, new MockTime());
    requestResponseHandler.start();
    connectComplete = new CountDownLatch(3);
    String conn1 = requestResponseHandler.connect("localhost", new Port(6667, PortType.PLAINTEXT));
    String conn2 = requestResponseHandler.connect("localhost", new Port(6667, PortType.PLAINTEXT));
    String conn3 = requestResponseHandler.connect("localhost", new Port(6668, PortType.PLAINTEXT));
    connectComplete.await();

    Time time = new MockTime();
    List<NetworkSend> requests = new ArrayList<NetworkSend>();
    requests.add(new NetworkSend(conn1, new MockSend(ByteBuffer.allocate(16)), null, time));
    requests.add(new NetworkSend(conn2, new MockSend(ByteBuffer.allocate(16)), null, time));
    requests.add(new NetworkSend(conn3, new MockSend(ByteBuffer.allocate(16)), null, time));

    sendComplete = new CountDownLatch(3);
    mockRequestor.setRequests(requests);
    sendComplete.await();

    requestResponseHandler.shutDown();
    server1.shutdown();
    server2.shutdown();
  }

  class MockRequestor implements Requestor {
    /**
     * Set the list of requests to be returned in the next poll()
     * @param mockRequests the list of requests to be returned in the next poll()
     */
    public void setRequests(List<NetworkSend> mockRequests) {
      synchronized (requestListlock) {
        requests = mockRequests;
      }
    }

    /**
     * Return previously set list of requests.
     * @return list of {@link NetworkSend}
     */
    @Override
    public List<NetworkSend> poll() {
      synchronized (requestListlock) {
        List<NetworkSend> requestsToReturn = requests;
        requests = null;
        return requestsToReturn;
      }
    }

    /**
     * Notifies the Requestor about Network events
     * @param connected a list of connection ids for any connections established.
     * @param disconnected a list of connection ids for any disconnections.
     * @param completedSends a list of {@link NetworkSend} for requests that were successufully sent out.
     * @param completedReceives a list of {@link NetworkReceive} for responses successfully received.
     */
    @Override
    public void onResponse(List<String> connected, List<String> disconnected, List<NetworkSend> completedSends,
        List<NetworkReceive> completedReceives) {
      for (String s: connected) {
        connectComplete.countDown();
      }
      for (NetworkSend send: completedSends) {
        sendComplete.countDown();
      }
    }

    /**
     * Notifies the Requestor of any exception encountered.
     * @param e the exception encountered.
     */
    @Override
    public void onException(Exception e) {
      Assert.assertTrue("Exception received: " + e.getMessage(), false);
    }
  }

  class MockSend implements Send {
    private ByteBuffer buffer;
    private int size;

    public MockSend(ByteBuffer buffer) {
      this.buffer = buffer;
      size = buffer.remaining();
    }

    @Override
    public long writeTo(WritableByteChannel channel)
        throws IOException {
      long written = channel.write(buffer);
      return written;
    }

    @Override
    public boolean isSendComplete() {
      return buffer.remaining() == 0;
    }

    @Override
    public long sizeInBytes() {
      return size;
    }
  }
}
