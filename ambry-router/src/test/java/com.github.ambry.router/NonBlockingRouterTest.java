package com.github.ambry.router;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.network.NetworkMetrics;
import com.github.ambry.network.NetworkReceive;
import com.github.ambry.network.NetworkSend;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.network.SSLFactory;
import com.github.ambry.network.Send;
import com.github.ambry.network.SocketServer;
import com.github.ambry.network.TestSSLUtils;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import org.junit.Assert;
import org.junit.Test;


/**
 * Class to test the {@link NonBlockingRouter}
 */
public class NonBlockingRouterTest {

  /**
   * Test Router with a single scaling unit.
   */
  @Test
  public void testRouterBasic()
      throws Exception {
    Properties props = RouterTestUtils.getProps();
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    MockClusterMap mockClusterMap = new MockClusterMap();
    BlobProperties putBlobProperties =
        new BlobProperties(100, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time);
    Random random = new Random();
    byte[] putUserMetadata = new byte[10];
    random.nextBytes(putUserMetadata);
    byte[] putContent = new byte[100];
    random.nextBytes(putContent);
    ReadableStreamChannel putChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(putContent));

    Router router =
        new NonBlockingRouterFactory(verifiableProperties, mockClusterMap, new LoggingNotificationSystem()).getRouter();

    //tests to be added when puts are implemented.
    router.putBlob(putBlobProperties, putUserMetadata, putChannel);
    router.getBlob("nonExistentblobId");
    router.deleteBlob("nonExistentblobId");
    router.close();

    //submission after closing should return a future that is already done.
    Future<String> future = router.putBlob(putBlobProperties, putUserMetadata, putChannel);
    Assert.assertTrue(future.isDone());
  }

  /**
   * Test that multiple scaling units can be instantiated, closed, and that closing one will close the router.
   */
  @Test
  public void testMultipleScalingUnit()
      throws Exception {
    Properties props = RouterTestUtils.getProps();
    props.setProperty("router.scaling.unit.count", "3");
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    MockClusterMap mockClusterMap = new MockClusterMap();
    BlobProperties putBlobProperties =
        new BlobProperties(100, "serviceId", "memberId", "contentType", false, Utils.Infinite_Time);
    Random random = new Random();
    byte[] putUserMetadata = new byte[10];
    random.nextBytes(putUserMetadata);
    byte[] putContent = new byte[100];
    random.nextBytes(putContent);
    ReadableStreamChannel putChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(putContent));

    NonBlockingRouter router = (NonBlockingRouter) new NonBlockingRouterFactory(verifiableProperties, mockClusterMap,
        new LoggingNotificationSystem()).getRouter();

    //tests to be added when puts are implemented.
    router.putBlob(putBlobProperties, putUserMetadata, putChannel);
    router.close();

    //submission after closing should return a future that is already done.
    Future<String> future = router.putBlob(putBlobProperties, putUserMetadata, putChannel);
    Assert.assertTrue(future.isDone());
  }

  /**
   * Starts up SocketServers, makes connections and sends requests.
   * @throws Exception
   */
  @Test
  public void testRequestHandling()
      throws Exception {
    File trustStoreFile = File.createTempFile("truststore", ".jks");
    SSLConfig serverSSLConfig1 = TestSSLUtils.createSSLConfig("DC1,DC2,DC3", SSLFactory.Mode.SERVER,  trustStoreFile, "server1");
    SSLConfig serverSSLConfig2 = TestSSLUtils.createSSLConfig("DC1,DC2,DC3", SSLFactory.Mode.SERVER,  trustStoreFile, "server2");

    //Start up the SocketServers
    Properties serverProps = new Properties();
    serverProps.setProperty("port", "6667");
    VerifiableProperties serverVProps = new VerifiableProperties(serverProps);
    NetworkConfig config = new NetworkConfig(serverVProps);
    ArrayList<Port> ports = new ArrayList<Port>();
    ports.add(new Port(6667, PortType.PLAINTEXT));
    ports.add(new Port(7667, PortType.SSL));
    SocketServer server1 = new SocketServer(config, serverSSLConfig1, new MetricRegistry(), ports);
    server1.start();

    serverProps.setProperty("port", "6668");
    serverVProps = new VerifiableProperties(serverProps);
    config = new NetworkConfig(serverVProps);
    ports = new ArrayList<Port>();
    ports.add(new Port(6668, PortType.PLAINTEXT));
    ports.add(new Port(7668, PortType.SSL));
    SocketServer server2 = new SocketServer(config, serverSSLConfig2, new MetricRegistry(), ports);
    server2.start();

    Properties props = RouterTestUtils.getProps();
    TestSSLUtils.addSSLProperties(props, "DC1,DC2,DC3", SSLFactory.Mode.CLIENT, trustStoreFile, "client");
    VerifiableProperties verifiableProperties = new VerifiableProperties((props));
    MockClusterMap mockClusterMap = new MockClusterMap();
    NonBlockingRouterWrapper router =
        (NonBlockingRouterWrapper) new NonBlockingRouterFactoryWrapper(verifiableProperties, mockClusterMap,
            new LoggingNotificationSystem()).getRouter();

    //@todo: Add SSL tests.
    ArrayList<MockRequest> mockRequests = new ArrayList<MockRequest>();
    mockRequests.add(new MockRequest(ByteBuffer.allocate(16), "localhost", new Port(6667, PortType.PLAINTEXT)));
    mockRequests.add(new MockRequest(ByteBuffer.allocate(16), "localhost", new Port(6668, PortType.PLAINTEXT)));
    mockRequests.add(new MockRequest(ByteBuffer.allocate(16), "localhost", new Port(6667, PortType.PLAINTEXT)));

    router.sendRequests(mockRequests);

    router.close();

    server1.shutdown();
    server2.shutdown();
  }
}

/**
 * A wrapper over {@link NonBlockingRouterFactory} that simply returns a {@link NonBlockingRouterWrapper}
 * instead.
 */
class NonBlockingRouterFactoryWrapper extends NonBlockingRouterFactory {
  NonBlockingRouterFactoryWrapper(VerifiableProperties verifiableProperties, ClusterMap clusterMap,
      NotificationSystem notificationSystem)
      throws Exception {
    super(verifiableProperties, clusterMap, notificationSystem);
  }

  @Override
  public Router getRouter()
      throws InstantiationException {
    try {
      return new NonBlockingRouterWrapper(routerConfig, routerMetrics, networkConfig, networkMetrics, sslFactory,
          notificationSystem, clusterMap, time);
    } catch (IOException e) {
      throw new InstantiationException();
    }
  }
}

/**
 * A wrapper over {@link NonBlockingRouter} that overrides the poll() method of the internal {@link OperationController}
 * to send {@link MockRequest} requests and exposes a few methods for testing.
 */
class NonBlockingRouterWrapper extends NonBlockingRouter {
  OperationController oc;
  Object requestListlock = new Object();
  List<NetworkSend> requests;
  private CountDownLatch connectComplete;
  private CountDownLatch sendComplete;

  NonBlockingRouterWrapper(RouterConfig routerConfig, NonBlockingRouterMetrics routerMetrics,
      NetworkConfig networkConfig, NetworkMetrics networkMetrics, SSLFactory sslFactory,
      NotificationSystem notificationSystem, ClusterMap clusterMap, Time time)
      throws IOException {
    super(routerConfig, routerMetrics, networkConfig, networkMetrics, sslFactory, notificationSystem, clusterMap, time);
    oc = new TestOperationController();
  }

  @Override
  protected OperationController getOperationController() {
    return oc;
  }

  public void sendRequests(List<MockRequest> mockRequests)
      throws Exception {
    MockTime time = new MockTime();
    connectComplete = new CountDownLatch(mockRequests.size());
    synchronized (requestListlock) {
      for (MockRequest request : mockRequests) {
        // this will just initiate the connections
        oc.connectionManager.checkOutConnection(request.host, request.port);
      }
    }
    // wait till the connections are established
    connectComplete.await();
    synchronized (requestListlock) {
      requests = new ArrayList<NetworkSend>();
      for (MockRequest request : mockRequests) {
        // the connections have been established now, so checkout should return valid ids.
        String connId = oc.connectionManager.checkOutConnection(request.host, request.port);
        requests.add(new NetworkSend(connId, request, null, time));
      }
    }
    sendComplete = new CountDownLatch(mockRequests.size());
    // the requests will be sent in the next poll. Once those are successfully sent,
    // sendComplete will be counted down.
    sendComplete.await();
  }

  /**
   * A wrapper over {@link OperationController} that overrides poll() and onResponse()
   */
  class TestOperationController extends OperationController {
    TestOperationController()
        throws IOException {
      super();
    }

    // Simply returns the requests that were set in a previous sendRequests() call.
    @Override
    protected List<NetworkSend> poll() {
      synchronized (requestListlock) {
        List<NetworkSend> requestsToReturn = requests;
        requests = null;
        return requestsToReturn;
      }
    }

    @Override
    protected void onResponse(List<String> connected, List<String> disconnected, List<NetworkSend> completedSends,
        List<NetworkReceive> completedReceives) {
      super.onResponse(connected, disconnected, completedSends, completedReceives);
      for (String s : connected) {
        connectComplete.countDown();
      }
      for (NetworkSend send : completedSends) {
        sendComplete.countDown();
      }
    }

    public String connect(String host, Port port)
        throws IOException {
      return connectionManager.checkOutConnection(host, port);
    }
  }
}

/**
 * An implementation of Send that simply writes out a buffer that it is
 * initialized with to the given host and port.
 */
class MockRequest implements Send {
  ByteBuffer buffer;
  int size;
  String host;
  Port port;

  public MockRequest(ByteBuffer buffer, String host, Port port) {
    this.buffer = buffer;
    this.host = host;
    this.port = port;
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
