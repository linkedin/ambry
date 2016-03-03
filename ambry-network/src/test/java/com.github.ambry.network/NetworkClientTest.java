package com.github.ambry.network;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Time;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;


/**
 * Test the {@link NetworkClient}
 */
public class NetworkClientTest {
  final int CHECKOUT_TIMEOUT_MS = 1000;
  final int MAX_PORTS_PLAIN_TEXT = 3;
  final int MAX_PORTS_SSL = 3;
  Time time;

  MockSelector selector;
  NetworkClient networkClient;
  String host1 = "host1";
  Port port1 = new Port(2222, PortType.PLAINTEXT);
  String host2 = "host2";
  Port port2 = new Port(3333, PortType.SSL);

  /**
   * Test the {@link NetworkClientFactory}
   */
  @Test
  public void testNetworkClientFactory()
      throws IOException {
    Properties props = new Properties();
    VerifiableProperties vprops = new VerifiableProperties(props);
    NetworkConfig networkConfig = new NetworkConfig(vprops);
    NetworkMetrics networkMetrics = new NetworkMetrics(new MetricRegistry());
    NetworkClientFactory networkClientFactory =
        new NetworkClientFactory(networkMetrics, networkConfig, null, MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL,
            CHECKOUT_TIMEOUT_MS, new MockTime());
    networkClientFactory.getNetworkClient();
  }

  private void initialize()
      throws IOException {
    Properties props = new Properties();
    VerifiableProperties vprops = new VerifiableProperties(props);
    selector = new MockSelector();
    ConnectionTracker connectionTracker = new ConnectionTracker(MAX_PORTS_PLAIN_TEXT, MAX_PORTS_SSL);
    NetworkConfig networkConfig = new NetworkConfig(vprops);
    time = new MockTime();
    networkClient = new NetworkClient(selector, connectionTracker, networkConfig, CHECKOUT_TIMEOUT_MS, time);
  }

  /**
   * tests basic request sending, polling and receiving responses correctly associated with the requests.
   */
  @Test
  public void testBasicSendAndPoll()
      throws IOException {
    initialize();
    List<RequestInfo> requestInfoList = new ArrayList<RequestInfo>();
    List<ResponseInfo> responseInfoList;
    int requestCount;
    requestInfoList.add(new RequestInfo(host1, port1, new MockSend(1)));
    requestInfoList.add(new RequestInfo(host1, port1, new MockSend(2)));
    requestCount = requestInfoList.size();

    do {
      responseInfoList = networkClient.sendAndPoll(requestInfoList);
      requestInfoList.clear();
      for (ResponseInfo responseInfo : responseInfoList) {
        MockSend send = (MockSend) responseInfo.getRequest();
        NetworkClientErrorCode error = responseInfo.getError();
        ByteBuffer response = responseInfo.getResponse();
        Assert.assertNull("Should not have encountered an error", error);
        Assert.assertNotNull("Should receive a valid response", response);
        int correlationIdInRequest = send.getCorrelationId();
        int correlationIdInResponse = response.getInt();
        Assert.assertEquals("Received response for the wrong request", correlationIdInRequest, correlationIdInResponse);
        requestCount--;
      }
    } while (requestCount > 0);
    Assert.assertEquals("Should receive only as many responses as there were requests", 0, requestCount);

    responseInfoList = networkClient.sendAndPoll(requestInfoList);
    requestInfoList.clear();
    Assert.assertEquals("No responses are expected at this time", 0, responseInfoList.size());
  }

  /**
   * Tests a failure scenario where requests remain too long in the {@link NetworkClient}'s pending requests queue.
   */
  @Test
  public void testConnectionUnavailable()
      throws IOException, InterruptedException {
    initialize();
    List<RequestInfo> requestInfoList = new ArrayList<RequestInfo>();
    List<ResponseInfo> responseInfoList;
    int requestCount;
    requestInfoList.add(new RequestInfo(host2, port2, new MockSend(3)));
    requestInfoList.add(new RequestInfo(host2, port2, new MockSend(4)));
    requestCount = requestInfoList.size();

    responseInfoList = networkClient.sendAndPoll(requestInfoList);
    requestInfoList.clear();
    Assert.assertEquals("There are no responses expected", 0, responseInfoList.size());
    // the requests were queued. Now increment the time so that they get timed out in the next sendAndPoll.
    time.sleep(CHECKOUT_TIMEOUT_MS + 1);

    do {
      responseInfoList = networkClient.sendAndPoll(requestInfoList);
      requestInfoList.clear();
      for (ResponseInfo responseInfo : responseInfoList) {
        NetworkClientErrorCode error = responseInfo.getError();
        ByteBuffer response = responseInfo.getResponse();
        Assert.assertNotNull("Should have encountered an error", error);
        Assert.assertEquals("Should have received a connection unavailable error",
            NetworkClientErrorCode.ConnectionUnavailable, error);
        Assert.assertNull("Should not have received a valid response", response);
        requestCount--;
      }
    } while (requestCount > 0);
    responseInfoList = networkClient.sendAndPoll(requestInfoList);
    requestInfoList.clear();
    Assert.assertEquals("No responses are expected at this time", 0, responseInfoList.size());
  }

  /**
   * Tests a failure scenario where connections get disconnected after requests are sent out.
   */
  @Test
  public void testNetworkError()
      throws IOException, InterruptedException {
    initialize();
    List<RequestInfo> requestInfoList = new ArrayList<RequestInfo>();
    List<ResponseInfo> responseInfoList;
    int requestCount;
    requestInfoList.add(new RequestInfo(host2, port2, new MockSend(5)));
    requestInfoList.add(new RequestInfo(host2, port2, new MockSend(6)));
    requestCount = requestInfoList.size();

    // set beBad so that requests end up failing due to "network error".
    selector.setBeBad(true);
    do {
      responseInfoList = networkClient.sendAndPoll(requestInfoList);
      requestInfoList.clear();
      for (ResponseInfo responseInfo : responseInfoList) {
        NetworkClientErrorCode error = responseInfo.getError();
        ByteBuffer response = responseInfo.getResponse();
        Assert.assertNotNull("Should have encountered an error", error);
        Assert.assertEquals("Should have received a connection unavailable error", NetworkClientErrorCode.NetworkError,
            error);
        Assert.assertNull("Should not have received a valid response", response);
        requestCount--;
      }
    } while (requestCount > 0);
    responseInfoList = networkClient.sendAndPoll(requestInfoList);
    requestInfoList.clear();
    Assert.assertEquals("No responses are expected at this time", 0, responseInfoList.size());
    selector.setBeBad(false);
  }

  /**
   * Test to ensure subsequent operations after a close throw an IllegalStateException.
   */
  @Test
  public void testClose()
      throws IOException {
    initialize();
    List<RequestInfo> requestInfoList = new ArrayList<RequestInfo>();
    {
      networkClient.close();
      try {
        networkClient.sendAndPoll(requestInfoList);
        requestInfoList.clear();
        Assert.fail("Polling after close should throw");
      } catch (IllegalStateException e) {
      }
    }
  }
}

/**
 * A mock implementation of the {@link Send} interface that simply stores a correlation id that can be used to
 * identify this request.
 */
class MockSend implements Send {
  private final ByteBuffer buf;
  private final int correlationId;
  private final int size;

  /**
   * Construct a MockSend
   * @param correlationId the id associated with this MockSend.
   */
  MockSend(int correlationId) {
    this.correlationId = correlationId;
    buf = ByteBuffer.allocate(16);
    size = 16;
  }

  /**
   * @return the correlation id of this MockSend.
   */
  int getCorrelationId() {
    return correlationId;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long writeTo(WritableByteChannel channel)
      throws IOException {
    long written = channel.write(buf);
    return written;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isSendComplete() {
    return buf.remaining() == 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long sizeInBytes() {
    return size;
  }
}

/**
 * A mock implementation of {@link BoundedByteBufferReceive} that constructs a buffer with the passed in correlation
 * id and returns that buffer as part of {@link #getPayload()}.
 */
class MockBoundedByteBufferReceive extends BoundedByteBufferReceive {
  ByteBuffer buf;

  /**
   * Construct a MockBoundedByteBufferReceive with the given correlation id.
   * @param correlationId the correlation id associated with this object.
   */
  public MockBoundedByteBufferReceive(int correlationId) {
    buf = ByteBuffer.allocate(16);
    buf.putInt(0, correlationId);
    buf.rewind();
  }

  /**
   * Return the buffer associated with this object.
   * @return the buffer associated with this object.
   */
  @Override
  public ByteBuffer getPayload() {
    return buf;
  }
}

/**
 * A class that mocks the {@link Selector} and simply queues connection requests and send requests within itself and
 * returns them in the next calls to {@link #connected()} and {@link #completedSends()} calls.
 */
class MockSelector extends Selector {
  int index;
  private Set<String> connectionIds = new HashSet<String>();
  private List<String> connected = new ArrayList<String>();
  private List<String> disconnected = new ArrayList<String>();
  private List<NetworkSend> sends = new ArrayList<NetworkSend>();
  private List<NetworkReceive> receives = new ArrayList<NetworkReceive>();
  private boolean beBad = false;

  /**
   * Create a MockSelector
   * @throws IOException if {@link Selector} throws.
   */
  MockSelector()
      throws IOException {
    super(new NetworkMetrics(new MetricRegistry()), new MockTime(), null);
  }

  /**
   * Set the beBad state of this selector. If beBad is on, all sends will result in disconnections in the poll.
   * @param beBad the beBad state.
   */
  void setBeBad(boolean beBad) {
    this.beBad = beBad;
  }

  /**
   * Mocks the connect by simply keeping track of the connection requests to a (host, port)
   * @param address The address to connect to
   * @param sendBufferSize not used.
   * @param receiveBufferSize not used.
   * @param portType {@PortType} which represents the type of connection to establish
   * @return the connection id for the connection.
   */
  @Override
  public String connect(InetSocketAddress address, int sendBufferSize, int receiveBufferSize, PortType portType) {
    String hostPortString = address.getHostString() + address.getPort() + index++;
    connected.add(hostPortString);
    connectionIds.add(hostPortString);
    return hostPortString;
  }

  /**
   * Mocks sending and polling. Creates a response for every send to be returned after the next poll,
   * with the correlation id in the Send, unless beBad state is on. If beBad is on,
   * all sends will result in disconnections.
   * @param timeoutMs Ignored.
   * @param sends The list of new sends.
   *
   */
  @Override
  public void poll(long timeoutMs, List<NetworkSend> sends) {
    this.sends = sends;
    if (sends != null) {
      for (NetworkSend send : sends) {
        MockSend mockSend = (MockSend) send.getPayload();
        if (beBad) {
          disconnected.add(send.getConnectionId());
        } else {
          receives.add(
              new NetworkReceive(send.getConnectionId(), new MockBoundedByteBufferReceive(mockSend.getCorrelationId()),
                  new MockTime()));
        }
      }
    }
  }

  /**
   * Returns a list of connection ids created between the last two poll() calls (or since instantiation if only one
   * {@link #poll(long, List)} was done).
   * @return a list of connection ids.
   */
  @Override
  public List<String> connected() {
    List<String> toReturn = connected;
    connected = new ArrayList<String>();
    return toReturn;
  }

  /**
   * Returns a list of connection ids destroyed between the last two poll() calls.
   * @return a list of connection ids.
   */
  @Override
  public List<String> disconnected() {
    List<String> toReturn = disconnected;
    disconnected = new ArrayList<String>();
    return toReturn;
  }

  /**
   * Returns a list of {@link NetworkSend} sent as part of the last poll.
   * @return a lit of {@link NetworkSend} initiated previously.
   */
  @Override
  public List<NetworkSend> completedSends() {
    List<NetworkSend> toReturn = sends;
    sends = new ArrayList<NetworkSend>();
    return toReturn;
  }

  /**
   * Returns a list of {@link NetworkReceive} constructed in the last poll to simulate a response for every send.
   * @return a list of {@line NetworkReceive} for every initiated send.
   */
  @Override
  public List<NetworkReceive> completedReceives() {
    List<NetworkReceive> toReturn = receives;
    receives = new ArrayList<NetworkReceive>();
    return toReturn;
  }

  /**
   * Close the given connection.
   * @param conn connection id to close.
   */
  @Override
  public void close(String conn) {
    if (connectionIds.contains(conn)) {
      disconnected.add(conn);
    }
  }
}
