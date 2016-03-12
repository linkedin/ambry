package com.github.ambry.router;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.network.BoundedByteBufferReceive;
import com.github.ambry.network.NetworkMetrics;
import com.github.ambry.network.NetworkReceive;
import com.github.ambry.network.NetworkSend;
import com.github.ambry.network.PortType;
import com.github.ambry.network.Selector;
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.protocol.RequestOrResponseType;
import com.github.ambry.utils.Time;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;


/**
 * A class that mocks the {@link Selector} and buffers all the puts in memory,
 * and sends out responses (as defined by its current state). In order to ensure that puts are done correctly,
 * it will interpret metadata blob and ensure chunks are in order.
 */
class MockSelector extends Selector {
  private HashMap<String, MockServer> connIdToServer = new HashMap<String, MockServer>();
  private int index;
  private List<String> connected = new ArrayList<String>();
  private List<String> disconnected = new ArrayList<String>();
  private List<NetworkSend> sends = new ArrayList<NetworkSend>();
  private List<NetworkReceive> receives = new ArrayList<NetworkReceive>();
  private final Time time;
  private AtomicReference<MockSelectorState> state;
  private MockServerLayout serverLayout;

  private HashMap<String, List<ByteBuffer>> putChunks = new HashMap<String, List<ByteBuffer>>();

  /**
   * Create a MockSelector
   * @throws IOException if {@link Selector} throws.
   */
  MockSelector(Time time, MockServerLayout serverLayout, AtomicReference<MockSelectorState> state)
      throws IOException {
    super(new NetworkMetrics(new MetricRegistry()), time, null);
    this.serverLayout = serverLayout;
    this.time = time;
    this.state = state;
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
  public String connect(InetSocketAddress address, int sendBufferSize, int receiveBufferSize, PortType portType)
      throws IOException {
    if (state.get() == MockSelectorState.ThrowExceptionOnConnect) {
      throw new IOException("Mock connect exception");
    }
    String host = address.getHostString();
    int port = address.getPort();
    String hostPortString = host + port;
    String connId = hostPortString + index++;
    connected.add(connId);
    connIdToServer.put(connId, serverLayout.getMockServer(host, port));
    return connId;
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
  public void poll(long timeoutMs, List<NetworkSend> sends)
      throws IOException {
    if (state.get() == MockSelectorState.ThrowExceptionOnPoll) {
      throw new IOException("Mock exception on poll");
    }
    this.sends = sends;
    if (sends != null) {
      for (NetworkSend send : sends) {
        if (state.get() == MockSelectorState.DisconnectOnSend) {
          disconnected.add(send.getConnectionId());
        } else {
          MockServer server = connIdToServer.get(send.getConnectionId());
          BoundedByteBufferReceive receive = server.send(send.getPayload());
          if(receive!=null) {
            receives.add(new NetworkReceive(send.getConnectionId(), receive, time));
          }
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
   * @return a list of {@link NetworkReceive} for every initiated send.
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
    if (connIdToServer.containsKey(conn)) {
      disconnected.add(conn);
    }
  }
}

/**
 * An enum that reflects the state of the MockSelector.
 */
enum MockSelectorState {
  /**
   * The Good state.
   */
  Good,
  /**
   * A state that causes all connect calls to throw an IOException.
   */
  ThrowExceptionOnConnect,
  /**
   * A state that causes disconnections of connections on which a send is attempted.
   */
  DisconnectOnSend,
  /**
   * A state that causes all poll calls to throw an IOException.
   */
  ThrowExceptionOnPoll,
}

