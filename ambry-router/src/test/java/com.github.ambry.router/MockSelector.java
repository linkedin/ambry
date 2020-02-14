/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.router;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.network.BoundedNettyByteBufReceive;
import com.github.ambry.network.NetworkMetrics;
import com.github.ambry.network.NetworkReceive;
import com.github.ambry.network.NetworkSend;
import com.github.ambry.network.PortType;
import com.github.ambry.network.Selector;
import com.github.ambry.utils.Time;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;


/**
 * A class that mocks the {@link Selector} and uses the in-memory {@link MockServer} to send requests and get responses
 * for sends.
 */
class MockSelector extends Selector {
  private final HashMap<String, MockServer> connIdToServer = new HashMap<String, MockServer>();
  private int index;
  private List<String> connected = new ArrayList<String>();
  private List<String> disconnected = new ArrayList<String>();
  private List<NetworkSend> sends = new ArrayList<NetworkSend>();
  private List<NetworkReceive> receives = new ArrayList<NetworkReceive>();
  private final Time time;
  private final AtomicReference<MockSelectorState> state;
  private final MockServerLayout serverLayout;
  private boolean isOpen = true;
  private final NetworkConfig config;

  /**
   *
   * Create a MockSelector
   * @param serverLayout the {@link MockServerLayout} that is used to get the {@link MockServer} given a host and a
   *                     port.
   * @param state the reference to the state that determines the way this object will behave.
   * @param time the Time instance to use.
   * @throws IOException if {@link Selector} throws.
   */
  MockSelector(MockServerLayout serverLayout, AtomicReference<MockSelectorState> state, Time time, NetworkConfig config)
      throws IOException {
    super(new NetworkMetrics(new MetricRegistry()), time, null, config);
    // we don't need the actual selector, close it.
    super.close();
    this.serverLayout = serverLayout;
    this.state = state == null ? new AtomicReference<MockSelectorState>(MockSelectorState.Good) : state;
    this.time = time;
    this.config = config;
  }

  /**
   * Mocks the connect by keeping track of the connection requests to a (host, port) and the mapping from the
   * connection id to the associated {@link MockServer}
   * @param address The address to connect to
   * @param sendBufferSize not used.
   * @param receiveBufferSize not used.
   * @param portType {@link PortType} which represents the type of connection to establish
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
   * Mocks sending and polling. Calls into the {@link MockServer} associated with the connection id with the request
   * and uses the response from the call to construct receives. If the state is not {@link MockSelectorState#Good},
   * then the behavior will be as determined by the state.
   * @param timeoutMs Ignored.
   * @param sends The list of new sends.
   *
   */
  @Override
  public void poll(long timeoutMs, List<NetworkSend> sends) throws IOException {
    this.sends = sends;
    disconnected.clear();
    if (state.get() == MockSelectorState.FailConnectionInitiationOnPoll) {
      disconnected.addAll(connected);
      connected.clear();
    }
    if (sends != null) {
      for (NetworkSend send : sends) {
        if (state.get() == MockSelectorState.ThrowExceptionOnSend) {
          throw new IOException("Mock exception on send");
        }
        if (state.get() == MockSelectorState.ThrowThrowableOnSend) {
          throw new Error("Mock throwable on send");
        }
        if (state.get() == MockSelectorState.DisconnectOnSend) {
          disconnected.add(send.getConnectionId());
        } else {
          MockServer server = connIdToServer.get(send.getConnectionId());
          BoundedNettyByteBufReceive receive = server.send(send.getPayload());
          if (receive != null) {
            receives.add(new NetworkReceive(send.getConnectionId(), receive, time));
          }
        }
      }
    }
    if (state.get() == MockSelectorState.ThrowExceptionOnAllPoll) {
      throw new IOException("Mock exception on poll");
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
    return this.disconnected;
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
   * Returns a list of {@link NetworkReceive} constructed in the last poll.
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

  /**
   * Close the MockSelector
   */
  @Override
  public void close() {
    isOpen = false;
  }

  /**
   * Check whether the MockSelector is open.
   * @return true if the MockSelector is open.
   */
  @Override
  public boolean isOpen() {
    return isOpen;
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
   * A state that causes all poll calls to throw an IOException if there is anything to send.
   */
  ThrowExceptionOnSend,
  /**
   * A state that causes all poll calls to throw an IOException regardless of whether there are sends to perform or
   * not.
   */
  ThrowExceptionOnAllPoll,
  /**
   * Throw a throwable during poll that sends.
   */
  ThrowThrowableOnSend,
  /**
   * A state that causes all connections initiated to fail during poll.
   */
  FailConnectionInitiationOnPoll
}

