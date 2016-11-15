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
package com.github.ambry.network;

import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Time;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


/**
 * Tests the {@link ConnectionTracker}. Checks out and checks in connections and ensures that the pool
 * totalConnectionsCount is honored. Also tests removing connections and closing the connection manager.
 */
public class ConnectionTrackerTest {
  private ConnectionTracker connectionTracker;
  private VerifiableProperties verifiableProperties;
  private RouterConfig routerConfig;
  private NetworkConfig networkConfig;
  private Time time;
  private int connStringIndex = 0;
  private ArrayList<String> connIds = new ArrayList<String>();

  @Before
  public void initialize() throws IOException {
    Properties props = new Properties();
    props.setProperty("router.hostname", "localhost");
    props.setProperty("router.datacenter.name", "DC1");
    props.setProperty("router.scaling.unit.max.connections.per.port.plain.text", "3");
    props.setProperty("router.scaling.unit.max.connections.per.port.ssl", "2");
    verifiableProperties = new VerifiableProperties((props));
    routerConfig = new RouterConfig(verifiableProperties);
    networkConfig = new NetworkConfig(verifiableProperties);
    time = new MockTime();
  }

  /**
   * Test successful instantiation.
   */
  @Test
  public void testConnectionTrackerInstantiation() {
    connectionTracker = new ConnectionTracker(routerConfig.routerScalingUnitMaxConnectionsPerPortPlainText,
        routerConfig.routerScalingUnitMaxConnectionsPerPortSsl);
  }

  /**
   * Tests honoring of pool totalConnectionsCounts.
   * @throws IOException
   */
  @Test
  public void testConnectionTracker() {
    connectionTracker = new ConnectionTracker(routerConfig.routerScalingUnitMaxConnectionsPerPortPlainText,
        routerConfig.routerScalingUnitMaxConnectionsPerPortSsl);
    // When no connections were ever made to a host:port, connectionTracker should return null, but
    // initiate connections.
    int totalConnectionsCount = 0;
    int availableCount = 0;
    Port port1 = new Port(100, PortType.PLAINTEXT);

    boolean done = false;
    do {
      String connId = connectionTracker.checkOutConnection("host1", port1);
      if (connId == null && connectionTracker.mayCreateNewConnection("host1", port1)) {
        connId = mockNewConnection("host1", port1);
        connectionTracker.startTrackingInitiatedConnection("host1", port1, connId);
      } else {
        done = true;
      }
    } while (!done);

    totalConnectionsCount = routerConfig.routerScalingUnitMaxConnectionsPerPortPlainText;
    assertCounts(totalConnectionsCount, availableCount);

    Port port2 = new Port(200, PortType.SSL);
    done = false;
    do {
      String connId = connectionTracker.checkOutConnection("host2", port2);
      if (connId == null && connectionTracker.mayCreateNewConnection("host2", port2)) {
        connId = mockNewConnection("host2", port2);
        connectionTracker.startTrackingInitiatedConnection("host2", port2, connId);
      } else {
        done = true;
      }
    } while (!done);

    totalConnectionsCount += routerConfig.routerScalingUnitMaxConnectionsPerPortSsl;
    assertCounts(totalConnectionsCount, availableCount);

    /* Let us say those connections were made */
    for (String conn : getNewlyEstablishedConnections()) {
      connectionTracker.checkInConnection(conn);
      availableCount++;
    }
    assertCounts(totalConnectionsCount, availableCount);

    String conn = connectionTracker.checkOutConnection("host2", port2);
    Assert.assertNotNull(conn);
    availableCount--;
    assertCounts(totalConnectionsCount, availableCount);

    // Check this connection id back in. This should be returned in a future
    // checkout.
    connectionTracker.checkInConnection(conn);
    availableCount++;
    assertCounts(totalConnectionsCount, availableCount);

    String checkedInConn = conn;
    Set<String> connIdSet = new HashSet<String>();
    for (int i = 0; i < routerConfig.routerScalingUnitMaxConnectionsPerPortSsl; i++) {
      conn = connectionTracker.checkOutConnection("host2", port2);
      Assert.assertNotNull(conn);
      connIdSet.add(conn);
      availableCount--;
    }
    assertCounts(totalConnectionsCount, availableCount);

    // Make sure that one of the checked out connection is the same as the previously checked in connection.
    Assert.assertTrue(connIdSet.contains(checkedInConn));

    // Now that the pool has been exhausted, checkOutConnection should return null.
    Assert.assertNull(connectionTracker.checkOutConnection("host2", port2));
    //And it should not have initiated a new connection.
    assertCounts(totalConnectionsCount, availableCount);

    // test invalid connectionId
    try {
      connectionTracker.checkInConnection("invalid");
      Assert.fail("Invalid connections should not get checked in.");
    } catch (IllegalArgumentException e) {
    }

    try {
      connectionTracker.removeConnection("invalid");
      Assert.fail("Removing invalid connections should not succeed.");
    } catch (IllegalArgumentException e) {
    }

    // test connection removal.
    String conn11 = connectionTracker.checkOutConnection("host1", port1);
    Assert.assertNotNull(conn11);
    availableCount--;
    String conn12 = connectionTracker.checkOutConnection("host1", port1);
    Assert.assertNotNull(conn12);
    availableCount--;
    connectionTracker.checkInConnection(conn11);
    availableCount++;
    // Remove a checked out connection.
    connectionTracker.removeConnection(conn12);
    totalConnectionsCount--;
    // Remove a checked in connection.
    connectionTracker.removeConnection(conn11);
    totalConnectionsCount--;
    availableCount--;

    // Remove the same connection again, which should throw.
    try {
      connectionTracker.removeConnection(conn11);
      Assert.fail("Removing the same connection twice should not succeed.");
    } catch (IllegalArgumentException e) {
    }
    assertCounts(totalConnectionsCount, availableCount);

    Assert.assertNotNull(connectionTracker.checkOutConnection("host1", port1));
    availableCount--;
    for (int i = 0; i < 2; i++) {
      Assert.assertNull("There should not be any available connections to check out",
          connectionTracker.checkOutConnection("host1", port1));
      Assert.assertTrue("It should be okay to initiate a new connection",
          connectionTracker.mayCreateNewConnection("host1", port1));
      connectionTracker.startTrackingInitiatedConnection("host1", port1, mockNewConnection("host1", port1));
      totalConnectionsCount++;
    }
    Assert.assertNull("There should not be any available connections to check out",
        connectionTracker.checkOutConnection("host1", port1));
    Assert.assertFalse("It should not be okay to initiate a new connection",
        connectionTracker.mayCreateNewConnection("host1", port1));
    assertCounts(totalConnectionsCount, availableCount);
  }

  private void assertCounts(int totalConnectionsCount, int availableCount) {
    Assert.assertEquals("total connections should match", totalConnectionsCount,
        connectionTracker.getTotalConnectionsCount());
    Assert.assertEquals("available connections should match", availableCount,
        connectionTracker.getAvailableConnectionsCount());
  }

  /**
   * Mocks a {@link Selector#connect(java.net.InetSocketAddress, int, int, PortType)} call.
   * @param host the host to connect to
   * @param port the port to connect to
   * @return return newly made connection.
   */
  private String mockNewConnection(String host, Port port) {
    // mocks selector connect.
    String connId = host + Integer.toString(port.getPort()) + connStringIndex++;
    connIds.add(connId);
    return connId;
  }

  /**
   * Mocks a {@link com.github.ambry.network.Selector#connected()} call.
   * @return a list of connections that were previously added as part of {@link #mockNewConnection(String, Port)}
   */
  private List<String> getNewlyEstablishedConnections() {
    ArrayList<String> toReturnList = connIds;
    connIds = new ArrayList<String>();
    return toReturnList;
  }
}

