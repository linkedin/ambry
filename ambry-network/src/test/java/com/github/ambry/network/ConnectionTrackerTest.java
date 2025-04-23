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

import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Time;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
  private Port plainTextPort;
  private Port sslPort;

  @Before
  public void initialize() {
    Properties props = new Properties();
    props.setProperty("router.hostname", "localhost");
    props.setProperty("router.datacenter.name", "DC1");
    props.setProperty("router.scaling.unit.max.connections.per.port.plain.text", "3");
    props.setProperty("router.scaling.unit.max.connections.per.port.ssl", "2");
    verifiableProperties = new VerifiableProperties((props));
    routerConfig = new RouterConfig(verifiableProperties);
    networkConfig = new NetworkConfig(verifiableProperties);
    time = new MockTime();
    plainTextPort = new Port(100, PortType.PLAINTEXT);
    sslPort = new Port(200, PortType.SSL);
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
   */
  @Test
  public void testConnectionTracker() throws IOException {
    connectionTracker = new ConnectionTracker(routerConfig.routerScalingUnitMaxConnectionsPerPortPlainText,
        routerConfig.routerScalingUnitMaxConnectionsPerPortSsl);
    // When no connections were ever made to a host:port, connectionTracker should return null, but
    // initiate connections.
    int totalConnectionsCount = 0;
    int availableCount = 0;
    List<String> mountPaths = Collections.singletonList("/mnt/path");
    MockDataNodeId dataNodeId1 =
        new MockDataNodeId("host1", Collections.singletonList(plainTextPort), mountPaths, "DC1");
    MockDataNodeId dataNodeId2 = new MockDataNodeId("host2", Arrays.asList(plainTextPort, sslPort), mountPaths, "DC1");
    boolean done = false;
    do {
      String connId = connectionTracker.checkOutConnection("host1", plainTextPort, dataNodeId1);
      if (connId == null && connectionTracker.mayCreateNewConnection("host1", plainTextPort, dataNodeId1)) {
        connId = connectionTracker.connectAndTrack(this::mockNewConnection, "host1", plainTextPort, dataNodeId1);
      } else {
        done = true;
      }
    } while (!done);

    totalConnectionsCount = routerConfig.routerScalingUnitMaxConnectionsPerPortPlainText;
    assertCounts(totalConnectionsCount, availableCount);

    done = false;
    do {
      String connId = connectionTracker.checkOutConnection("host2", sslPort, dataNodeId2);
      if (connId == null && connectionTracker.mayCreateNewConnection("host2", sslPort, dataNodeId2)) {
        connId = connectionTracker.connectAndTrack(this::mockNewConnection, "host2", sslPort, dataNodeId1);
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

    String conn = connectionTracker.checkOutConnection("host2", sslPort, dataNodeId2);
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
      conn = connectionTracker.checkOutConnection("host2", sslPort, dataNodeId2);
      Assert.assertNotNull(conn);
      connIdSet.add(conn);
      availableCount--;
    }
    assertCounts(totalConnectionsCount, availableCount);

    // Make sure that one of the checked out connection is the same as the previously checked in connection.
    Assert.assertTrue(connIdSet.contains(checkedInConn));

    // Now that the pool has been exhausted, checkOutConnection should return null.
    Assert.assertNull(connectionTracker.checkOutConnection("host2", sslPort, dataNodeId2));
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
    String conn11 = connectionTracker.checkOutConnection("host1", plainTextPort, dataNodeId1);
    Assert.assertNotNull(conn11);
    availableCount--;
    String conn12 = connectionTracker.checkOutConnection("host1", plainTextPort, dataNodeId1);
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

    Assert.assertNotNull(connectionTracker.checkOutConnection("host1", plainTextPort, dataNodeId1));
    availableCount--;
    for (int i = 0; i < 2; i++) {
      Assert.assertNull("There should not be any available connections to check out",
          connectionTracker.checkOutConnection("host1", plainTextPort, dataNodeId1));
      Assert.assertTrue("It should be okay to initiate a new connection",
          connectionTracker.mayCreateNewConnection("host1", plainTextPort, dataNodeId1));
      connectionTracker.connectAndTrack(this::mockNewConnection, "host1", plainTextPort, dataNodeId1);
      totalConnectionsCount++;
    }
    Assert.assertNull("There should not be any available connections to check out",
        connectionTracker.checkOutConnection("host1", plainTextPort, dataNodeId1));
    Assert.assertFalse("It should not be okay to initiate a new connection",
        connectionTracker.mayCreateNewConnection("host1", plainTextPort, dataNodeId1));
    assertCounts(totalConnectionsCount, availableCount);
  }

  /**
   * Test behavior of {@link ConnectionTracker#setMinimumActiveConnectionsPercentage} and
   * {@link ConnectionTracker#replenishConnections}.
   * @throws IOException
   */
  @Test
  public void testReplenishConnections() {
    connectionTracker = new ConnectionTracker(routerConfig.routerScalingUnitMaxConnectionsPerPortPlainText,
        routerConfig.routerScalingUnitMaxConnectionsPerPortSsl);
    // When no connections were ever made to a host:port, connectionTracker should return null, but
    // initiate connections.
    int minActiveConnectionsCount = 0;
    int totalConnectionsCount = 0;
    int availableCount = 0;

    MockDataNodeId dataNodeId1 =
        new MockDataNodeId("host1", Collections.singletonList(plainTextPort), Collections.emptyList(), "DC1");
    MockDataNodeId dataNodeId2 =
        new MockDataNodeId("host2", Arrays.asList(plainTextPort, sslPort), Collections.emptyList(), "DC1");
    dataNodeId2.setSslEnabledDataCenters(Collections.singletonList("DC1"));
    connectionTracker.setMinimumActiveConnectionsPercentage(dataNodeId1, 50);
    minActiveConnectionsCount += 50 * routerConfig.routerScalingUnitMaxConnectionsPerPortPlainText / 100;
    connectionTracker.setMinimumActiveConnectionsPercentage(dataNodeId2, 200);
    minActiveConnectionsCount += routerConfig.routerScalingUnitMaxConnectionsPerPortSsl;

    // call replenishConnections to warm up connections
    assertCounts(totalConnectionsCount, availableCount);
    connectionTracker.replenishConnections(this::mockNewConnection, Integer.MAX_VALUE);
    totalConnectionsCount += minActiveConnectionsCount;
    assertCounts(totalConnectionsCount, availableCount);
    List<String> newConnections = getNewlyEstablishedConnections();
    newConnections.forEach(connectionTracker::checkInConnection);
    availableCount += minActiveConnectionsCount;
    assertCounts(totalConnectionsCount, availableCount);
    Assert.assertTrue(connectionTracker.mayCreateNewConnection("host1", plainTextPort, dataNodeId1));
    Assert.assertFalse(connectionTracker.mayCreateNewConnection("host2", sslPort, dataNodeId2));

    // remove 2 connections
    newConnections.stream().limit(2).forEach(connectionTracker::removeConnection);
    totalConnectionsCount -= 2;
    availableCount -= 2;
    assertCounts(totalConnectionsCount, availableCount);

    // replenish connections again
    connectionTracker.replenishConnections(this::mockNewConnection, Integer.MAX_VALUE);
    totalConnectionsCount += 2;
    assertCounts(totalConnectionsCount, availableCount);
    newConnections = getNewlyEstablishedConnections();
    newConnections.forEach(connectionTracker::checkInConnection);
    availableCount += 2;
    assertCounts(totalConnectionsCount, availableCount);

    // check out connections
    String conn1 = connectionTracker.checkOutConnection("host1", plainTextPort, dataNodeId1);
    Assert.assertNotNull(conn1);
    String conn2 = connectionTracker.checkOutConnection("host2", sslPort, dataNodeId2);
    Assert.assertNotNull(conn2);
    availableCount -= 2;
    assertCounts(totalConnectionsCount, availableCount);

    // destroy one and return the other and then replenish
    connectionTracker.removeConnection(conn1);
    connectionTracker.checkInConnection(conn2);
    totalConnectionsCount -= 1;
    availableCount += 1;
    assertCounts(totalConnectionsCount, availableCount);
    connectionTracker.replenishConnections(this::mockNewConnection, Integer.MAX_VALUE);
    totalConnectionsCount += 1;
    assertCounts(totalConnectionsCount, availableCount);

    // check out and destroy all host2 connections
    String connId;
    while ((connId = connectionTracker.checkOutConnection("host2", sslPort, dataNodeId2)) != null) {
      connectionTracker.removeConnection(connId);
    }
    totalConnectionsCount -= 2;
    availableCount -= 2;
    assertCounts(totalConnectionsCount, availableCount);

    // Replenish connection with rate limit of 1 connection per host per call.
    connectionTracker.replenishConnections(this::mockNewConnection, 1);
    totalConnectionsCount += 1;
    assertCounts(totalConnectionsCount, availableCount);
    connectionTracker.replenishConnections(this::mockNewConnection, 1);
    totalConnectionsCount += 1;
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
    String connId = host + port.getPort() + connStringIndex++;
    connIds.add(connId);
    return connId;
  }

  /**
   * Mocks a {@link com.github.ambry.network.Selector#connected()} call.
   * @return a list of connections that were previously added as part of {@link #mockNewConnection(String, Port)}
   */
  private List<String> getNewlyEstablishedConnections() {
    ArrayList<String> toReturnList = connIds;
    connIds = new ArrayList<>();
    return toReturnList;
  }
}

