/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.mysql;

import com.codahale.metrics.MetricRegistry;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.function.Predicate;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static com.github.ambry.mysql.MySqlUtils.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/** Unit test for MySqlDataAccessor class */
@RunWith(MockitoJUnitRunner.class)
public class MySqlDataAccessorTest {
  private MySqlDataAccessor dataAccessor;
  private final String localDc = "local", remoteDc = "remote";
  private final String username = "ambry", password = "ambry";
  private final String localUrl = "jdbc:mysql://node.localdc/AccountMetadata";
  private final String remoteUrl = "jdbc:mysql://node.remotedc/AccountMetadata";
  private final String writableUrl = "jdbc:mysql://primary/AccountMetadata";
  private final String readonlyUrl = "jdbc:mysql://replica/AccountMetadata";
  private final Driver mockDriver = mock(Driver.class);
  private final MySqlMetrics metrics = new MySqlMetrics(MySqlDataAccessor.class, new MetricRegistry());
  private final SQLException connectionException = new SQLTransientConnectionException("No connection");

  public MySqlDataAccessorTest() {
  }

  @Test
  public void testInputErrors() throws Exception {
    // Pass no endpoints
    expectFailure(() -> new MySqlDataAccessor(Collections.emptyList(), localDc, metrics), "no endpoints",
        e -> e instanceof IllegalArgumentException);
  }

  @Test
  public void testGetConnection() throws Exception {
    // Two writable endpoints, one local one remote
    DbEndpoint localEndpoint = new DbEndpoint(localUrl, localDc, true, username, password);
    DbEndpoint remoteEndpoint = new DbEndpoint(remoteUrl, remoteDc, true, username, password);
    Connection localConnection = mock(Connection.class);
    Connection remoteConnection = mock(Connection.class);
    bringEndpointUp(localEndpoint, localConnection);
    bringEndpointUp(remoteEndpoint, remoteConnection);
    dataAccessor = new MySqlDataAccessor(Arrays.asList(localEndpoint, remoteEndpoint), mockDriver, metrics);
    // If both endpoints are available, expect to connect to local one
    assertEquals(localConnection, dataAccessor.getDatabaseConnection(true));
    // If local endpoint is down, expect to connect to remote one
    bringEndpointDown(localEndpoint, localConnection);
    assertEquals(remoteConnection, dataAccessor.getDatabaseConnection(true));

    // Single read-only endpoint
    DbEndpoint readOnlyEndpoint = new DbEndpoint(readonlyUrl, remoteDc, false, username, password);
    Connection readOnlyConnection = mock(Connection.class);
    bringEndpointUp(readOnlyEndpoint, readOnlyConnection);
    dataAccessor = new MySqlDataAccessor(Collections.singletonList(readOnlyEndpoint), mockDriver, metrics);
    // Ask for readonly connection: expect to connect
    assertEquals(readOnlyConnection, dataAccessor.getDatabaseConnection(false));
    // Ask for writable connection, expect to fail
    expectFailure(() -> dataAccessor.getDatabaseConnection(true), "no writable connection",
        e -> e instanceof SQLException);

    // Two endpoints, one writable one readonly
    DbEndpoint writableEndpoint = new DbEndpoint(writableUrl, remoteDc, true, username, password);
    Connection writableConnection = mock(Connection.class);
    bringEndpointUp(writableEndpoint, writableConnection);
    dataAccessor = new MySqlDataAccessor(Arrays.asList(readOnlyEndpoint, writableEndpoint), mockDriver, metrics);
    // If both endpoints are available, expect to connect to writable one
    assertEquals(writableConnection, dataAccessor.getDatabaseConnection(true));
    // Bring writable endpoint down
    bringEndpointDown(writableEndpoint, writableConnection);
    // Ask for writable connection, expect to fail
    expectFailure(() -> dataAccessor.getDatabaseConnection(true), "no writable connection",
        e -> e.equals(connectionException));
    // Ask for readonly connection: expect to connect
    assertEquals(readOnlyConnection, dataAccessor.getDatabaseConnection(false));
    // Now bring writable endpoint back up.  Request for readonly connection should fail back.
    bringEndpointUp(writableEndpoint, writableConnection);
    assertEquals(writableConnection, dataAccessor.getDatabaseConnection(false));
  }

  @Test
  public void testGetPreparedStatement() throws Exception {
    // TODO
  }

  private void bringEndpointUp(DbEndpoint endpoint, Connection connection) throws SQLException {
    doReturn(connection).when(mockDriver).connect(eq(endpoint.getUrl()), any(Properties.class));
    when(connection.isValid(anyInt())).thenReturn(true);
  }

  private void bringEndpointDown(DbEndpoint endpoint, Connection connection) throws SQLException {
    doThrow(connectionException).when(mockDriver).connect(eq(endpoint.getUrl()), any(Properties.class));
    when(connection.isValid(anyInt())).thenReturn(false);
  }

  /**
   * Call a lambda and expect failure.
   * @param callable the {@link Callable} to run.
   * @param reason the reason for expected failure.
   * @param predicate the predicate for the exception to match.
   */
  private void expectFailure(Callable callable, String reason, Predicate<Exception> predicate) {
    try {
      callable.call();
      fail("Expected failure due to " + reason);
    } catch (Exception e) {
      assertTrue(predicate.test(e));
    }
  }
}
