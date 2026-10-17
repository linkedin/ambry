/**
 * Copyright 2026 LinkedIn Corp. All rights reserved.
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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.ambry.accountstats;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.mysql.MySqlMetrics;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import javax.sql.DataSource;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;


/**
 * Tests transactional host report publication state transitions.
 */
public class HostAccountReportsStateDaoTest {
  @Test
  public void testPublicationStateTransitions() throws Exception {
    DataSource dataSource = mock(DataSource.class);
    Connection connection = mock(Connection.class);
    PreparedStatement startStatement = mock(PreparedStatement.class);
    PreparedStatement queryStatement = mock(PreparedStatement.class);
    PreparedStatement completeStatement = mock(PreparedStatement.class);
    ResultSet resultSet = mock(ResultSet.class);
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.getAutoCommit()).thenReturn(true);
    when(connection.prepareStatement(startsWith("INSERT INTO HostAccountReportsState"))).thenReturn(startStatement);
    when(connection.prepareStatement(startsWith("SELECT reportTimestampMs"))).thenReturn(queryStatement);
    when(connection.prepareStatement(startsWith("UPDATE HostAccountReportsState"))).thenReturn(completeStatement);
    when(queryStatement.executeQuery()).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getLong("reportTimestampMs")).thenReturn(123L);
    when(resultSet.getLong("reportVersion")).thenReturn(7L);
    when(resultSet.getBoolean("isComplete")).thenReturn(false);
    when(resultSet.getString("reportedPartitions")).thenReturn("1,2");
    when(completeStatement.executeUpdate()).thenReturn(1);
    HostAccountReportsStateDao dao = new HostAccountReportsStateDao(dataSource,
        new MySqlMetrics(HostAccountReportsStateDao.class, new MetricRegistry()));

    HostAccountReportsStateDao.State state = dao.markReportStarted("cluster", "host", 123L, "1,2");
    assertEquals(123L, state.getReportTimestampMs());
    assertEquals(7L, state.getReportVersion());
    assertFalse(state.isComplete());
    assertEquals("1,2", state.getReportedPartitions());
    verify(connection).setAutoCommit(false);
    verify(connection).commit();
    verify(connection).setAutoCommit(true);

    dao.markReportComplete("cluster", "host", state.getReportVersion());
    verify(completeStatement).setLong(3, 7L);
  }

  @Test(expected = java.sql.SQLException.class)
  public void testCompletionRejectsStaleVersion() throws Exception {
    DataSource dataSource = mock(DataSource.class);
    Connection connection = mock(Connection.class);
    PreparedStatement completeStatement = mock(PreparedStatement.class);
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.prepareStatement(startsWith("UPDATE HostAccountReportsState"))).thenReturn(completeStatement);
    when(completeStatement.executeUpdate()).thenReturn(0);
    HostAccountReportsStateDao dao = new HostAccountReportsStateDao(dataSource,
        new MySqlMetrics(HostAccountReportsStateDao.class, new MetricRegistry()));

    dao.markReportComplete("cluster", "host", 7L);
  }

  @Test
  public void testStartFailureRollsBack() throws Exception {
    DataSource dataSource = mock(DataSource.class);
    Connection connection = mock(Connection.class);
    PreparedStatement startStatement = mock(PreparedStatement.class);
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.prepareStatement(startsWith("INSERT INTO HostAccountReportsState"))).thenReturn(startStatement);
    when(startStatement.executeUpdate()).thenThrow(new java.sql.SQLException("write failed"));
    HostAccountReportsStateDao dao = new HostAccountReportsStateDao(dataSource,
        new MySqlMetrics(HostAccountReportsStateDao.class, new MetricRegistry()));

    try {
      dao.markReportStarted("cluster", "host", 123L, "1,2");
      fail("Expected publication start failure");
    } catch (java.sql.SQLException expected) {
      verify(connection).rollback();
    }
  }
}
