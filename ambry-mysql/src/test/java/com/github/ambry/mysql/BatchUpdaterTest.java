/*
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

import static com.github.ambry.mysql.MySqlUtils.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Unit for {@link BatchUpdater}.
 */
public class BatchUpdaterTest {

  @Test
  public void testBatchUpdater() throws Exception {
    List<String> committedBatchValues = new ArrayList<>();
    List<String> currentBatchValues = new ArrayList<>();
    AtomicReference<StatementStatus> statusRef = new AtomicReference<>(StatementStatus.PREPARING);
    AtomicReference<String> valueRef = new AtomicReference<>("");
    PreparedStatement statement = mock(PreparedStatement.class);
    doAnswer(invocation -> {
      currentBatchValues.clear();
      return null;
    }).when(statement).clearBatch();
    doAnswer(invocation -> {
      valueRef.set(invocation.getArgument(1));
      return null;
    }).when(statement).setString(anyInt(), anyString());
    doAnswer(invocation -> {
      currentBatchValues.add(valueRef.get());
      statusRef.set(StatementStatus.PREPARING);
      return null;
    }).when(statement).addBatch();
    doAnswer(invocation -> {
      statusRef.set(StatementStatus.EXECUTED);
      return null;
    }).when(statement).executeBatch();

    // Set up MySqlDataAccessor
    AtomicBoolean autoCommit = new AtomicBoolean(true);
    Connection connection = mock(Connection.class);
    when(connection.isValid(anyInt())).thenReturn(true);
    when(connection.prepareStatement(anyString())).thenReturn(statement);
    doAnswer(invocation -> {
      autoCommit.set(invocation.getArgument(0));
      return null;
    }).when(connection).setAutoCommit(anyBoolean());
    doAnswer(invocation -> {
      return autoCommit.get();
    }).when(connection).getAutoCommit();
    doAnswer(invocation -> {
      committedBatchValues.addAll(currentBatchValues);
      statusRef.set(StatementStatus.COMMITTED);
      return null;
    }).when(connection).commit();
    doAnswer(invocation -> {
      statusRef.set(StatementStatus.ROLLBACK);
      return null;
    }).when(connection).rollback();

    DbEndpoint localEndpoint = new DbEndpoint("jdbc:mysql://localhost/testdb", "localDC", true, "user", "password");

    // Test BatchUpdater when there is no exception
    MySqlMetrics metrics = new MySqlMetrics(BatchUpdater.class, new MetricRegistry());
    BatchUpdater batchUpdater = new BatchUpdater(connection, metrics, "insert into myTable values(?)", "myTable", 17);
    assertFalse(connection.getAutoCommit());

    for (int i = 0; i < 100; i++) {
      final int index = i;
      batchUpdater.addUpdateToBatch(st -> {
        assertEquals(statement, st);
        st.setString(1, String.valueOf(index));
      });
      assertEquals(valueRef.get(), String.valueOf(index));
    }
    batchUpdater.flush();
    assertEquals("Number of batches mismatch", 100 / 17 + 1, batchUpdater.getNumBatches());
    for (int i = 0; i < 100; i++) {
      assertEquals(String.valueOf(i), committedBatchValues.get(i));
    }
    assertEquals(StatementStatus.COMMITTED, statusRef.get());
    assertTrue(connection.getAutoCommit());

    // Test BatchUpdater when exception is thrown at setString.
    committedBatchValues.clear();
    valueRef.set("");
    batchUpdater = new BatchUpdater(connection, metrics, "insert into myTable values(?)", "myTable", 17);
    AtomicInteger setStringCount = new AtomicInteger(0);
    final int maxSetStringCount = 20;
    doAnswer(invocation -> {
      if (setStringCount.addAndGet(1) == maxSetStringCount) {
        throw new SQLException("Exception at setstring");
      }
      valueRef.set(invocation.getArgument(1));
      return null;
    }).when(statement).setString(anyInt(), anyString());
    for (int i = 0; i < maxSetStringCount - 1; i++) {
      final int index = i;
      batchUpdater.addUpdateToBatch(st -> {
        assertEquals(statement, st);
        st.setString(1, String.valueOf(index));
      });
      assertEquals(valueRef.get(), String.valueOf(index));
    }
    try {
      batchUpdater.addUpdateToBatch(st -> {
        assertEquals(statement, st);
        st.setString(1, String.valueOf(maxSetStringCount - 1));
      });
      fail("Should fail due to exception");
    } catch (SQLException e) {
      assertEquals("Number of batches mismatch", 1, batchUpdater.getNumBatches());
      assertEquals(StatementStatus.ROLLBACK, statusRef.get());
      assertEquals(String.valueOf(maxSetStringCount - 2), valueRef.get());
      assertEquals(17, committedBatchValues.size());
      assertEquals(2, currentBatchValues.size());
    }

    // Test BatchUpdater when exception is thrown at addBatch
    committedBatchValues.clear();
    valueRef.set("");
    batchUpdater = new BatchUpdater(connection, metrics, "insert into myTable values(?)", "myTable", 17);
    doAnswer(invocation -> {
      valueRef.set(invocation.getArgument(1));
      return null;
    }).when(statement).setString(anyInt(), anyString());
    final int maxAddBatchCount = 20;
    AtomicInteger addBatchCount = new AtomicInteger(0);
    doAnswer(invocation -> {
      if (addBatchCount.addAndGet(1) == maxAddBatchCount) {
        throw new SQLException("Exception at addBatch");
      }
      currentBatchValues.add(valueRef.get());
      statusRef.set(StatementStatus.PREPARING);
      return null;
    }).when(statement).addBatch();
    for (int i = 0; i < maxAddBatchCount - 1; i++) {
      final int index = i;
      batchUpdater.addUpdateToBatch(st -> {
        assertEquals(statement, st);
        st.setString(1, String.valueOf(index));
      });
      assertEquals(valueRef.get(), String.valueOf(index));
    }
    try {
      batchUpdater.addUpdateToBatch(st -> {
        assertEquals(statement, st);
        st.setString(1, String.valueOf(maxAddBatchCount - 1));
      });
      fail("Should fail due to exception");
    } catch (SQLException e) {
      assertEquals("Number of batches mismatch", 1, batchUpdater.getNumBatches());
      assertEquals(StatementStatus.ROLLBACK, statusRef.get());
      assertEquals(String.valueOf(maxSetStringCount - 1), valueRef.get());
      assertEquals(17, committedBatchValues.size());
      assertEquals(2, currentBatchValues.size());
    }

    // Test BatchUpdater when exception is thrown at addBatch
    committedBatchValues.clear();
    valueRef.set("");
    batchUpdater = new BatchUpdater(connection, metrics, "insert into myTable values(?)", "myTable", 17);
    doAnswer(invocation -> {
      currentBatchValues.add(valueRef.get());
      statusRef.set(StatementStatus.PREPARING);
      return null;
    }).when(statement).addBatch();
    doAnswer(invocation -> {
      if (committedBatchValues.size() > 0) {
        // not the first time executeBatch
        throw new SQLException("Exception at executeBatch");
      }
      statusRef.set(StatementStatus.EXECUTED);
      return null;
    }).when(statement).executeBatch();
    for (int i = 0; i < 17 * 2; i++) {
      final int index = i;
      batchUpdater.addUpdateToBatch(st -> {
        assertEquals(statement, st);
        st.setString(1, String.valueOf(index));
      });
      assertEquals(valueRef.get(), String.valueOf(index));
    }
    try {
      batchUpdater.addUpdateToBatch(st -> {
        assertEquals(statement, st);
        st.setString(1, String.valueOf(17 * 2));
      });
      fail("Should fail due to exception");
    } catch (SQLException e) {
      assertEquals("Number of batches mismatch", 1, batchUpdater.getNumBatches());
      assertEquals(StatementStatus.ROLLBACK, statusRef.get());
      assertEquals(String.valueOf(17 * 2 - 1), valueRef.get());
      assertEquals(17, committedBatchValues.size());
      assertEquals(17, currentBatchValues.size());
    }

    // Test BatchUpdater when exception is thrown at commit
    committedBatchValues.clear();
    valueRef.set("");
    batchUpdater = new BatchUpdater(connection, metrics, "insert into myTable values(?)", "myTable", 17);
    doAnswer(invocation -> {
      statusRef.set(StatementStatus.EXECUTED);
      return null;
    }).when(statement).executeBatch();
    doAnswer(invocation -> {
      if (committedBatchValues.size() > 0) {
        // not the first time commit
        throw new SQLException("Exception at commit");
      }
      committedBatchValues.addAll(currentBatchValues);
      statusRef.set(StatementStatus.COMMITTED);
      return null;
    }).when(connection).commit();
    for (int i = 0; i < 17 * 2; i++) {
      final int index = i;
      batchUpdater.addUpdateToBatch(st -> {
        assertEquals(statement, st);
        st.setString(1, String.valueOf(index));
      });
      assertEquals(valueRef.get(), String.valueOf(index));
    }
    try {
      batchUpdater.addUpdateToBatch(st -> {
        assertEquals(statement, st);
        st.setString(1, String.valueOf(17 * 2));
      });
      fail("Should fail due to exception");
    } catch (SQLException e) {
      assertEquals("Number of batches mismatch", 2, batchUpdater.getNumBatches());
      assertEquals(StatementStatus.ROLLBACK, statusRef.get());
      assertEquals(String.valueOf(17 * 2 - 1), valueRef.get());
      assertEquals(17, committedBatchValues.size());
      assertEquals(17, currentBatchValues.size());
    }

    // Test BatchUpdater when exception is thrown at last commit and rollback
    committedBatchValues.clear();
    valueRef.set("");
    batchUpdater = new BatchUpdater(connection, metrics, "insert into myTable values(?)", "myTable", 17);
    doAnswer(invocation -> {
      statusRef.set(StatementStatus.EXECUTED);
      return null;
    }).when(statement).executeBatch();
    doAnswer(invocation -> {
      if (committedBatchValues.size() + currentBatchValues.size() == 100) {
        // not the first time commit
        throw new SQLException("Exception at commit");
      }
      committedBatchValues.addAll(currentBatchValues);
      statusRef.set(StatementStatus.COMMITTED);
      return null;
    }).when(connection).commit();
    doAnswer(invocation -> {
      throw new SQLException("Exception at rollback");
    }).when(connection).rollback();
    for (int i = 0; i < 100; i++) {
      final int index = i;
      batchUpdater.addUpdateToBatch(st -> {
        assertEquals(statement, st);
        st.setString(1, String.valueOf(index));
      });
      assertEquals(valueRef.get(), String.valueOf(index));
    }
    try {
      batchUpdater.flush();
      fail("Should fail due to exception");
    } catch (SQLException e) {
      assertEquals("Number of batches mismatch", 100 / 17 + 1, batchUpdater.getNumBatches());
      assertEquals(StatementStatus.EXECUTED, statusRef.get());
      assertEquals(String.valueOf(99), valueRef.get());
      assertEquals(100 / 17 * 17, committedBatchValues.size());
      assertEquals(100 % 17, currentBatchValues.size());
    }
  }

  enum StatementStatus {
    PREPARING, EXECUTED, COMMITTED, ROLLBACK
  }
}
