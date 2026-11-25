/*
 * Copyright 2026 LinkedIn Corp. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.github.ambry.named;

import com.zaxxer.hikari.HikariConfig;
import org.junit.Test;

import static org.junit.Assert.*;


public class MySqlNamedBlobDbFactoryTest {
  @Test
  public void testFlagOffPreservesConfiguredIsolation() {
    HikariConfig pool = new HikariConfig();
    MySqlNamedBlobDbFactory.configureIsolation(pool, TransactionIsolationLevel.TRANSACTION_NONE, false);
    assertNull(pool.getTransactionIsolation());
    assertNull(pool.getConnectionInitSql());
    MySqlNamedBlobDbFactory.configureIsolation(pool, TransactionIsolationLevel.TRANSACTION_READ_COMMITTED, false);
    assertEquals("TRANSACTION_READ_COMMITTED", pool.getTransactionIsolation());
    assertNull(pool.getConnectionInitSql());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFenceAwareWriterRejectsExplicitReadCommitted() {
    MySqlNamedBlobDbFactory.configureIsolation(new HikariConfig(), TransactionIsolationLevel.TRANSACTION_READ_COMMITTED,
        true);
  }
}
