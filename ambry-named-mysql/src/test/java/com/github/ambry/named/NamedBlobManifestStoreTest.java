/*
 * Copyright 2026 LinkedIn Corp. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.github.ambry.named;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.sql.DataSource;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class NamedBlobManifestStoreTest {
  @Test
  public void testReaderRejectsDirtyReadsWithoutChangingPoolIsolation() throws SQLException {
    DataSource db = mock(DataSource.class);
    Connection connection = mock(Connection.class);
    when(db.getConnection()).thenReturn(connection);
    when(connection.getTransactionIsolation()).thenReturn(Connection.TRANSACTION_READ_UNCOMMITTED);
    NamedBlobManifestStore store = new NamedBlobManifestStore(db);
    verify(db, never()).getConnection();
    try {
      store.getPublished("00000000-0000-0000-0000-000000000001");
      fail("A reader could observe a publication that rolls back");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("READ COMMITTED"));
    }
    verify(connection, never()).setTransactionIsolation(anyInt());
    verify(connection, never()).prepareStatement(anyString());
    verify(connection).close();
  }

  @Test
  public void testPublicationUsesADistinctOrderIndependentSet() {
    String first = "00000000-0000-0000-0000-000000000001";
    String second = "00000000-0000-0000-0000-000000000002";
    assertEquals(Arrays.asList(first, second), NamedBlobManifestStore.canonicalIds(Arrays.asList(second, first)));
    for (List<String> invalid : Arrays.asList(Arrays.asList(first, first), Collections.singletonList("0-0-0-0-1"),
        Collections.<String>emptyList())) {
      try {
        NamedBlobManifestStore.canonicalIds(invalid);
        fail("Invalid publication set accepted: " + invalid);
      } catch (IllegalArgumentException expected) {
        // Reject before acquiring database locks.
      }
    }
  }
}
