/*
 * Copyright 2026 LinkedIn Corp. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.github.ambry.named;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
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
      store.getPublished((short) 17, (short) 4, "folder/MixedCase", 1);
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
    assertEquals(Arrays.asList(1L, 2L), NamedBlobManifestStore.canonicalVersions(Arrays.asList(2L, 1L)));
    for (List<Long> invalid : Arrays.asList(Arrays.asList(1L, 1L), Collections.<Long>singletonList(null),
        Collections.<Long>emptyList())) {
      try {
        NamedBlobManifestStore.canonicalVersions(invalid);
        fail("Invalid publication set accepted: " + invalid);
      } catch (IllegalArgumentException expected) {
        // Reject before acquiring database locks.
      }
    }
  }

  @Test
  public void testPublicationRejectsMoreThanTheDeclaredVersionCap() {
    List<Long> tooMany = new ArrayList<>();
    for (long version = 1; version <= 33; version++) {
      tooMany.add(version);
    }
    try {
      NamedBlobManifestStore.checkManifestCount(tooMany);
      fail("More than the declared cap of distinct named versions was accepted in one publication");
    } catch (IllegalArgumentException expected) {
      assertEquals("Invalid number of manifests", expected.getMessage());
    }
  }
}
