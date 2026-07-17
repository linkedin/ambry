/*
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
package com.github.ambry.frontend;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.ambry.account.AccountService;
import com.github.ambry.account.Container;
import com.github.ambry.named.NamedBlobDb;
import com.github.ambry.router.Router;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Time;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.Test;


/**
 * Tests for {@link NamedBlobsCleanupRunner}.
 */
public class NamedBlobsCleanupRunnerTest {

  private static final String FIRST_BLOB_NAME = "\0";

  @Test
  public void testZeroDelayProcessesAllEligibleContainers() {
    Container activeContainer = mockContainer((short) 1, Container.NamedBlobMode.OPTIONAL);
    Container inactiveContainer = mockContainer((short) 2, Container.NamedBlobMode.NO_UPDATE);
    Container disabledContainer = mockContainer((short) 3, Container.NamedBlobMode.DISABLED);
    AccountService accountService = mock(AccountService.class);
    when(accountService.getContainersByStatus(Container.ContainerStatus.ACTIVE)).thenReturn(
        new HashSet<>(Arrays.asList(activeContainer, disabledContainer)));
    when(accountService.getContainersByStatus(Container.ContainerStatus.INACTIVE)).thenReturn(
        Collections.singleton(inactiveContainer));
    NamedBlobDb namedBlobDb = mockNamedBlobDb();
    MockTime time = new MockTime();

    new NamedBlobsCleanupRunner(mock(Router.class), namedBlobDb, accountService, 0, time).run();

    verify(namedBlobDb).pullStaleBlobs(activeContainer, FIRST_BLOB_NAME);
    verify(namedBlobDb).pullStaleBlobs(inactiveContainer, FIRST_BLOB_NAME);
    verify(namedBlobDb, never()).pullStaleBlobs(disabledContainer, FIRST_BLOB_NAME);
    assertEquals("A zero delay should not advance time", 0, time.milliseconds());
  }

  @Test
  public void testConfiguredDelayOccursOnlyBetweenEligibleContainers() {
    int containerDelaySeconds = 7;
    Container firstActiveContainer = mockContainer((short) 1, Container.NamedBlobMode.OPTIONAL);
    Container secondActiveContainer = mockContainer((short) 2, Container.NamedBlobMode.NO_UPDATE);
    Container inactiveContainer = mockContainer((short) 3, Container.NamedBlobMode.OPTIONAL);
    Container disabledContainer = mockContainer((short) 4, Container.NamedBlobMode.DISABLED);
    AccountService accountService = mock(AccountService.class);
    when(accountService.getContainersByStatus(Container.ContainerStatus.ACTIVE)).thenReturn(
        new HashSet<>(Arrays.asList(firstActiveContainer, secondActiveContainer, disabledContainer)));
    when(accountService.getContainersByStatus(Container.ContainerStatus.INACTIVE)).thenReturn(
        Collections.singleton(inactiveContainer));
    NamedBlobDb namedBlobDb = mockNamedBlobDb();
    MockTime time = new MockTime();

    new NamedBlobsCleanupRunner(mock(Router.class), namedBlobDb, accountService, containerDelaySeconds, time).run();

    verify(namedBlobDb).pullStaleBlobs(firstActiveContainer, FIRST_BLOB_NAME);
    verify(namedBlobDb).pullStaleBlobs(secondActiveContainer, FIRST_BLOB_NAME);
    verify(namedBlobDb).pullStaleBlobs(inactiveContainer, FIRST_BLOB_NAME);
    verify(namedBlobDb, never()).pullStaleBlobs(disabledContainer, FIRST_BLOB_NAME);
    assertEquals("Three eligible containers should result in two delays",
        TimeUnit.SECONDS.toMillis(2L * containerDelaySeconds), time.milliseconds());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegativeDelayRejected() {
    new NamedBlobsCleanupRunner(mock(Router.class), mock(NamedBlobDb.class), mock(AccountService.class), -1,
        new MockTime());
  }

  @Test
  public void testInterruptedDelayStopsCleanupRun() throws Exception {
    Container firstContainer = mockContainer((short) 1, Container.NamedBlobMode.OPTIONAL);
    Container secondContainer = mockContainer((short) 2, Container.NamedBlobMode.OPTIONAL);
    AccountService accountService = mock(AccountService.class);
    when(accountService.getContainersByStatus(Container.ContainerStatus.ACTIVE)).thenReturn(
        new HashSet<>(Arrays.asList(firstContainer, secondContainer)));
    when(accountService.getContainersByStatus(Container.ContainerStatus.INACTIVE)).thenReturn(Collections.emptySet());
    NamedBlobDb namedBlobDb = mockNamedBlobDb();
    Time time = mock(Time.class);
    doThrow(new InterruptedException("test interruption")).when(time).sleep(TimeUnit.SECONDS.toMillis(1));

    try {
      new NamedBlobsCleanupRunner(mock(Router.class), namedBlobDb, accountService, 1, time).run();

      verify(namedBlobDb, times(1)).pullStaleBlobs(any(Container.class), eq(FIRST_BLOB_NAME));
      assertTrue("The interrupt status should be restored", Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
  }

  private NamedBlobDb mockNamedBlobDb() {
    NamedBlobDb namedBlobDb = mock(NamedBlobDb.class);
    when(namedBlobDb.pullStaleBlobs(any(Container.class), eq(FIRST_BLOB_NAME))).thenReturn(
        CompletableFuture.completedFuture(
            new NamedBlobDb.StaleBlobsWithLatestBlobName(Collections.emptyList(), null)));
    return namedBlobDb;
  }

  private Container mockContainer(short id, Container.NamedBlobMode namedBlobMode) {
    Container container = mock(Container.class);
    when(container.getId()).thenReturn(id);
    when(container.getNamedBlobMode()).thenReturn(namedBlobMode);
    return container;
  }
}
