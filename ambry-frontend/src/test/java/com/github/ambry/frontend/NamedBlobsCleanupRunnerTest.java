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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.ambry.account.Account;
import com.github.ambry.account.AccountService;
import com.github.ambry.account.Container;
import com.github.ambry.named.NamedBlobDb;
import com.github.ambry.router.Router;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.sql.SQLException;
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

  @Test
  public void testKilledScanDefersContainerWithoutAbortingRunOrScheduler() {
    // A killed scan (DB long-transaction guard) surfaces as an ExecutionException from future.get(). The runner must
    // retry the page a bounded number of times, defer the offending container, and still clean the other containers,
    // all without letting run() throw (which would permanently suppress the scheduleAtFixedRate schedule).
    Container goodContainer = mockContainer((short) 1, Container.NamedBlobMode.OPTIONAL);
    Container killedContainer = mockContainer((short) 2, Container.NamedBlobMode.OPTIONAL);
    AccountService accountService = mock(AccountService.class);
    when(accountService.getContainersByStatus(Container.ContainerStatus.ACTIVE)).thenReturn(
        new HashSet<>(Arrays.asList(goodContainer, killedContainer)));
    when(accountService.getContainersByStatus(Container.ContainerStatus.INACTIVE)).thenReturn(Collections.emptySet());

    NamedBlobDb namedBlobDb = mock(NamedBlobDb.class);
    when(namedBlobDb.pullStaleBlobs(eq(goodContainer), eq(FIRST_BLOB_NAME))).thenReturn(
        CompletableFuture.completedFuture(new NamedBlobDb.StaleBlobsWithLatestBlobName(Collections.emptyList(), null)));
    CompletableFuture<NamedBlobDb.StaleBlobsWithLatestBlobName> killedScan = new CompletableFuture<>();
    killedScan.completeExceptionally(new SQLException("Query execution was interrupted", "70100", 1317));
    when(namedBlobDb.pullStaleBlobs(eq(killedContainer), eq(FIRST_BLOB_NAME))).thenReturn(killedScan);
    when(namedBlobDb.pullStaleBlobs(eq(killedContainer), eq(FIRST_BLOB_NAME), anyInt())).thenReturn(killedScan);

    // run() must complete normally despite the killed scan.
    new NamedBlobsCleanupRunner(mock(Router.class), namedBlobDb, accountService, 0, new MockTime()).run();

    // The killed container is retried up to the bounded cap, then deferred; the healthy container is still cleaned.
    // A kill is not retried at the same page size; the full page is attempted once, then the page is shrunk.
    verify(namedBlobDb, times(1)).pullStaleBlobs(killedContainer, FIRST_BLOB_NAME);
    verify(namedBlobDb, times(1)).pullStaleBlobs(eq(killedContainer), eq(FIRST_BLOB_NAME), anyInt());
    verify(namedBlobDb, times(1)).pullStaleBlobs(goodContainer, FIRST_BLOB_NAME);
  }

  @Test
  public void testKilledShrunkScanSkipsBloatedBlobNameAndContinues() {
    // When even the shrunk page is killed, the runner looks up the offending blob name and skips past it, resuming
    // after it so the rest of the container is still cleaned instead of making no progress at all.
    Container container = mockContainer((short) 1, Container.NamedBlobMode.OPTIONAL, (short) 100, "container-a");
    AccountService accountService = mock(AccountService.class);
    when(accountService.getContainersByStatus(Container.ContainerStatus.ACTIVE)).thenReturn(
        Collections.singleton(container));
    when(accountService.getContainersByStatus(Container.ContainerStatus.INACTIVE)).thenReturn(Collections.emptySet());

    NamedBlobDb namedBlobDb = mock(NamedBlobDb.class);
    CompletableFuture<NamedBlobDb.StaleBlobsWithLatestBlobName> killedScan = new CompletableFuture<>();
    killedScan.completeExceptionally(new SQLException("Query execution was interrupted", "70100", 1317));
    // The blob name at the start of the container is fatally bloated: both the full and shrunk scans are killed.
    when(namedBlobDb.pullStaleBlobs(eq(container), eq(FIRST_BLOB_NAME))).thenReturn(killedScan);
    when(namedBlobDb.pullStaleBlobs(eq(container), eq(FIRST_BLOB_NAME), anyInt())).thenReturn(killedScan);
    when(namedBlobDb.getFirstBlobName(eq(container), eq(FIRST_BLOB_NAME))).thenReturn(
        CompletableFuture.completedFuture("bloated"));
    // Everything after the skipped blob name scans cleanly and the container finishes.
    when(namedBlobDb.pullStaleBlobs(eq(container), eq("bloated\u0000"))).thenReturn(CompletableFuture.completedFuture(
        new NamedBlobDb.StaleBlobsWithLatestBlobName(Collections.emptyList(), null)));

    new NamedBlobsCleanupRunner(mock(Router.class), namedBlobDb, accountService, 0, new MockTime()).run();

    verify(namedBlobDb).getFirstBlobName(container, FIRST_BLOB_NAME);
    verify(namedBlobDb).pullStaleBlobs(container, "bloated\u0000");
  }

  @Test
  public void testResumesFromSavedCursorAfterKill() {
    // On the first run the container's first page succeeds (cursor advances to "cursor1"), then the page at "cursor1"
    // is killed and the container is deferred. The second run must resume from "cursor1", not restart at "\0".
    Container container = mockContainer((short) 1, Container.NamedBlobMode.OPTIONAL);
    AccountService accountService = mock(AccountService.class);
    when(accountService.getContainersByStatus(Container.ContainerStatus.ACTIVE)).thenReturn(
        Collections.singleton(container));
    when(accountService.getContainersByStatus(Container.ContainerStatus.INACTIVE)).thenReturn(Collections.emptySet());

    NamedBlobDb namedBlobDb = mock(NamedBlobDb.class);
    when(namedBlobDb.pullStaleBlobs(eq(container), eq(FIRST_BLOB_NAME))).thenReturn(CompletableFuture.completedFuture(
        new NamedBlobDb.StaleBlobsWithLatestBlobName(Collections.emptyList(), "cursor1")));
    CompletableFuture<NamedBlobDb.StaleBlobsWithLatestBlobName> killedScan = new CompletableFuture<>();
    killedScan.completeExceptionally(new SQLException("Query execution was interrupted", "70100", 1317));
    when(namedBlobDb.pullStaleBlobs(eq(container), eq("cursor1"))).thenReturn(killedScan);
    when(namedBlobDb.pullStaleBlobs(eq(container), eq("cursor1"), anyInt())).thenReturn(killedScan);

    NamedBlobsCleanupRunner runner =
        new NamedBlobsCleanupRunner(mock(Router.class), namedBlobDb, accountService, 0, new MockTime());
    runner.run();
    runner.run();

    // "\0" is scanned only on the first run; the second run resumes from the saved "cursor1".
    verify(namedBlobDb, times(1)).pullStaleBlobs(container, FIRST_BLOB_NAME);
    // "cursor1" is attempted MAX_PULL_ATTEMPTS (3) times per run, across both runs.
    verify(namedBlobDb, times(2)).pullStaleBlobs(container, "cursor1");
  }

  @Test
  public void testCursorResetsAfterContainerFullySwept() {
    // A container that is fully swept (no kill) must drop its saved cursor, so the next run restarts from "\0".
    Container container = mockContainer((short) 2, Container.NamedBlobMode.OPTIONAL);
    AccountService accountService = mock(AccountService.class);
    when(accountService.getContainersByStatus(Container.ContainerStatus.ACTIVE)).thenReturn(
        Collections.singleton(container));
    when(accountService.getContainersByStatus(Container.ContainerStatus.INACTIVE)).thenReturn(Collections.emptySet());

    NamedBlobDb namedBlobDb = mock(NamedBlobDb.class);
    when(namedBlobDb.pullStaleBlobs(eq(container), eq(FIRST_BLOB_NAME))).thenReturn(CompletableFuture.completedFuture(
        new NamedBlobDb.StaleBlobsWithLatestBlobName(Collections.emptyList(), "c2")));
    when(namedBlobDb.pullStaleBlobs(eq(container), eq("c2"))).thenReturn(CompletableFuture.completedFuture(
        new NamedBlobDb.StaleBlobsWithLatestBlobName(Collections.emptyList(), null)));

    NamedBlobsCleanupRunner runner =
        new NamedBlobsCleanupRunner(mock(Router.class), namedBlobDb, accountService, 0, new MockTime());
    runner.run();
    runner.run();

    // Each run does a full sweep from "\0", because the cursor is cleared once the container completes.
    verify(namedBlobDb, times(2)).pullStaleBlobs(container, FIRST_BLOB_NAME);
    verify(namedBlobDb, times(2)).pullStaleBlobs(container, "c2");
  }

  @Test
  public void testConnectionFailureIsRetriedNotShrunk() {
    // A connection-class failure (SQLSTATE 08*) is transient and should be retried on the same full-size page, not
    // treated as a kill (which would shrink the page).
    Container container = mockContainer((short) 5, Container.NamedBlobMode.OPTIONAL);
    AccountService accountService = mock(AccountService.class);
    when(accountService.getContainersByStatus(Container.ContainerStatus.ACTIVE)).thenReturn(
        Collections.singleton(container));
    when(accountService.getContainersByStatus(Container.ContainerStatus.INACTIVE)).thenReturn(Collections.emptySet());

    NamedBlobDb namedBlobDb = mock(NamedBlobDb.class);
    CompletableFuture<NamedBlobDb.StaleBlobsWithLatestBlobName> connectionFailure = new CompletableFuture<>();
    connectionFailure.completeExceptionally(new SQLException("Communications link failure", "08S01"));
    CompletableFuture<NamedBlobDb.StaleBlobsWithLatestBlobName> ok =
        CompletableFuture.completedFuture(new NamedBlobDb.StaleBlobsWithLatestBlobName(Collections.emptyList(), null));
    // First call fails with a connection error (retriable), the retry succeeds.
    when(namedBlobDb.pullStaleBlobs(eq(container), eq(FIRST_BLOB_NAME))).thenReturn(connectionFailure, ok);

    new NamedBlobsCleanupRunner(mock(Router.class), namedBlobDb, accountService, 0, new MockTime()).run();

    // The full-size page was retried (2 calls) and the page was never shrunk.
    verify(namedBlobDb, times(2)).pullStaleBlobs(container, FIRST_BLOB_NAME);
    verify(namedBlobDb, never()).pullStaleBlobs(eq(container), eq(FIRST_BLOB_NAME), anyInt());
  }

  @Test
  public void testExcludedContainersAreSkipped() {
    Container excludedContainer = mockContainer((short) 1, Container.NamedBlobMode.OPTIONAL, (short) 100, "container-a");
    Container includedContainer = mockContainer((short) 2, Container.NamedBlobMode.OPTIONAL, (short) 100, "container-b");
    Account account = mock(Account.class);
    when(account.getName()).thenReturn("account1");
    AccountService accountService = mock(AccountService.class);
    when(accountService.getAccountById((short) 100)).thenReturn(account);
    when(accountService.getContainersByStatus(Container.ContainerStatus.ACTIVE)).thenReturn(
        new HashSet<>(Arrays.asList(excludedContainer, includedContainer)));
    when(accountService.getContainersByStatus(Container.ContainerStatus.INACTIVE)).thenReturn(Collections.emptySet());
    NamedBlobDb namedBlobDb = mockNamedBlobDb();

    new NamedBlobsCleanupRunner(mock(Router.class), namedBlobDb, accountService, 0,
        Collections.singleton("account1/container-a"), new MockTime()).run();

    verify(namedBlobDb, never()).pullStaleBlobs(excludedContainer, FIRST_BLOB_NAME);
    verify(namedBlobDb).pullStaleBlobs(includedContainer, FIRST_BLOB_NAME);
  }

  @Test
  public void testContainerProcessedWhenAccountUnresolvedDespiteExclusionList() {
    Container container = mockContainer((short) 1, Container.NamedBlobMode.OPTIONAL, (short) 200, "container-a");
    AccountService accountService = mock(AccountService.class);
    when(accountService.getAccountById((short) 200)).thenReturn(null);
    when(accountService.getContainersByStatus(Container.ContainerStatus.ACTIVE)).thenReturn(
        Collections.singleton(container));
    when(accountService.getContainersByStatus(Container.ContainerStatus.INACTIVE)).thenReturn(Collections.emptySet());
    NamedBlobDb namedBlobDb = mockNamedBlobDb();

    new NamedBlobsCleanupRunner(mock(Router.class), namedBlobDb, accountService, 0,
        Collections.singleton("account1/container-a"), new MockTime()).run();

    verify(namedBlobDb).pullStaleBlobs(container, FIRST_BLOB_NAME);
  }

  @Test
  public void testExcludedContainerWithWhitespaceInNameIsSkipped() {
    // Ambry historically allows whitespace (and other special characters) within account/container names. The
    // exclusion list matches the full "accountName/containerName" verbatim; config parsing trims only the outer
    // padding around each comma-separated entry, so whitespace inside a name is preserved and still matches.
    Container excludedContainer = mockContainer((short) 1, Container.NamedBlobMode.OPTIONAL, (short) 100, "my container");
    Account account = mock(Account.class);
    when(account.getName()).thenReturn("my account");
    AccountService accountService = mock(AccountService.class);
    when(accountService.getAccountById((short) 100)).thenReturn(account);
    when(accountService.getContainersByStatus(Container.ContainerStatus.ACTIVE)).thenReturn(
        Collections.singleton(excludedContainer));
    when(accountService.getContainersByStatus(Container.ContainerStatus.INACTIVE)).thenReturn(Collections.emptySet());
    NamedBlobDb namedBlobDb = mockNamedBlobDb();

    new NamedBlobsCleanupRunner(mock(Router.class), namedBlobDb, accountService, 0,
        Utils.splitString("my account/my container", ","), new MockTime()).run();

    verify(namedBlobDb, never()).pullStaleBlobs(excludedContainer, FIRST_BLOB_NAME);
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

  private Container mockContainer(short id, Container.NamedBlobMode namedBlobMode, short parentAccountId, String name) {
    Container container = mockContainer(id, namedBlobMode);
    when(container.getParentAccountId()).thenReturn(parentAccountId);
    when(container.getName()).thenReturn(name);
    return container;
  }
}
