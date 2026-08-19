/**
 * Copyright 2023 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.frontend;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.AccountService;
import com.github.ambry.account.Container;
import com.github.ambry.named.NamedBlobDb;
import com.github.ambry.named.StaleNamedBlob;
import com.github.ambry.router.Router;
import com.github.ambry.router.RouterErrorCode;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTransientConnectionException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Pull out stale named blob records, delete those blobs, and soft delete the MySQL db rows.
 *
 * <p>This runner is scheduled with {@link java.util.concurrent.ScheduledExecutorService#scheduleAtFixedRate}, which
 * permanently suppresses all future executions if a single execution throws. To keep cleanup self-healing,
 * {@link #run()} never propagates an exception: a page scan that is killed by the database long-transaction guard (a
 * bloated container whose scan crosses the server-side time limit) is retried a bounded number of times, and if it
 * still fails that one container is deferred to the next scheduled run while the remaining containers are still
 * cleaned in the current run. A deferred container resumes from its last processed page cursor on the next run
 * (retained in memory), rather than restarting the scan from the beginning.
 */
public class NamedBlobsCleanupRunner implements Runnable {
  private final Router router;
  private final NamedBlobDb namedBlobDb;
  private static final Logger logger = LoggerFactory.getLogger(NamedBlobsCleanupRunner.class);
  private final AccountService accountService;
  private final String smallestASCII = "\0";
  private final int containerDelaySeconds;
  private final Time time;
  /**
   * Per-container resume cursor keyed by "{accountId}_{containerId}", retained in memory across scheduled runs so a
   * container whose scan was killed and deferred resumes from its last processed page on the next run instead of
   * restarting from the beginning. An entry is removed once its container is fully swept, so the next cycle starts
   * over from the beginning. This is per-pod state; a pod restart resets to a full sweep, which is safe because the
   * scan is idempotent.
   */
  private final Map<String, String> containerCleanupCursors = new ConcurrentHashMap<>();
  /** Increments each time a container is deferred to the next run because its scan could not be completed. */
  private final Counter containerCleanupDeferredCount;
  /** Increments each time a full-size page scan is killed and the runner shrinks the page to make progress. */
  private final Counter pageScanKilledCount;

  /**
   * Maximum number of attempts for a single stale-blob page scan before the container is deferred to the next
   * scheduled run. Bounds the time spent on a persistently slow (bloated) container while still absorbing transient
   * failures such as connection resets or momentary load spikes.
   */
  private static final int MAX_PULL_ATTEMPTS = 3;
  /** Backoff between page-scan attempts. */
  private static final long PULL_RETRY_BACKOFF_MS = 1000L;
  /** Per-page client-side wait for a stale-blob scan. Preserves the previous {@code get(2, TimeUnit.MINUTES)} bound. */
  private static final long PULL_STALE_BLOBS_TIMEOUT_SECONDS = 120L;
  /**
   * Reduced page size used after a full-size page scan is killed, so the scan reads fewer rows, stays under the
   * database time limit, and the cursor keeps moving forward through the container.
   */
  private static final int SHRUNK_PAGE_SIZE = 50;

  public NamedBlobsCleanupRunner(Router router, NamedBlobDb namedBlobDb, AccountService accountService) {
    this(router, namedBlobDb, accountService, 0);
  }

  public NamedBlobsCleanupRunner(Router router, NamedBlobDb namedBlobDb, AccountService accountService,
      int containerDelaySeconds) {
    this(router, namedBlobDb, accountService, containerDelaySeconds, new MetricRegistry());
  }

  public NamedBlobsCleanupRunner(Router router, NamedBlobDb namedBlobDb, AccountService accountService,
      int containerDelaySeconds, MetricRegistry metricRegistry) {
    this(router, namedBlobDb, accountService, containerDelaySeconds, SystemTime.getInstance(), metricRegistry);
  }

  NamedBlobsCleanupRunner(Router router, NamedBlobDb namedBlobDb, AccountService accountService,
      int containerDelaySeconds, Time time) {
    this(router, namedBlobDb, accountService, containerDelaySeconds, time, new MetricRegistry());
  }

  NamedBlobsCleanupRunner(Router router, NamedBlobDb namedBlobDb, AccountService accountService,
      int containerDelaySeconds, Time time, MetricRegistry metricRegistry) {
    if (containerDelaySeconds < 0) {
      throw new IllegalArgumentException("containerDelaySeconds must not be negative");
    }
    this.router = router;
    this.namedBlobDb = namedBlobDb;
    this.accountService = accountService;
    this.containerDelaySeconds = containerDelaySeconds;
    this.time = time;
    this.containerCleanupDeferredCount =
        metricRegistry.counter(MetricRegistry.name(NamedBlobsCleanupRunner.class, "ContainerDeferredCount"));
    this.pageScanKilledCount =
        metricRegistry.counter(MetricRegistry.name(NamedBlobsCleanupRunner.class, "PageScanKilledCount"));
    String cursorMapSizeGauge = MetricRegistry.name(NamedBlobsCleanupRunner.class, "CursorMapSize");
    metricRegistry.remove(cursorMapSizeGauge);
    metricRegistry.register(cursorMapSizeGauge, (Gauge<Integer>) containerCleanupCursors::size);
  }

  @Override
  public void run() {
    logger.info("Named Blobs Cleanup Runner is initiated");
    Set<Container> combinedContainers;
    try {
      Set<Container> activeContainers = accountService.getContainersByStatus(Container.ContainerStatus.ACTIVE);
      Set<Container> inactiveContainers = accountService.getContainersByStatus(Container.ContainerStatus.INACTIVE);
      combinedContainers = new HashSet<>(activeContainers);
      combinedContainers.addAll(inactiveContainers);
    } catch (Exception e) {
      // Never propagate out of run(): scheduleAtFixedRate() would suppress all future runs. Skip this run instead.
      logger.error("Named blob cleanup could not list containers; skipping this run", e);
      return;
    }

    // Drop resume cursors for containers that are no longer eligible (deleted, or now disabled) so the in-memory map
    // cannot grow without bound as containers are created and removed over time.
    Set<String> eligibleCursorKeys = combinedContainers.stream()
        .filter(c -> c.getNamedBlobMode() != Container.NamedBlobMode.DISABLED)
        .map(NamedBlobsCleanupRunner::cursorKey)
        .collect(Collectors.toSet());
    containerCleanupCursors.keySet().retainAll(eligibleCursorKeys);

    boolean processedContainer = false;
    for (Container container : combinedContainers) {
      if (container.getNamedBlobMode() == Container.NamedBlobMode.DISABLED) {
        continue;
      }
      if (processedContainer && containerDelaySeconds > 0) {
        try {
          logger.info("Waiting {} seconds before cleaning container {}", containerDelaySeconds, container.getId());
          time.sleep(TimeUnit.SECONDS.toMillis(containerDelaySeconds));
        } catch (InterruptedException e) {
          logger.info("Named blob cleanup interrupted before container {}; stopping cleanup run", container.getId());
          Thread.currentThread().interrupt();
          return;
        }
      }
      processedContainer = true;
      try {
        cleanupContainer(container);
      } catch (InterruptedException e) {
        logger.info("Named blob cleanup interrupted while cleaning container {}; stopping cleanup run",
            container.getId());
        Thread.currentThread().interrupt();
        return;
      } catch (Exception e) {
        // A container whose scan keeps getting killed (e.g. a bloated prefix crossing the DB long-transaction limit)
        // must not abort the whole run or kill the schedule. Defer it: the next scheduled run retries it, and the
        // remaining containers are still cleaned now.
        containerCleanupDeferredCount.inc();
        logger.error("Deferring cleanup of container {} to the next scheduled run after repeated scan failures",
            container.getId(), e);
      }
    }
    logger.info("Named Blobs Cleanup Runner is completed");
  }

  /**
   * Clean a single container by paging through its stale named blobs, deleting the blobs and soft-deleting the rows.
   * The page cursor ({@code blobName}) advances only after a page has been processed, so a retry resumes from the same
   * cursor rather than restarting the container.
   * @param container the container to clean.
   * @throws InterruptedException if the thread is interrupted (cleanup shutdown); the caller stops the run.
   * @throws Exception if a page scan cannot be completed after {@link #MAX_PULL_ATTEMPTS} attempts; the caller defers
   *                   the container to the next scheduled run.
   */
  private void cleanupContainer(Container container) throws Exception {
    String cursorKey = cursorKey(container);
    // Resume from the cursor saved by a previous run (if this container was deferred after a kill); otherwise start at
    // "\0", the lowest ASCII value, so the scan begins at the start of the container.
    String blobName = containerCleanupCursors.getOrDefault(cursorKey, smallestASCII);
    if (smallestASCII.equals(blobName)) {
      logger.info("Started the cleaner for container: {}", container.getId());
    } else {
      logger.info("Resuming cleanup of container {} from a saved cursor after a previous deferral", container.getId());
    }
    int pageSize = 0;  // 0 means the database default (full-size) page.
    NamedBlobDb.StaleBlobsWithLatestBlobName staleBlobsWithLatestBlobName;
    do {
      try {
        staleBlobsWithLatestBlobName = pullStaleBlobsResilient(container, blobName, pageSize);
      } catch (PageKilledException e) {
        if (pageSize != 0) {
          // Already at the shrunk page size and still killed: cannot make progress on this container right now.
          // Defer it; the next scheduled run resumes from this same cursor.
          throw new IllegalStateException(
              "Stale-blob scan for container " + container.getId() + " was killed even at the shrunk page size", e);
        }
        // Shrink and retry the same cursor so the scan reads fewer rows and the cursor can still move forward,
        // rather than re-running the same heavy query (which only adds load) or sitting on the page across runs.
        pageScanKilledCount.inc();
        pageSize = SHRUNK_PAGE_SIZE;
        logger.warn("Stale-blob scan for container {} was killed at its cursor; shrinking the page to {} to make "
            + "progress instead of retrying the same query", container.getId(), pageSize);
        continue;
      }
      List<StaleNamedBlob> batchStaleBlobs = staleBlobsWithLatestBlobName.getStaleBlobs();
      List<StaleNamedBlob> failedResults = new ArrayList<>();
      for (StaleNamedBlob staleBlob : batchStaleBlobs) {
        try {
          router.deleteBlob(staleBlob.getBlobId(), "ambry-named-blobs-cleanup-runner").get();
        } catch (Exception e) {
          if (e.getMessage() == null || !e.getMessage().contains(RouterErrorCode.BlobDoesNotExist.name())) {
            logger.error("Failed to cleanup named stale blob {}", staleBlob, e);
            failedResults.add(staleBlob);
          }
        }
      }

      batchStaleBlobs.removeAll(failedResults);
      namedBlobDb.cleanupStaleData(batchStaleBlobs);

      if (!batchStaleBlobs.isEmpty()) {
        logger.info("Named Blobs Cleanup Runner processed {} stale blobs ({} failed deletions)",
            batchStaleBlobs.size(), failedResults.size());

        Set<String> cleanedBlobIds =
            batchStaleBlobs.stream().map(StaleNamedBlob::getBlobId).collect(Collectors.toSet());
        logger.info("The cleaned blobIds are: {}", cleanedBlobIds);
      }

      blobName = staleBlobsWithLatestBlobName.getLatestBlob();
      if (blobName != null) {
        // Persist progress so that if a later page is killed and this container is deferred, the next run resumes
        // here instead of restarting the container scan from the beginning.
        containerCleanupCursors.put(cursorKey, blobName);
      }
    } while (blobName != null);
    // Fully swept: drop the saved cursor so the next cleanup cycle re-scans this container from the beginning.
    containerCleanupCursors.remove(cursorKey);
    logger.info("Finished cleaning container {}", container.getId());
  }

  private static String cursorKey(Container container) {
    return container.getParentAccountId() + "_" + container.getId();
  }

  /**
   * Pull one page of stale blobs. A connection-class blip (for example a reset or a pool timeout) is retried a bounded
   * number of times with backoff. A kill or query timeout is not retried: re-running the same expensive query only
   * adds load to an already-struggling database, so it is signalled to the caller (via {@link PageKilledException}) to
   * shrink the page instead.
   * @param container the container being cleaned.
   * @param blobName the page cursor to resume from.
   * @param pageSize the page size to read, or {@code 0} to use the database default (full page).
   * @return the stale blobs page and the next cursor.
   * @throws PageKilledException if the scan was killed or timed out (the caller should shrink and retry).
   * @throws InterruptedException if the thread is interrupted while waiting or backing off.
   * @throws Exception if a retriable failure persists after {@link #MAX_PULL_ATTEMPTS} attempts.
   */
  private NamedBlobDb.StaleBlobsWithLatestBlobName pullStaleBlobsResilient(Container container, String blobName,
      int pageSize) throws Exception {
    Exception lastException = null;
    for (int attempt = 1; attempt <= MAX_PULL_ATTEMPTS; attempt++) {
      try {
        CompletableFuture<NamedBlobDb.StaleBlobsWithLatestBlobName> future =
            pageSize > 0 ? namedBlobDb.pullStaleBlobs(container, blobName, pageSize)
                : namedBlobDb.pullStaleBlobs(container, blobName);
        return future.get(PULL_STALE_BLOBS_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        throw e;
      } catch (Exception e) {
        lastException = e;
        if (!isRetriable(e)) {
          // Killed or timed out. Do not retry the same query; let the caller shrink the page instead.
          throw new PageKilledException(e);
        }
        if (attempt < MAX_PULL_ATTEMPTS) {
          logger.warn("pullStaleBlobs hit a retriable failure for container {} (attempt {}/{}); backing off {} ms",
              container.getId(), attempt, MAX_PULL_ATTEMPTS, PULL_RETRY_BACKOFF_MS, e);
          time.sleep(PULL_RETRY_BACKOFF_MS);
        }
      }
    }
    throw new IllegalStateException(
        "Failed to pull stale blobs for container " + container.getId() + " after " + MAX_PULL_ATTEMPTS + " attempts",
        lastException);
  }

  /**
   * Whether the given throwable is a transient, connection-class failure worth retrying. Only the ISO/SQL
   * "connection exception" class (SQLSTATE 08*) and JDBC's connection-failure subclasses qualify. A kill or a query
   * timeout is deliberately not retriable, so the runner shrinks the page rather than re-running the same heavy query.
   */
  private static boolean isRetriable(Throwable t) {
    Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    Deque<Throwable> stack = new ArrayDeque<>();
    stack.push(t);
    while (!stack.isEmpty()) {
      Throwable cur = stack.pop();
      if (!seen.add(cur)) {
        continue;
      }
      if (cur instanceof SQLNonTransientConnectionException || cur instanceof SQLTransientConnectionException) {
        return true;
      }
      if (cur instanceof SQLException) {
        String state = ((SQLException) cur).getSQLState();
        if (state != null && state.startsWith("08")) {
          return true;
        }
        SQLException next = ((SQLException) cur).getNextException();
        if (next != null) {
          stack.push(next);
        }
      }
      Throwable cause = cur.getCause();
      if (cause != null) {
        stack.push(cause);
      }
    }
    return false;
  }

  /**
   * Signals that a page scan was killed or timed out and should not be retried at the same page size.
   */
  private static class PageKilledException extends Exception {
    PageKilledException(Throwable cause) {
      super(cause);
    }
  }
}
