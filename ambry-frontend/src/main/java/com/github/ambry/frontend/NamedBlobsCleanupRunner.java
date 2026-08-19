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

import com.github.ambry.account.AccountService;
import com.github.ambry.account.Container;
import com.github.ambry.named.NamedBlobDb;
import com.github.ambry.named.StaleNamedBlob;
import com.github.ambry.router.Router;
import com.github.ambry.router.RouterErrorCode;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

  public NamedBlobsCleanupRunner(Router router, NamedBlobDb namedBlobDb, AccountService accountService) {
    this(router, namedBlobDb, accountService, 0);
  }

  public NamedBlobsCleanupRunner(Router router, NamedBlobDb namedBlobDb, AccountService accountService,
      int containerDelaySeconds) {
    this(router, namedBlobDb, accountService, containerDelaySeconds, SystemTime.getInstance());
  }

  NamedBlobsCleanupRunner(Router router, NamedBlobDb namedBlobDb, AccountService accountService,
      int containerDelaySeconds, Time time) {
    if (containerDelaySeconds < 0) {
      throw new IllegalArgumentException("containerDelaySeconds must not be negative");
    }
    this.router = router;
    this.namedBlobDb = namedBlobDb;
    this.accountService = accountService;
    this.containerDelaySeconds = containerDelaySeconds;
    this.time = time;
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
    NamedBlobDb.StaleBlobsWithLatestBlobName staleBlobsWithLatestBlobName;
    do {
      staleBlobsWithLatestBlobName = pullStaleBlobsWithRetry(container, blobName);
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
   * Pull one page of stale blobs, retrying a bounded number of times with backoff. The database long-transaction guard
   * can kill a scan over a container that has accumulated many versions; a brief bounded retry absorbs transient
   * failures (connection resets, load spikes) while the cap ensures a persistently slow page defers the container
   * instead of blocking the entire run.
   *
   * <p>Follow-ups tracked separately: adaptively shrinking the page size on repeated kills, and a covering index so
   * the scan cannot cross the time limit in the first place. (Cross-run resume from the last processed cursor is
   * handled by {@link #cleanupContainer}.)
   * @param container the container being cleaned.
   * @param blobName the page cursor to resume from.
   * @return the stale blobs page and the next cursor.
   * @throws InterruptedException if the thread is interrupted while waiting or backing off.
   * @throws Exception if the scan still fails after {@link #MAX_PULL_ATTEMPTS} attempts.
   */
  private NamedBlobDb.StaleBlobsWithLatestBlobName pullStaleBlobsWithRetry(Container container, String blobName)
      throws Exception {
    Exception lastException = null;
    for (int attempt = 1; attempt <= MAX_PULL_ATTEMPTS; attempt++) {
      try {
        return namedBlobDb.pullStaleBlobs(container, blobName).get(PULL_STALE_BLOBS_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        throw e;
      } catch (Exception e) {
        lastException = e;
        if (attempt < MAX_PULL_ATTEMPTS) {
          logger.warn("pullStaleBlobs failed for container {} (attempt {}/{}); backing off {} ms before retry",
              container.getId(), attempt, MAX_PULL_ATTEMPTS, PULL_RETRY_BACKOFF_MS, e);
          time.sleep(PULL_RETRY_BACKOFF_MS);
        }
      }
    }
    throw new IllegalStateException(
        "Failed to pull stale blobs for container " + container.getId() + " after " + MAX_PULL_ATTEMPTS + " attempts",
        lastException);
  }
}
