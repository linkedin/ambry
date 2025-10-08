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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Pull out stale named blob records, delete those blobs, and soft delete the MySQL db rows
 */
public class NamedBlobsCleanupRunner implements Runnable {
  private final Router router;
  private final NamedBlobDb namedBlobDb;
  private static final Logger logger = LoggerFactory.getLogger(NamedBlobsCleanupRunner.class);
  private final AccountService accountService;
  private final String smallestASCII = "\0";

  public NamedBlobsCleanupRunner(Router router, NamedBlobDb namedBlobDb, AccountService accountService) {
    this.router = router;
    this.namedBlobDb = namedBlobDb;
    this.accountService = accountService;
  }

  @Override
  public void run() {
    logger.info("Named Blobs Cleanup Runner is initiated");
    try {
      Set<Container> activeContainers = accountService.getContainersByStatus(Container.ContainerStatus.ACTIVE);
      Set<Container> inactiveContainers = accountService.getContainersByStatus(Container.ContainerStatus.INACTIVE);
      Set<Container> combinedContainers = new HashSet<>(activeContainers);
      combinedContainers.addAll(inactiveContainers);
      List<StaleNamedBlob> batchStaleBlobs = Collections.emptyList();
      NamedBlobDb.StaleBlobsWithLatestBlobName staleBlobsWithLatestBlobName;
      for (Container container : combinedContainers) {
        if (container.getNamedBlobMode() == Container.NamedBlobMode.DISABLED) {
          continue;
        }
        logger.info("Started the cleaner for container: {}", container.getId());
        // set blobName to be "\0" since it is the lowest ASCII value and everything is greater than it
        String blobName = smallestASCII;
        do {
          staleBlobsWithLatestBlobName = namedBlobDb.pullStaleBlobs(container, blobName).get(2, TimeUnit.MINUTES);
          batchStaleBlobs = staleBlobsWithLatestBlobName.getStaleBlobs();
          List<StaleNamedBlob> failedResults = new ArrayList<>();
          for (StaleNamedBlob staleBlob : staleBlobsWithLatestBlobName.getStaleBlobs()) {
            try {
              router.deleteBlob(staleBlob.getBlobId(), "ambry-named-blobs-cleanup-runner").get();
            } catch (Exception e) {
              if (!e.getMessage().contains(RouterErrorCode.BlobDoesNotExist.name())) {
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
        } while (staleBlobsWithLatestBlobName.getLatestBlob() != null);
        try {
          logger.info("Finished cleaning container {}. Sleeping for 7 days before next container...", container.getId());
          Thread.sleep(604800000); // 5 seconds
        } catch (InterruptedException e) {
          logger.error("Sleep interrupted after container cleanup", e);
        }
      }
      logger.info("Named Blobs Cleanup Runner is completed");
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (Exception e) {
      logger.error("Unexpected error occurred while cleaning up named blobs", e);
    }
  }
}
