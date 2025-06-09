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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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

  public NamedBlobsCleanupRunner(Router router, NamedBlobDb namedBlobDb, AccountService accountService) {
    this.router = router;
    this.namedBlobDb = namedBlobDb;
    this.accountService = accountService;
  }

  @Override
  public void run() {
    logger.info("Named Blobs Cleanup Runner is initiated");
    try {
      List<StaleNamedBlob> staleResultList;
      Set<Container> activeContainers = accountService.getContainersByStatus(Container.ContainerStatus.ACTIVE);
      Set<Container> inactiveContainers = accountService.getContainersByStatus(Container.ContainerStatus.INACTIVE);
      Set<Container> combinedContainers = new HashSet<>(activeContainers);
      combinedContainers.addAll(inactiveContainers);

      for (Container container : combinedContainers) {
        staleResultList = namedBlobDb.pullStaleBlobs(container).get();
        List<StaleNamedBlob> failedResults = new ArrayList<>();

        int deletedCount = 0;
        for (StaleNamedBlob staleResult : staleResultList) {
          try {
            router.deleteBlob(staleResult.getBlobId(), "ambry-named-blobs-cleanup-runner").get();
          } catch (Exception e) {
            if (!e.getMessage().contains(RouterErrorCode.BlobDoesNotExist.name())) {
              logger.error("Failed to cleanup named stale blob {}", staleResult, e);
              failedResults.add(staleResult);
            }
          }
          deletedCount++;
          if (deletedCount % 1000 == 0) {
            logger.info("Deleted {} blobs, sleeping for a while...", deletedCount);
            Thread.sleep(1000); // sleep for 1 second (adjust as needed)
          }
        }
        staleResultList.removeAll(failedResults);
        namedBlobDb.cleanupStaleData(staleResultList);
        // TODO: do batch delete here?
        logger.info("Named Blobs Cleanup Runner is completed for {} stale cases (there are {} failed cases)",
            staleResultList.size(), failedResults.size());
        Set<String> cleanedBlobIds =
            staleResultList.stream().map(StaleNamedBlob::getBlobId).collect(Collectors.toSet());
        logger.info("The cleaned blobIds are: {}", cleanedBlobIds);
      }
    } catch (Throwable t) {
      logger.error("Exception occurs when running Named Blobs Cleanup Runner", t);
    }
  }
}
