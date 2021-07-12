/**
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud;

import com.codahale.metrics.Timer;
import com.github.ambry.account.AccountService;
import com.github.ambry.account.AccountUtils;
import com.github.ambry.account.Container;
import java.util.Set;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helix task to get the deprecated containers information from {@link AccountService} and update
 * it to cloud.
 */
public class DeprecatedContainerCloudSyncTask implements Task {
  private static final Logger logger = LoggerFactory.getLogger(DeprecatedContainerCloudSyncTask.class);
  public static String COMMAND = DeprecatedContainerCloudSyncTask.class.getSimpleName();
  private final AccountService accountService;
  private final long containerDeletionRetentionDays;
  private final CloudDestination cloudDestination;
  private final VcrMetrics vcrMetrics;

  /**
   * Constructor for {@link DeprecatedContainerCloudSyncTask}.
   * @param accountService {@link AccountService} object.
   * @param containerDeletionRetentionDays retention days for a deprecated container.
   * @param cloudDestination the {@link CloudDestination} object where deprecated container information will be updated.
   */
  public DeprecatedContainerCloudSyncTask(AccountService accountService, long containerDeletionRetentionDays,
      CloudDestination cloudDestination, VcrMetrics vcrMetrics) {
    this.accountService = accountService;
    this.containerDeletionRetentionDays = containerDeletionRetentionDays;
    this.cloudDestination = cloudDestination;
    this.vcrMetrics = vcrMetrics;
  }

  @Override
  public TaskResult run() {
    TaskResult taskResult = null;
    Timer.Context deprecationTaskRunTimer = vcrMetrics.deprecationTaskRunTime.time();
    try {
      logger.info("DeprecatedContainerCloudSyncTask run started.");
      Timer.Context accountServiceFetchTimer = vcrMetrics.accountServiceFetchTime.time();
      Set<Container> deprecatedContainers =
          AccountUtils.getDeprecatedContainers(accountService, containerDeletionRetentionDays);
      accountServiceFetchTimer.stop();
      logger.info("Attempting deprecation of {} containers.", deprecatedContainers.size());
      cloudDestination.deprecateContainers(deprecatedContainers);
      taskResult =
          new TaskResult(TaskResult.Status.COMPLETED, "DeprecatedContainerCloudSyncTask completed successfully.");
    } catch (CloudStorageException cloudStorageException) {
      logger.error("Error in updating deprecated containers from account service to cloud: ", cloudStorageException);
      taskResult = new TaskResult(TaskResult.Status.FAILED,
          "DeprecatedContainerCloudSyncTask failed due to ." + cloudStorageException.getMessage());
    } finally {
      logger.info("DeprecatedContainerCloudSyncTask done.");
      deprecationTaskRunTimer.stop();
    }
    return taskResult;
  }

  @Override
  public void cancel() {

  }
}
