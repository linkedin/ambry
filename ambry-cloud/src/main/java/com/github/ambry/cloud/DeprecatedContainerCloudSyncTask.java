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

import com.github.ambry.account.AccountService;
import com.github.ambry.account.Container;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helix task to get the deprecated containers information from {@link com.github.ambry.account.AccountService} and update
 * it to cloud.
 */
public class DeprecatedContainerCloudSyncTask implements Task {
  private static final Logger logger = LoggerFactory.getLogger(DeprecatedContainerCloudSyncTask.class);
  private final AccountService accountService;
  private final long containerDeletionRetentionDays;
  private final CloudDestination cloudDestination;
  private final Collection<String> allPartitionIds;

  /**
   * Constructor for {@link DeprecatedContainerCloudSyncTask}.
   * @param accountService {@link AccountService} object.
   * @param containerDeletionRetentionDays retention days for a deprecated container.
   * @param cloudDestination the {@link CloudDestination} object where deprecated container information will be updated.
   * @param allPartitionIds {@link Collection} of all the partitions in the cluster.
   */
  public DeprecatedContainerCloudSyncTask(AccountService accountService, long containerDeletionRetentionDays,
      CloudDestination cloudDestination, Collection<String> allPartitionIds) {
    this.accountService = accountService;
    this.containerDeletionRetentionDays = containerDeletionRetentionDays;
    this.cloudDestination = cloudDestination;
    this.allPartitionIds = Collections.unmodifiableCollection(allPartitionIds);
  }

  @Override
  public TaskResult run() {
    try {
      Set<Container> deprecatedContainers = accountService.getDeprecatedContainers(containerDeletionRetentionDays);
      cloudDestination.deprecateContainers(deprecatedContainers, allPartitionIds);
    } catch (CloudStorageException cloudStorageException) {
      logger.error("Error in updating deprecated containers from account service to cloud: ", cloudStorageException);
    }
    return null;
  }

  @Override
  public void cancel() {

  }
}
