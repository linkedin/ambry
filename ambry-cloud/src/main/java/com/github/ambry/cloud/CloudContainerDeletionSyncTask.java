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
import java.util.Set;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helix task to get the deleted containers information from {@link com.github.ambry.account.AccountService} and update
 * it in CosmosDb DeleteContainers table.
 */
public class CloudContainerDeletionSyncTask implements Task {
  private static final Logger logger = LoggerFactory.getLogger(CloudContainerDeletionSyncTask.class);
  private final AccountService accountService;
  private final long containerDeletionRetentionDays;
  private final CloudDestination cloudDestination;

  /**
   * Constructor for {@link CloudContainerDeletionSyncTask}.
   * @param accountService {@link AccountService} object.
   */
  public CloudContainerDeletionSyncTask(AccountService accountService, long containerDeletionRetentionDays,
      CloudDestination cloudDestination) {
    this.accountService = accountService;
    this.containerDeletionRetentionDays = containerDeletionRetentionDays;
    this.cloudDestination = cloudDestination;
  }

  @Override
  public TaskResult run() {
    try {
      Set<Container> deletedContainers = accountService.getDeprecatedContainers(containerDeletionRetentionDays);
      cloudDestination.updateDeletedContainers(deletedContainers);
    } catch (CloudStorageException cloudStorageException) {
      logger.error("Error in updating deleted containers from account service to cloud: ", cloudStorageException);
    }
    return null;
  }

  @Override
  public void cancel() {

  }
}
