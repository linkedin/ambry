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


/**
 * Helix task to get the deleted containers information from {@link com.github.ambry.account.AccountService} and update
 * it in CosmosDb DeleteContainers table.
 */
public class ContainerDeletionSyncTask implements Task {
  private final AccountService accountService;
  private final long containerDeletionRetentionDays;
  private final CloudDestination cloudDestination;

  /**
   * Constructor for {@link ContainerDeletionSyncTask}.
   * @param accountService {@link AccountService} object.
   */
  public ContainerDeletionSyncTask(AccountService accountService, long containerDeletionRetentionDays,
      CloudDestination cloudDestination) {
    this.accountService = accountService;
    this.containerDeletionRetentionDays = containerDeletionRetentionDays;
    this.cloudDestination = cloudDestination;
  }

  @Override
  public TaskResult run() {
    Set<Container> deletedContainers = accountService.getDeprecatedContainers(containerDeletionRetentionDays);
    try {
      cloudDestination.updateDeletedContainers(deletedContainers);
    } catch (CloudStorageException csex) {

    }
    return null;
  }

  @Override
  public void cancel() {

  }
}
