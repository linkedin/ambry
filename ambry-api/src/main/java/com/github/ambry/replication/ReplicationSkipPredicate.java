/*
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

package com.github.ambry.replication;

import com.github.ambry.account.Account;
import com.github.ambry.account.AccountService;
import com.github.ambry.account.Container;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.store.MessageInfo;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ReplicationSkipPredicate implements Predicate<MessageInfo> {
  private final AccountService accountService;
  private final ReplicationConfig replicationConfig;
  private static final Logger logger = LoggerFactory.getLogger(ReplicationSkipPredicate.class);

  /**
   * Construct a ReplicationSkipPredicate object
   * @param accountService the {@link AccountService} associated with this predicate.
   */
  public ReplicationSkipPredicate(AccountService accountService, ReplicationConfig replicationConfig) {
    this.accountService = accountService;
    this.replicationConfig = replicationConfig;
  }

  /**
   * Determines if {@link MessageInfo} container in the status of DELETED_IN_PROGRESS or INACTIVE.
   * DELETED_IN_PROGRESS containers won't be skipper from replication within the container deletion retention time.
   * @param messageInfo A message info class that contains basic info about a blob
   * @return {@code true} if the blob associates with the deprecated container, {@code false} otherwise.
   * Deprecated containers status include DELETE_IN_PROGRESS and INACTIVE.
   */
  @Override
  public boolean test(MessageInfo messageInfo) {
    if (accountService != null) {
      Account account = accountService.getAccountById(messageInfo.getAccountId());
      if (account == null) {
        logger.trace("Can't get account through accountService : {}", accountService);
        return false;
      }
      Container container = account.getContainerById(messageInfo.getContainerId());
      if (container == null) {
        logger.trace("Can't get container through account : {}", account);
        return false;
      }
      Container.ContainerStatus status = container.getStatus();
      if (status == Container.ContainerStatus.DELETE_IN_PROGRESS &&
          container.getDeleteTriggerTime() + TimeUnit.DAYS.toMillis(
              replicationConfig.replicationContainerDeletionRetentionDays) > System.currentTimeMillis()) {
        logger.debug("Container {} is not qualified as it’s still within retention time", container);
        return false;
      }
      if (status == Container.ContainerStatus.DELETE_IN_PROGRESS || status == Container.ContainerStatus.INACTIVE) {
        logger.trace("Container {} will be skipped during replication", container);
        return true;
      } else {
        logger.debug("Container {} is Active", container);
        return false;
      }
    } else {
      logger.debug("Current accountService : {}", accountService);
      return false;
    }
  }
}
