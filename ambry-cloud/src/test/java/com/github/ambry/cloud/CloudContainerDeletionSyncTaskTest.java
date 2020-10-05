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
package com.github.ambry.cloud;

import com.github.ambry.account.AccountService;
import com.github.ambry.account.Container;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;


/**
 * Test {@link CloudContainerDeletionSyncTask}.
 */
@RunWith(MockitoJUnitRunner.class)
public class CloudContainerDeletionSyncTaskTest {

  private static final Set<String> ALL_PARTITIONS = new HashSet<>(Arrays.asList("1", "2", "3"));
  private final CloudContainerDeletionSyncTask cloudContainerDeletionSyncTask;
  private final AccountService accountService;
  private final LatchBasedInMemoryCloudDestination cloudDestination;

  /**
   * Constructor for {@link CloudContainerDeletionSyncTaskTest}.
   */
  public CloudContainerDeletionSyncTaskTest() {
    accountService = Mockito.mock(AccountService.class);
    long containerDeletionRetentionDays = 2;
    cloudDestination = new LatchBasedInMemoryCloudDestination(new ArrayList<>());
    cloudContainerDeletionSyncTask =
        new CloudContainerDeletionSyncTask(accountService, containerDeletionRetentionDays, cloudDestination,
            ALL_PARTITIONS);
  }

  /**
   * Test that the {@link CloudContainerDeletionSyncTask} is able to sync between account service and cloud destination.
   */
  @Test
  public void testContainerDeletionSyncTask() {
    Set<Container> deleteInProgressSet =
        new HashSet<>(Arrays.asList(Mockito.mock(Container.class), Mockito.mock(Container.class)));
    Set<Container> inactiveSet =
        new HashSet<>(Arrays.asList(Mockito.mock(Container.class), Mockito.mock(Container.class)));
    Mockito.when(accountService.getDeprecatedContainers(ArgumentMatchers.anyLong())).thenCallRealMethod();
    Mockito.when(accountService.getContainersByStatus(Container.ContainerStatus.DELETE_IN_PROGRESS))
        .thenReturn(deleteInProgressSet);
    Mockito.when(accountService.getContainersByStatus(Container.ContainerStatus.INACTIVE)).thenReturn(inactiveSet);
    Assert.assertTrue("There should be no deleted containers in cloud before running the container deletion sync task",
        cloudDestination.getDeletedContainers().isEmpty());
    cloudContainerDeletionSyncTask.run();
    Assert.assertEquals("After sync deleted containers should be same in cloud and account service",
        cloudDestination.getDeletedContainers().size(), deleteInProgressSet.size() + inactiveSet.size());
  }
}
