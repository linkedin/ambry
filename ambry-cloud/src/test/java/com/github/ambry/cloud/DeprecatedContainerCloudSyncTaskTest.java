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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.AccountService;
import com.github.ambry.account.Container;
import com.github.ambry.clustermap.ClusterMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;


/**
 * Test {@link DeprecatedContainerCloudSyncTask}.
 */
@RunWith(MockitoJUnitRunner.class)
public class DeprecatedContainerCloudSyncTaskTest {

  private final DeprecatedContainerCloudSyncTask deprecatedContainerCloudSyncTask;
  private final AccountService accountService;
  private final LatchBasedInMemoryCloudDestination cloudDestination;

  /**
   * Constructor for {@link DeprecatedContainerCloudSyncTaskTest}.
   */
  public DeprecatedContainerCloudSyncTaskTest() {
    accountService = Mockito.mock(AccountService.class);
    long containerDeletionRetentionDays = 2;
    cloudDestination = new LatchBasedInMemoryCloudDestination(new ArrayList<>(), Mockito.mock(ClusterMap.class));
    deprecatedContainerCloudSyncTask =
        new DeprecatedContainerCloudSyncTask(accountService, containerDeletionRetentionDays, cloudDestination,
            new VcrMetrics(new MetricRegistry()));
  }

  /**
   * Test that the {@link DeprecatedContainerCloudSyncTask} is able to sync between account service and cloud destination.
   */
  @Test
  public void testDeprecatedContainerCloudSyncTask() {
    Set<Container> deleteInProgressSet =
        new HashSet<>(Arrays.asList(Mockito.mock(Container.class), Mockito.mock(Container.class)));
    Set<Container> inactiveSet =
        new HashSet<>(Arrays.asList(Mockito.mock(Container.class), Mockito.mock(Container.class)));
    Mockito.when(accountService.getContainersByStatus(Container.ContainerStatus.DELETE_IN_PROGRESS))
        .thenReturn(deleteInProgressSet);
    Mockito.when(accountService.getContainersByStatus(Container.ContainerStatus.INACTIVE)).thenReturn(inactiveSet);
    Assert.assertTrue(
        "There should be no deprecated containers in cloud before running the container deletion sync task",
        cloudDestination.getDeletedContainers().isEmpty());
    deprecatedContainerCloudSyncTask.run();
    Assert.assertEquals("After sync deprecated containers should be same in cloud and account service",
        cloudDestination.getDeletedContainers().size(), deleteInProgressSet.size() + inactiveSet.size());
  }
}
