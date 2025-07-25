/**
 * Copyright 2025 LinkedIn Corp. All rights reserved.
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

package com.github.ambry.replica.prioritization;

import com.github.ambry.clustermap.ClusterManagerQueryHelper;
import com.github.ambry.config.ReplicaPrioritizationStrategy;
import com.github.ambry.replica.prioritization.disruption.DefaultDisruptionService;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.*;


public class PrioritizationManagerFactoryTest {

  @Test
  public void testGetPrioritizationManagerFactory() {
    ClusterManagerQueryHelper clusterManagerQueryHelper = Mockito.mock(ClusterManagerQueryHelper.class);
    PrioritizationManagerFactory prioritizationManagerFactory =
        new FileBasedReplicationPrioritizationManagerFactory(new DefaultDisruptionService(), clusterManagerQueryHelper,
            "");
    PrioritizationManager prioritizationManager =
        prioritizationManagerFactory.getPrioritizationManager(ReplicaPrioritizationStrategy.FirstComeFirstServe);

    assertTrue(prioritizationManager instanceof FCFSPrioritizationManager);
    PrioritizationManager prioritizationManager1 = prioritizationManagerFactory.getPrioritizationManager(
        ReplicaPrioritizationStrategy.ACMAdvanceNotificationsBased);
    assertTrue(prioritizationManager1 instanceof FileCopyDisruptionBasedPrioritizationManager);
  }
}
