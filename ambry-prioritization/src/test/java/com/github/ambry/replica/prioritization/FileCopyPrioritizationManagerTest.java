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

import com.github.ambry.clustermap.AmbryDataNode;
import com.github.ambry.clustermap.AmbryDisk;
import com.github.ambry.clustermap.AmbryPartition;
import com.github.ambry.clustermap.AmbryReplica;
import com.github.ambry.clustermap.ClusterManagerQueryHelper;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.replica.prioritization.disruption.DefaultDisruptionService;
import java.io.IOException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;


public class FileCopyPrioritizationManagerTest {
  private MockClusterMap clusterMap;
  FileCopyPrioritizationManager prioritizationManager;

  @Mock
  private ClusterManagerQueryHelper<AmbryReplica, AmbryDisk, AmbryPartition, AmbryDataNode> clusterManagerQueryHelper;

  @Before
  public void initializeCluster() throws IOException {
    clusterMap = new MockClusterMap(false, true, 1, 10, 3, false, false, null);
    prioritizationManager =
        new FileCopyPrioritizationManager(new DefaultDisruptionService(), "", clusterManagerQueryHelper);
  }

  @Test
  public void testAddAndRemoveReplica(){

  }
}
