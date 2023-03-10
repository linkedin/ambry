/*
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
package com.github.ambry.clustermap;

import org.junit.Assert;
import org.junit.Test;


/**
 * Tests for {@link ReplicaSealStatus}.
 */
public class ReplicaSealStatusTest {

  /**
   * Test {@link ReplicaSealStatus#mergeReplicaSealStatus(ReplicaSealStatus, ReplicaSealStatus)}.
   */
  @Test
  public void testMergeReplicaSealStatus() {
    ReplicaSealStatus[] replicaSealStatuses = ReplicaSealStatus.values();
    for (ReplicaSealStatus replicaSealStatus1 : replicaSealStatuses) {
      for (ReplicaSealStatus replicaSealStatus2 : replicaSealStatuses) {
        ReplicaSealStatus mergedStatus =
            ReplicaSealStatus.mergeReplicaSealStatus(replicaSealStatus1, replicaSealStatus2);
        Assert.assertEquals(mergedStatus,
            replicaSealStatuses[Math.max(replicaSealStatus1.ordinal(), replicaSealStatus2.ordinal())]);
      }
    }
  }
}
