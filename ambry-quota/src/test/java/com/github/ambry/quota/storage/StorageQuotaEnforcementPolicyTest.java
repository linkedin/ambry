/**
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.quota.storage;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.config.StorageQuotaConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.SystemTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.*;


/**
 * Unit test for various {@link StorageQuotaEnforcementPolicy}s.
 */
public class StorageQuotaEnforcementPolicyTest {

  /**
   * Unit test for {@link SimpleStorageQuotaEnforcementPolicy}.
   */
  @Test
  public void testSimpleStorageQuotaEnforcementPolicy() {
    StorageQuotaEnforcementPolicy policy = new SimpleStorageQuotaEnforcementPolicy();
    Assert.assertTrue(policy.shouldThrottleRequest(1000, 1000));
    Assert.assertTrue(policy.shouldThrottleRequest(1000, 1001));
    Assert.assertFalse(policy.shouldThrottleRequest(1000, 500));
  }

  /**
   * Unit test for {@link AdaptiveStorageQuotaEnforcementPolicy}.
   */
  @Test
  public void testAdaptiveStorageQuotaEnforcementPolicy() {
    ClusterMap clusterMap = mock(ClusterMap.class);
    MockTime mockTime = new MockTime(SystemTime.getInstance().milliseconds());
    // make threshold as 25, which means as long writable partition is more than 25% of the all partitions, we don't throttle
    when(clusterMap.getWritablePartitionIds(any())).thenAnswer(invocation -> makePartitionIdList(30));
    when(clusterMap.getAllPartitionIds(any())).thenAnswer(invocation -> makePartitionIdList(100));

    AdaptiveStorageQuotaEnforcementPolicy policy =
        new AdaptiveStorageQuotaEnforcementPolicy(clusterMap, mockTime, makeStorageQuotaConfig(10 * 60 * 1000, 25));

    Assert.assertFalse(policy.shouldThrottleRequest(1000, 1000));
    Assert.assertFalse(policy.shouldThrottleRequest(1000, 1001));
    Assert.assertFalse(policy.shouldThrottleRequest(1000, 500));

    // Change the writable partition number, but the cached value will not get updated, should throttle return false.
    when(clusterMap.getWritablePartitionIds(any())).thenAnswer(invocation -> makePartitionIdList(10));
    Assert.assertFalse(policy.shouldThrottleRequest(1000, 1000));
    Assert.assertFalse(policy.shouldThrottleRequest(1000, 1001));
    Assert.assertFalse(policy.shouldThrottleRequest(1000, 500));

    // Sleep for more than the update interval, now it should start throttling.
    mockTime.sleep(10 * 60 * 1000 + 1);
    Assert.assertTrue(policy.shouldThrottleRequest(1000, 1000));
    Assert.assertTrue(policy.shouldThrottleRequest(1000, 1001));
    Assert.assertFalse(policy.shouldThrottleRequest(1000, 500));
  }

  /**
   * Making a list of partitions with the given size.
   * @param num The number of partitions to return.
   * @return
   */
  private List<? extends PartitionId> makePartitionIdList(int num) {
    ArrayList<PartitionId> result = new ArrayList<>(num);
    for (int i = 0; i < num; i++) {
      result.add(new MockPartitionId(i, "default"));
    }
    return result;
  }

  /**
   * Making a {@link StorageQuotaConfig} with the given values.
   * @param interval The interval in ms to update clustermap value is adaptive enforcement policy
   * @param threshold The writable partition percentage threshold in adaptive enforcement policy
   * @return
   */
  private StorageQuotaConfig makeStorageQuotaConfig(long interval, int threshold) {
    Properties prop = new Properties();
    prop.setProperty(StorageQuotaConfig.ADAPTIVE_ENFORCEMENT_CLUSTERMAP_UPDATE_INTERVAL_MS, String.valueOf(interval));
    prop.setProperty(StorageQuotaConfig.ADAPTIVE_ENFORCEMENT_WRITEABLE_PARTITION_THRESHOLD, String.valueOf(threshold));
    return new StorageQuotaConfig(new VerifiableProperties(prop));
  }
}
