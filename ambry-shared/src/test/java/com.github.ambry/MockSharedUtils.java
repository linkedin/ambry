package com.github.ambry;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;

/**
 */
public class MockSharedUtils {

  public static MockClusterMap getMockClusterMap() {
    return new MockClusterMap();
  }

  public static PartitionId getMockPartitionId() {
    return new MockPartitionId();
  }
}
