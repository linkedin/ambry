package com.github.ambry.cloud.azure;

import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


public class AzureStorageContainerMetricsTest {

  private AzureStorageContainerMetrics partitionMetrics;

  public AzureStorageContainerMetricsTest() {
    partitionMetrics = new AzureStorageContainerMetrics(314L);
  }

  @Test
  public void testAzureStorageContainerMetrics() {
    // no replicas
    assertEquals(Optional.of(-1L).get(), partitionMetrics.getPartitionLag());

    // one replica
    partitionMetrics.setPartitionReplicaLag("localhost1", 100);
    assertEquals(Optional.of(100L).get(), partitionMetrics.getPartitionLag());

    // two replicas
    partitionMetrics.setPartitionReplicaLag("localhost1", 100);
    partitionMetrics.setPartitionReplicaLag("localhost2", 1000);
    assertEquals(Optional.of(100L).get(), partitionMetrics.getPartitionLag());

    // two replicas, lag changes
    partitionMetrics.setPartitionReplicaLag("localhost2", 10);
    assertEquals(Optional.of(10L).get(), partitionMetrics.getPartitionLag());

    // one replica removed
    partitionMetrics.removePartitionReplica("localhost2");
    assertEquals(Optional.of(100L).get(), partitionMetrics.getPartitionLag());

    // both replicas removed
    partitionMetrics.removePartitionReplica("localhost1");
    assertEquals(Optional.of(-1L).get(), partitionMetrics.getPartitionLag());
  }

}
