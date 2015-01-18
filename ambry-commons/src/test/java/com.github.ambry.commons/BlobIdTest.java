package com.github.ambry.commons;

import com.github.ambry.clustermap.Partition;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.PartitionState;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class BlobIdTest {
  @Test
  public void basicTest() {
    final long id = 99;
    final long replicaCapacityInBytes = 1024 * 1024 * 1024;
    PartitionId partitionId = new Partition(id, PartitionState.READ_WRITE, replicaCapacityInBytes);
    BlobId blobId = new BlobId(partitionId);

    assertEquals(blobId.getPartition(), partitionId);
    System.out.println("Blob Id toString: " + blobId);
    System.out.println("Blob id sizeInBytes: " + blobId.toString().length());
  }
}
