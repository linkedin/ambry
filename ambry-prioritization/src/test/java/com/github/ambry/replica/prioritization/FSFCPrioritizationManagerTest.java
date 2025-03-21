package com.github.ambry.replica.prioritization;

import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


public class FSFCPrioritizationManagerTest {
  private MockClusterMap clusterMap;

  @Before
  public void initializeCluster() throws IOException {
    clusterMap = new MockClusterMap(false, true, 1,
        10, 3, false,
        false, null);
  }
   @Test
   public void testAddReplica() {
     FCFSPrioritizationManager prioritizationManager = new FCFSPrioritizationManager();
     int partitionId1 = 1;
     int partitionId2 = 2;
     int partitionId3 = 3;
     int mountPathIndex1 = 1;
     int mountPathIndex2 = 2;
     int mountPathIndex3 = 3;
     PartitionId partition1 =
         new MockPartitionId(partitionId1, MockClusterMap.DEFAULT_PARTITION_CLASS,
             clusterMap.getDataNodes(), mountPathIndex1);
      PartitionId partition2 =
         new MockPartitionId(partitionId2, MockClusterMap.DEFAULT_PARTITION_CLASS,
             clusterMap.getDataNodes(), mountPathIndex2);
      PartitionId partition3 =
          new MockPartitionId(partitionId3, MockClusterMap.DEFAULT_PARTITION_CLASS,
              clusterMap.getDataNodes(), mountPathIndex3);
     assertFalse(prioritizationManager.addReplica(partition1.getReplicaIds().get(0)));
     assertFalse(prioritizationManager.addReplica(partition2.getReplicaIds().get(0)));
     assertFalse(prioritizationManager.addReplica(partition3.getReplicaIds().get(0)));

     prioritizationManager.start();
     assertTrue(prioritizationManager.addReplica(partition1.getReplicaIds().get(0)));
     assertTrue(prioritizationManager.addReplica(partition2.getReplicaIds().get(0)));
     assertTrue(prioritizationManager.addReplica(partition3.getReplicaIds().get(0)));
   }

  @Test
  public void test1(){

  }
//   @Test
//    public void testAdditionAndRemovalOfReplicasFromPrioritizationManager() throws InterruptedException {
//      FCFSPrioritizationManager prioritizationManager = new FCFSPrioritizationManager();
//      Map<Integer, List<PartitionId>> diskToPartitionMap = new ConcurrentHashMap<>();
//
//      prioritizationManager.start();
//
////      int partitionCounter = 1;
////      for(int diskId=1; diskId <= 10;diskId++){
////        for(int numPartitions=0 ; numPartitions < 5; numPartitions++){
////          PartitionId partition =
////              new MockPartitionId(partitionCounter++, MockClusterMap.DEFAULT_PARTITION_CLASS,
////                  clusterMap.getDataNodes(), diskId-1);
////          diskToPartitionMap.putIfAbsent(diskId, new ArrayList<>());
////          diskToPartitionMap.get(diskId).add(partition);
////          prioritizationManager.addReplica(partition.getReplicaIds().get(0));
////          assertTrue(prioritizationManager.addReplica(partition.getReplicaIds().get(0)));
////        }
////      }
//
//      ExecutorService executor = Executors.newFixedThreadPool(10);
//      for(int diskIndex=1; diskIndex <= 10;diskIndex++){
//        int finalDiskIndex = diskIndex;
//        int finalDiskIndex1 = diskIndex;
//        executor.submit(() -> {
//          int partitionCount = 10*finalDiskIndex;
//          for(int numPartitions=0 ; numPartitions < 5; numPartitions++){
//            PartitionId partition =
//                new MockPartitionId(partitionCount + numPartitions, MockClusterMap.DEFAULT_PARTITION_CLASS,
//                    clusterMap.getDataNodes(), finalDiskIndex);
//            DiskId diskId = clusterMap.getDataNodes().get(0).getDiskIds().get(finalDiskIndex -1);
//            diskToPartitionMap.putIfAbsent(finalDiskIndex1, new ArrayList<>());
//            diskToPartitionMap.get(finalDiskIndex1).add(partition);
//            assertTrue(prioritizationManager.addReplica(partition.getReplicaIds().get(0)));
//
//          }
//        });
//      }
//
//     executor.awaitTermination(3, TimeUnit.SECONDS);
//
//      for(int diskIndex=1; diskIndex <= 10;diskIndex++){
//        for(PartitionId partition : diskToPartitionMap.get(diskIndex)){
//          assertTrue(prioritizationManager.removeReplica(clusterMap.getDataNodes().get(0).getDiskIds().get(diskIndex-1),
//              partition.getReplicaIds().get(0)));
//        }
//      }
//
//
//
//    }


}
