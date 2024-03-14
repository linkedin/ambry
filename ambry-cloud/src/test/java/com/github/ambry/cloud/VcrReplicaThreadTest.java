package com.github.ambry.cloud;

import com.github.ambry.cloud.azure.AzuriteUtils;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.commons.ResponseHandler;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.replication.MockFindToken;
import com.github.ambry.replication.RemoteReplicaInfo;
import com.github.ambry.utils.SystemTime;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;


public class VcrReplicaThreadTest {
  protected final VerifiableProperties properties;

  public VcrReplicaThreadTest() {
    properties = new VerifiableProperties(new AzuriteUtils().getAzuriteConnectionProperties());
  }

  @Test
  public void testCustomFilter() throws IOException {
    // Create test cluster
    MockClusterMap mockClusterMap = new MockClusterMap(false, false, 3,
        1, 3, true, false,
        "localhost");
    // Create a test partition
    List<PartitionId> partitions = mockClusterMap.getAllPartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS);
    DataNodeId localhost = partitions.get(0).getReplicaIds().get(0).getDataNodeId();
    VcrReplicaThread replicaThread =
        new VcrReplicaThread("vcrReplicaThreadTest", null, mockClusterMap,
            new AtomicInteger(0), localhost, null,
            null,
            null, null, false,
            "localhost", new ResponseHandler(mockClusterMap), new SystemTime(), null,
            null, null, null, null,
            properties);

    Map<DataNodeId, List<RemoteReplicaInfo>> replicas = new HashMap<>();
    for (PartitionId partition : partitions) {
      for (ReplicaId replica : partition.getReplicaIds()) {

        DataNodeId dnode = replica.getDataNodeId();
        List alist = replicas.getOrDefault(dnode, new ArrayList<>());
        alist.add(replica);
        replicas.putIfAbsent(dnode, alist);

        RemoteReplicaInfo rinfo =
            new RemoteReplicaInfo(replica, null, null, new MockFindToken(0, 0),
                Long.MAX_VALUE, SystemTime.getInstance(),
                new Port(replica.getDataNodeId().getPort(), PortType.PLAINTEXT));
        replicaThread.addRemoteReplicaInfo(rinfo);
      }
    }

    replicaThread.customFilter(replicas);
  }

}
