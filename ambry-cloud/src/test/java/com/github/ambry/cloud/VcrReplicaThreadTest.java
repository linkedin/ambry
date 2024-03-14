package com.github.ambry.cloud;

import com.github.ambry.cloud.azure.AzuriteUtils;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class VcrReplicaThreadTest {
  protected final VerifiableProperties properties;
  private static final Logger logger = LoggerFactory.getLogger(VcrReplicaThreadTest.class);

  public VcrReplicaThreadTest() {
    properties = new VerifiableProperties(new AzuriteUtils().getAzuriteConnectionProperties());
  }

  @Test
  public void testCustomFilter() throws IOException {
    // Create test cluster MAP
    int NUM_NODES = 5;
    int NUM_PARTITIONS = 10;
    MockClusterMap clustermap = new MockClusterMap(false, false, NUM_NODES,
        1, NUM_PARTITIONS, true, false,
        "localhost");
    // Give hosts a name
    AtomicInteger ai = new AtomicInteger(0);
    int Z = 'Z';
    clustermap.getDataNodes().forEach(d -> d.setHostname(String.valueOf((char)(Z - (ai.getAndIncrement() % 26)))));

    // Create a test-thread
    List<PartitionId> partitions = clustermap.getAllPartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS);
    DataNodeId localhost = partitions.get(0).getReplicaIds().get(0).getDataNodeId();
    VcrReplicaThread replicaThread =
        new VcrReplicaThread("vcrReplicaThreadTest", null, clustermap,
            new AtomicInteger(0), localhost, null,
            null,
            null, null, false,
            "localhost", new ResponseHandler(clustermap), new SystemTime(), null,
            null, null, null, null,
            properties);

    // Assign replicas to test-thread
    Map<DataNodeId, List<RemoteReplicaInfo>> nodes = new HashMap<>();
    partitions.forEach(partition -> partition.getReplicaIds().forEach(replica -> {
      RemoteReplicaInfo rinfo =
          new RemoteReplicaInfo(replica, null, null, new MockFindToken(0, 0),
              Long.MAX_VALUE, SystemTime.getInstance(),
              new Port(replica.getDataNodeId().getPort(), PortType.PLAINTEXT));
      DataNodeId dnode = replica.getDataNodeId();
      List rlist = nodes.getOrDefault(dnode, new ArrayList<>());
      rlist.add(rinfo);
      nodes.putIfAbsent(dnode, rlist);
      replicaThread.addRemoteReplicaInfo(rinfo);
    }));

    // Call custom-filter. Each time its called, it picks one replica per partition per node.
    // If we call NUM_NODES, then all replicas across all nodes are covered.
    HashMap<Long, List<String>> replicas = new HashMap<>();
    IntStream.rangeClosed(1,NUM_NODES).forEach(i -> replicaThread.customFilter(nodes).forEach((dnode, rlist) -> rlist.forEach(r -> {
      long pid = r.getReplicaId().getPartitionId().getId();
      List dlist = replicas.getOrDefault(pid, new ArrayList<>());
      dlist.add(dnode.getHostname());
      replicas.putIfAbsent(pid, dlist);
    })));

    // Check that all replicas are covered
    replicas.keySet().forEach(pid -> {
      List<String> dlist = replicas.get(pid);
      List<String> slist = dlist.stream().sorted().collect(Collectors.toList());
      if (!slist.equals(dlist)) {
        logger.error("Replicas are not sorted for partition {}, original list = [{}], sorted list = [{}]",
            pid, String.join(", ", dlist), String.join(", ", slist));
        Assert.assertTrue(false);
      }
    });
  }

}
