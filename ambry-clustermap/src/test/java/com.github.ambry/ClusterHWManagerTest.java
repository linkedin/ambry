package com.github.ambry;

import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * Created with IntelliJ IDEA. User: jwylie Date: 10/29/13 Time: 11:02 AM To change this template use File | Settings |
 * File Templates.
 */
public class ClusterHWManagerTest {

    // TODO: Will want a shared test file with helper methods like this.
    public ClusterHW getTestClusterHW() {
        ClusterHWManager clusterHWManager = new ClusterHWManager();

        clusterHWManager.addNewDatacenter("LVA1");
        clusterHWManager.addNewDataNode("LVA1", "lva1-app000.prod");
        clusterHWManager.addNewDataNode("LVA1", "lva1-app001.prod");
        clusterHWManager.addNewDataNode("LVA1", "lva1-app002.prod");
        clusterHWManager.addNewDataNode("LVA1", "lva1-app003.prod");
        clusterHWManager.addNewDisks(5, "lva1-app000.prod", 1000);
        clusterHWManager.addNewDisks(5, "lva1-app001.prod", 1000);
        clusterHWManager.addNewDisks(5, "lva1-app002.prod", 1000);
        clusterHWManager.addNewDisks(5, "lva1-app003.prod", 1000);

        clusterHWManager.addNewDatacenter("ELA4");
        clusterHWManager.addNewDataNode("ELA4", "ela4-app000.prod");
        clusterHWManager.addNewDataNode("ELA4", "ela4-app001.prod");
        clusterHWManager.addNewDataNode("ELA4", "ela4-app002.prod");
        clusterHWManager.addNewDataNode("ELA4", "ela4-app003.prod");
        clusterHWManager.addNewDisks(5, "ela4-app000.prod", 1000);
        clusterHWManager.addNewDisks(5, "ela4-app001.prod", 1000);
        clusterHWManager.addNewDisks(5, "ela4-app002.prod", 1000);
        clusterHWManager.addNewDisks(5, "ela4-app003.prod", 1000);

        return clusterHWManager.getClusterHW();
    }

    public ClusterLV getTestClusterLV(ClusterHWManager clusterHWManager) {
        final long capacityGB = 100;
        final long maxDiskId = 40; // From getTestClusterHW()
        ClusterLVManager clusterLVManager = new ClusterLVManager(clusterHWManager);

        for(long i = 0; i <5; i++) {
            Set<Long> diskIds = new HashSet<Long>();
            for (long j = 0; j < 6; j++) {
                diskIds.add((i + ((j+1)*6)) % maxDiskId);
            }
            clusterLVManager.addNewLogicalVolume(diskIds, capacityGB);
        }

        return clusterLVManager.getClusterLV();
    }

    @Test
    public void e2eJsonSerDeClusterHWTest() {
        String filename = "/tmp/ambry/hw.json";

        ClusterHW clusterHW = getTestClusterHW();
        ClusterHWManager clusterHWManager = new ClusterHWManager(clusterHW);

        System.out.println("INITIAL: " + System.lineSeparator() + clusterHWManager);
        clusterHWManager.toJson(filename);

        ClusterHWManager ct2 = ClusterHWManager.makeFromJson(filename);
        System.out.println("SECOND:" + System.lineSeparator() + ct2);
    }

    @Test
    public void e2eJsonSerDeClusterLVTest() {
        String filename = "/tmp/ambry/lv.json";

        ClusterHWManager clusterHWManager = new ClusterHWManager(getTestClusterHW());
        ClusterLV clusterLV = getTestClusterLV(clusterHWManager);

        ClusterLVManager clusterLVManager = new ClusterLVManager(clusterHWManager, clusterLV);

        System.out.println("INITIAL: " + System.lineSeparator() + clusterLVManager);
        clusterLVManager.toJson(filename);

        ClusterLVManager clusterLVManagerCopy = ClusterLVManager.makeFromJson(filename, clusterHWManager);
        System.out.println("SECOND:" + System.lineSeparator() + clusterLVManagerCopy);
    }

    @Test
    public void capacityHackTest() {
        ClusterHW clusterHW = getTestClusterHW();
        ClusterHWTopology clusterHWTopology = new ClusterHWTopology(clusterHW);

        System.out.println("Total capacity: " + clusterHWTopology.getCapacityGB());

        for (Datacenter datacenter : clusterHW.getDatacenters()) {
            System.out.println( datacenter.getName() + " : " + clusterHWTopology.getCapacityGB(datacenter));
        }

        for (DataNode dataNode : clusterHW.getDataNodes()) {
            System.out.println(dataNode.getHostname()  + " : " + clusterHWTopology.getCapacityGB(dataNode));
        }
    }

    @Test
    public void randomTestOfStuff() {
        ClusterHW clusterHW = getTestClusterHW();
        ClusterHWManager clusterHWManager = new ClusterHWManager(clusterHW);
        ClusterHWTopology clusterHWTopology = new ClusterHWTopology(clusterHW);

        ClusterLV clusterLV = getTestClusterLV(clusterHWManager);
        ClusterLVManager clusterLVManager = new ClusterLVManager(clusterHWManager, clusterLV);
        ClusterLVTopology clusterLVTopology = new ClusterLVTopology(clusterHWTopology, clusterLV);

        System.out.println("Capacity (GB):");
        System.out.println("\tRaw: " + clusterHWTopology.getCapacityGB());
        System.out.println("\tAllocated: " + clusterLVTopology.getAllocatedCapacityGB());
        System.out.println("\tFree: " + clusterLVTopology.getFreeCapacityGB());

        CoordinatorClusterMap coordinatorClusterMap = new CoordinatorClusterMap(clusterHWTopology, clusterLVTopology);
        System.out.println("Read-write logical volumes:");
        for(LogicalVolume logicalVolume : coordinatorClusterMap.getReadWriteLogicalVolumes()) {
            System.out.println("\t" + logicalVolume.getLogicalVolumeId());
        }

        DataNodeClusterMap dataNodeClusterMap = new DataNodeClusterMap(clusterHWTopology, clusterLVTopology);
        System.out.println("Peer replicated stores:");
        for(ReplicatedStore replicatedStore : clusterLVTopology.getReplicatedStores()) {
            StringBuilder sb = new StringBuilder();
            sb.append("\t").append(replicatedStore).append(" ~ ");
            for(ReplicatedStore peerReplicatedStore : dataNodeClusterMap.getPeerReplicatedStores(replicatedStore)) {
                sb.append(peerReplicatedStore).append(" ");
            }
            System.out.println(sb.toString());
        }
    }



}
