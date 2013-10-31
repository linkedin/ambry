package com.github.ambry;

import java.util.*;

/**
 * ClusterHWTopology wraps up the physical topology of an Ambry instance. This includes relationships among all physical
 * components (datacenters, data nodes, and disks).
 */
public class ClusterHWTopology {
    private ClusterHW clusterHW;

    private Map<String,Datacenter> datacenterMap;
    private Map<String,DataNode> dataNodeMap;
    private Map<Long,Disk> diskIdMap;

    private Map<Datacenter,Set<DataNode>> nodesInDatacenterMap;
    private Map<Datacenter,Set<Disk>> disksInDatacenterMap;
    private Map<DataNode,Set<Disk>> disksInDataNodeMap;

    // Use ClusterHWManager to confirm clusterHW is kosher
    public ClusterHWTopology(ClusterHW clusterHW) {
        this.clusterHW = clusterHW;

        this.datacenterMap = new HashMap<String,Datacenter>();
        this.dataNodeMap = new  HashMap<String,DataNode>();
        this.diskIdMap = new HashMap<Long,Disk>();

        this.nodesInDatacenterMap = new HashMap<Datacenter,Set<DataNode>>();
        this.disksInDatacenterMap = new HashMap<Datacenter,Set<Disk>>();
        this.disksInDataNodeMap = new HashMap<DataNode,Set<Disk>>();

        for (Datacenter datacenter : clusterHW.getDatacenters()) {
            datacenterMap.put(datacenter.getName(), datacenter);

            nodesInDatacenterMap.put(datacenter, new HashSet<DataNode>());
            disksInDatacenterMap.put(datacenter, new HashSet<Disk>());
        }

        for (DataNode dataNode : clusterHW.getDataNodes()) {
            dataNodeMap.put(dataNode.getHostname(), dataNode);

            disksInDataNodeMap.put(dataNode, new HashSet<Disk>());

            Datacenter dc = datacenterMap.get(dataNode.getDatacenterName());
            nodesInDatacenterMap.get(dc).add(dataNode);
        }

        for (Disk disk : clusterHW.getDisks()) {
            diskIdMap.put(disk.getDiskId(), disk);

            DataNode dataNode = dataNodeMap.get(disk.getDataNodeName());
            disksInDataNodeMap.get(dataNode).add(disk);

            Datacenter dc = datacenterMap.get(dataNode.getDatacenterName());
            disksInDatacenterMap.get(dc).add(disk);
        }
    }

    protected long getCapacityGB(Iterator<Disk> disks) {
        long capacityGB = 0;
        while(disks.hasNext()) {
            capacityGB += disks.next().getCapacityGB();
        }
        return capacityGB;
    }

    public long getCapacityGB() {
        return getCapacityGB(clusterHW.getDisks().iterator());
    }

    public long getCapacityGB(Datacenter datacenter) {
        return getCapacityGB(disksInDatacenterMap.get(datacenter).iterator());
    }

    public long getCapacityGB(DataNode dataNode) {
        return getCapacityGB(disksInDataNodeMap.get(dataNode).iterator());
    }

    public List<Datacenter> getDatacenters() {
        return clusterHW.getDatacenters();
    }

    public List<DataNode> getDataNodes() {
        return clusterHW.getDataNodes();
    }

    public List<Disk> getDisks() {
        return clusterHW.getDisks();
    }

    public Disk getDisk(long diskId) {
        return diskIdMap.get(diskId);
    }

    public DataNode getDataNode(Disk disk) {
        return dataNodeMap.get(disk.getDataNodeName());
    }

    public Datacenter getDatacenter(DataNode dataNode) {
        return datacenterMap.get(dataNode.getDatacenterName());
    }

}
