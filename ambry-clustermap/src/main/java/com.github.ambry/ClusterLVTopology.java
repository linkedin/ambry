package com.github.ambry;

import java.util.*;

/**
 * ClusterLVTopology wraps up the logical topology of an Ambry instance. This includes relationships among logical
 * components (logical volumes and replicated stores) as well as the relationship between logical components and
 * physical components.
 *
 * Ensures higher level constraints are maintained:
 * - Replication policy of logical volumes are obeyed
 * - Capacity is not over-allocated
 */
public class ClusterLVTopology {
    private ClusterHWTopology clusterHWTopology;
    private ClusterLV clusterLV;

    private Map<Long, LogicalVolume> logicalVolumeMap;

    private Map<ReplicatedStore, Disk>  replicatedStoreDiskMap;
    private Map<ReplicatedStore, DataNode>  replicatedStoreDataNodeMap;
    private Map<ReplicatedStore, Datacenter> replicatedStoreDatacenterMap;

    private Map<LogicalVolume,Set<ReplicatedStore>> replicatedStoresInLogicalVolume;
    private Map<Disk,Set<ReplicatedStore>> replicatedStoresInDisk;
    private Map<DataNode,Set<ReplicatedStore>> replicatedStoresInDataNode;
    private Map<Datacenter,Set<ReplicatedStore>> replicatedStoresInDatacenter;

    public ClusterLVTopology(ClusterHWTopology clusterHWTopology, ClusterLV clusterLV) {
        this.clusterHWTopology = clusterHWTopology;
        this.clusterLV = clusterLV;

        this.logicalVolumeMap = new HashMap<Long, LogicalVolume>();


        this.replicatedStoreDiskMap = new HashMap<ReplicatedStore, Disk>();
        this.replicatedStoreDataNodeMap = new HashMap<ReplicatedStore, DataNode>();
        this.replicatedStoreDatacenterMap = new HashMap<ReplicatedStore, Datacenter>();

        this.replicatedStoresInLogicalVolume = new HashMap<LogicalVolume, Set<ReplicatedStore>>();
        this.replicatedStoresInDisk = new HashMap<Disk, Set<ReplicatedStore>>();
        this.replicatedStoresInDataNode = new HashMap<DataNode, Set<ReplicatedStore>>();
        this.replicatedStoresInDatacenter = new HashMap<Datacenter, Set<ReplicatedStore>>();

        for (LogicalVolume logicalVolume : clusterLV.getLogicalVolumes()) {
            logicalVolumeMap.put(logicalVolume.getLogicalVolumeId(), logicalVolume);

            replicatedStoresInLogicalVolume.put(logicalVolume, new HashSet<ReplicatedStore>());
        }

        for (Disk disk : clusterHWTopology.getDisks()) {
            replicatedStoresInDisk.put(disk, new HashSet<ReplicatedStore>());
        }
        for (DataNode dataNode : clusterHWTopology.getDataNodes()) {
            replicatedStoresInDataNode.put(dataNode, new HashSet<ReplicatedStore>());
        }
        for (Datacenter datacenter : clusterHWTopology.getDatacenters()) {
            replicatedStoresInDatacenter.put(datacenter, new HashSet<ReplicatedStore>());
        }

        for (ReplicatedStore replicatedStore : clusterLV.getReplicatedStores()) {
            LogicalVolume logicalVolume = logicalVolumeMap.get(replicatedStore.getLogicalVolumeId());
            Disk disk = clusterHWTopology.getDisk(replicatedStore.getDiskId());
            DataNode dataNode = clusterHWTopology.getDataNode(disk);
            Datacenter datacenter = clusterHWTopology.getDatacenter(dataNode);

            replicatedStoreDiskMap.put(replicatedStore, disk);
            replicatedStoreDataNodeMap.put(replicatedStore, dataNode);
            replicatedStoreDatacenterMap.put(replicatedStore, datacenter);

            replicatedStoresInLogicalVolume.get(logicalVolume).add(replicatedStore);
            replicatedStoresInDisk.get(disk).add(replicatedStore);
            replicatedStoresInDataNode.get(dataNode).add(replicatedStore);
            replicatedStoresInDatacenter.get(datacenter).add(replicatedStore);
        }

        if (!isValid()) {
            // TODO: Log
            // TODO: Throw an exception?
        }
    }

    protected boolean isLogicalVolumeValid(LogicalVolume logicalVolume) {
        boolean valid = true;

        Set<Disk> disks = new HashSet<Disk>();
        Set<DataNode> dataNodes = new HashSet<DataNode>();
        Map<Datacenter,Integer> datacenterReplicaCount = new HashMap<Datacenter, Integer>();
        for (ReplicatedStore replicatedStore : replicatedStoresInLogicalVolume.get(logicalVolume)) {
            if (!disks.add(replicatedStoreDiskMap.get(replicatedStore))) {
                // TODO: Log
                valid = false;
            }

            if (!dataNodes.add(replicatedStoreDataNodeMap.get(replicatedStore))) {
                // TODO: Log
                valid = false;
            }

            Datacenter datacenter = replicatedStoreDatacenterMap.get(replicatedStore);
            if(!datacenterReplicaCount.containsKey(datacenter)) {
                datacenterReplicaCount.put(datacenter, 0);
            }
            datacenterReplicaCount.put(datacenter, datacenterReplicaCount.get(datacenter)+1);
            // TODO: Need replication policy to verify replicas count per data center.
        }

        return valid;
    }

    protected boolean isValid() {
        boolean valid = true;
        for(LogicalVolume logicalVolume : clusterLV.getLogicalVolumes()) {
            if (!isLogicalVolumeValid(logicalVolume)) {
                // TODO: Log
                valid = false;
            }
        }

        for(Disk disk : clusterHWTopology.getDisks()) {
            if(getAllocatedCapacityGB(disk) > disk.getCapacityGB()) {
                // TODO: Log
                valid = false;
            }
        }

        return valid;
    }

    public List<LogicalVolume> getLogicalVolumes() {
        return clusterLV.getLogicalVolumes();
    }

    public List<ReplicatedStore> getReplicatedStores() {
        return clusterLV.getReplicatedStores();
    }

    public Set<ReplicatedStore> getReplicatedStores(LogicalVolume logicalVolume) {
        return replicatedStoresInLogicalVolume.get(logicalVolume);
    }

    public LogicalVolume getLogicalVolume(long logicalVolumeId) {
        return logicalVolumeMap.get(logicalVolumeId);
    }

    protected long getAllocatedCapacityGB(Iterator<ReplicatedStore> replicatedStores) {
        long allocatedCapacityGB = 0;
        while(replicatedStores.hasNext()) {
            allocatedCapacityGB += logicalVolumeMap.get(replicatedStores.next().getLogicalVolumeId()).getCapacityGB();
        }
        return allocatedCapacityGB;
    }

    public long getAllocatedCapacityGB() {
        return getAllocatedCapacityGB(clusterLV.getReplicatedStores().iterator());
    }

    public long getAllocatedCapacityGB(Datacenter datacenter) {
        return getAllocatedCapacityGB(replicatedStoresInDatacenter.get(datacenter).iterator());
    }

    public long getAllocatedCapacityGB(DataNode dataNode) {
        return getAllocatedCapacityGB(replicatedStoresInDataNode.get(dataNode).iterator());
    }

    public long getAllocatedCapacityGB(Disk disk) {
        return getAllocatedCapacityGB(replicatedStoresInDisk.get(disk).iterator());
    }

    public long getFreeCapacityGB() {
        return clusterHWTopology.getCapacityGB() - getAllocatedCapacityGB();
    }

    public long getFreeCapacityGB(Datacenter datacenter) {
        return clusterHWTopology.getCapacityGB(datacenter) - getAllocatedCapacityGB(datacenter);
    }

    public long getFreeCapacityGB(DataNode dataNode) {
        return clusterHWTopology.getCapacityGB(dataNode) - getAllocatedCapacityGB(dataNode);
    }

    public long getFreeCapacityGB(Disk disk) {
        return disk.getCapacityGB() - getAllocatedCapacityGB(disk);
    }

}
