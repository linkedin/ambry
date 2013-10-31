package com.github.ambry;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * ClusterLVManager constructs and modifies clusterLV objects. I.e., it manages the logical relationships in an Ambry
 * instance. Also provides hooks for SerDe of clusterLV objects.
 *
 * Ensures 'referential integrity' of relationships between logical components and logical+physical components. E.g.,
 * that all replicated stores belong to a disk that exists as well as a logical volume that exists.
 *
 * Ensures uniqueness of all each logical component. E.g., logical volume id, replicated store (lv id & disk id pair).
 */
public class ClusterLVManager {
    private ClusterHWManager clusterHWManager;
    private ClusterLV clusterLV;

    // Structures used to ensure all logical volume IDs are unique.
    private long nextLogicalVolumeId;
    private Set<Long> logicalVolumeIds;

    private ClusterLVManager() {
        this.clusterHWManager = new ClusterHWManager();
        this.clusterLV = new ClusterLV();
        this.nextLogicalVolumeId = 0;
        this.logicalVolumeIds = new HashSet<Long>();
    }

    public ClusterLVManager(ClusterHWManager clusterHWManager) {
        this.clusterHWManager = clusterHWManager;
        this.clusterLV = new ClusterLV();
        this.nextLogicalVolumeId = 0;
        this.logicalVolumeIds = new HashSet<Long>();
    }

    public ClusterLVManager(ClusterHWManager clusterHWManager, ClusterLV clusterLV) {
        this.clusterHWManager = clusterHWManager;
        this.clusterLV = clusterLV;
        this.logicalVolumeIds = new HashSet<Long>();

        long maxLogicalVolumeId = LogicalVolume.INVALID_LOGICAL_VOLUME_ID;
        for (LogicalVolume logicalVolume : clusterLV.getLogicalVolumes()) {
            if (logicalVolumeIds.contains(logicalVolume.getLogicalVolumeId())) {
                throw new IllegalArgumentException("Logical volume ID is not unique: " + logicalVolume.getLogicalVolumeId());
            }
            if (!logicalVolume.isValid()) {
                throw new IllegalArgumentException("Logical volume state is invalid.");
            }

            logicalVolumeIds.add(logicalVolume.getLogicalVolumeId());
            if (logicalVolume.getLogicalVolumeId() > maxLogicalVolumeId) {
                maxLogicalVolumeId = logicalVolume.getLogicalVolumeId();
            }
        }
        if (maxLogicalVolumeId == LogicalVolume.INVALID_LOGICAL_VOLUME_ID) {
            this.nextLogicalVolumeId = 0;
        } else {
            this.nextLogicalVolumeId = maxLogicalVolumeId + 1;
        }

        for (ReplicatedStore replicatedStore : clusterLV.getReplicatedStores()) {
            if(!clusterHWManager.isValidDiskId(replicatedStore.getDiskId())) {
                throw new IllegalArgumentException("Disk ID  " + replicatedStore.getDiskId() + " does not exist.");
            }
            if(!logicalVolumeIds.contains(replicatedStore.getLogicalVolumeId())) {
                throw new IllegalArgumentException("Logical Volume ID  " + replicatedStore.getLogicalVolumeId() + " does not exist.");
            }
        }
    }

    public void addNewLogicalVolume(Set<Long> diskIds, long capacityGB) {
        for (Long diskId : diskIds) {
            if(!clusterHWManager.isValidDiskId(diskId)) {
                throw new IllegalArgumentException("Disk ID  " + diskId + " does not exist.");
            }
        }

        long logicalVolumeId = nextLogicalVolumeId;
        logicalVolumeIds.add(logicalVolumeId);
        nextLogicalVolumeId++;

        LogicalVolume logicalVolume = new LogicalVolume(logicalVolumeId, LogicalVolume.State.READ_WRITE, capacityGB);
        clusterLV.addLogicalVolume(logicalVolume);

        for (Long diskId : diskIds) {
            ReplicatedStore replicatedStore = new ReplicatedStore(logicalVolumeId, diskId);
            clusterLV.addReplicatedStore(replicatedStore);
        }
    }

    public void toJson(String filename) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            File clusterFile = new File(filename);

            mapper.writeValue(clusterFile, clusterLV);

        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    static public ClusterLVManager makeFromJson(String filename, ClusterHWManager clusterHWManager) {
        ObjectMapper mapper = new ObjectMapper();
        File clusterFile = new File(filename);

        ClusterLV clusterLV = null;
        try {
            clusterLV = mapper.readValue(clusterFile, ClusterLV.class);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        return new ClusterLVManager(clusterHWManager, clusterLV);
    }

    public ClusterLV getClusterLV() {
        return clusterLV;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("nextLVId : ").append(nextLogicalVolumeId).append(System.lineSeparator());

        sb.append("logical volumes : ");
        for(Long lvid : logicalVolumeIds) {
            sb.append(lvid).append(" ");
        }
        sb.append(System.lineSeparator());

        sb.append("replicated stores : ");
        for(ReplicatedStore replicatedStore : clusterLV.getReplicatedStores()) {
            sb.append(replicatedStore.getLogicalVolumeId()).append(":").append(replicatedStore.getDiskId()).append(" ");
        }
        sb.append(System.lineSeparator());
        return sb.toString();
    }

}
