package com.github.ambry;

import java.util.ArrayList;

/**
 * ClusterLV is the lowest level representation of the logical components (logical volumes and replicated stores) that
 * comprise an Ambry instance.
 */
public class ClusterLV {
    // TODO: Add version number here.
    private ArrayList<LogicalVolume> logicalVolumes;
    private ArrayList<ReplicatedStore> replicatedStores;

    // "simple" constructor needed for JSON SerDe.
    public ClusterLV() {
        this.logicalVolumes = new ArrayList<LogicalVolume>();
        this.replicatedStores = new ArrayList<ReplicatedStore>();
    }

    public ArrayList<LogicalVolume> getLogicalVolumes() {
        return logicalVolumes;
    }

    public ArrayList<ReplicatedStore> getReplicatedStores() {
        return replicatedStores;
    }

    public void addLogicalVolume(LogicalVolume logicalVolume) {
        logicalVolumes.add(logicalVolume);
    }

    public void addReplicatedStore(ReplicatedStore replicatedStore) {
        replicatedStores.add(replicatedStore);
    }

}
