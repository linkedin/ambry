package com.github.ambry;

import java.util.ArrayList;
import java.util.List;

/**
 * ClusterMap sub-type with methods for use by the DataNode.
 */
public class DataNodeClusterMap extends ClusterMap {
    public DataNodeClusterMap(ClusterHWTopology clusterHWTopology, ClusterLVTopology clusterLVTopology) {
        super(clusterHWTopology, clusterLVTopology);
    }

    // Get peer replicated stores
    public List<ReplicatedStore> getPeerReplicatedStores(ReplicatedStore replicatedStore) {
        ArrayList<ReplicatedStore> replicatedStores = new ArrayList<ReplicatedStore>();

        LogicalVolume logicalVolume = clusterLVTopology.getLogicalVolume(replicatedStore.getLogicalVolumeId());
        for (ReplicatedStore lvReplicatedStore : clusterLVTopology.getReplicatedStores(logicalVolume)) {
            if(!lvReplicatedStore.equals(replicatedStore)) {
                replicatedStores.add(lvReplicatedStore);
            }
        }

        return replicatedStores;
    }
}
