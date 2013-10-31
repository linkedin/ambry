package com.github.ambry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * ClusterMap sub-type with methods for use by the Coordinator.
 */
public class CoordinatorClusterMap extends ClusterMap {
    ArrayList<LogicalVolume> readWriteLogicalVolumes;

    public CoordinatorClusterMap(ClusterHWTopology clusterHWTopology, ClusterLVTopology clusterLVTopology) {
        super(clusterHWTopology, clusterLVTopology);

        this.readWriteLogicalVolumes = new ArrayList<LogicalVolume>();
        for (LogicalVolume logicalVolume : clusterLVTopology.getLogicalVolumes()) {
            if(logicalVolume.getState() == LogicalVolume.State.READ_WRITE) {
                readWriteLogicalVolumes.add(logicalVolume);
            }
        }

    }

    public List<LogicalVolume> getReadWriteLogicalVolumes() {
        return Collections.unmodifiableList(readWriteLogicalVolumes);
    }
}
