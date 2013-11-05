package com.github.ambry;

/**
 * ReplicatedStore represents one replica of a logical volume in Ambry. Each ReplicatedStore is uniquely identifiable
 * by a (logical volume id, disk id) pair. Note that this induces a constraint that a logical volume can never have
 * more than one replica on a given disk. This ensures replicas do not share a lowest-level single-point-of-failure.
 */
public class ReplicatedStore {
    // State is derived from logical volume (RW or RO) and disk/node (available/unavailable)
    /*
    public enum State {
        INVALID,
        READ_WRITE,
        READ_ONLY,
        UNAVAILABLE
    }
    */

    long logicalVolumeId;
    long diskId;

    public ReplicatedStore() {
        this.logicalVolumeId = LogicalVolume.INVALID_LOGICAL_VOLUME_ID;
        this.diskId = -1;
    }

    public ReplicatedStore(long logicalVolumeId, long diskId) {
        this.logicalVolumeId = logicalVolumeId;
        this.diskId = diskId;
    }

    public long getLogicalVolumeId() {
        return logicalVolumeId;
    }

    public long getDiskId() {
        return diskId;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("(").append(logicalVolumeId).append(":").append(diskId).append(")");
        return sb.toString();
    }
}
