package com.github.ambry;

import org.codehaus.jackson.annotate.JsonIgnore;

/*
 * TODO: Different name than LogicalVolume?
 *  - Partition
 *  - Bucket
 *  - Volume
 *  - Segment
 *  - Store
 *  - BlobXXX Where XXX is one of the above
 *  - LogicalXXX where XXX is one of the above
 *  - ReplicatedXXX where XXX is one of the above
 *
 *  Call "ReplicatedStore" "BlobStore" and then call 'LogicalVolume' "ReplicatedStore" or LogicalStore.
 */

/**
 * LogicalVolume represents a logical volume (set of replicated stores) in Ambry. Each LogicalVolume is uniquely
 * identifiable by its logical volume id.
 */
public class LogicalVolume {
    public static final long INVALID_LOGICAL_VOLUME_ID = -1;

    public enum State {
        INVALID,
        READ_WRITE,
        READ_ONLY
    }

    // TODO: long -> LogicalVolumeID?
    private long logicalVolumeId;
    // TODO: private ReplicationPolicy replicationPolicy
    private State state;
    private long capacityGB;


    public LogicalVolume() {
        this.logicalVolumeId = INVALID_LOGICAL_VOLUME_ID;
        this.state = State.INVALID;
        this.capacityGB = 0;
    }

    public LogicalVolume(long logicalVolumeId, State state, long capacityGB) {
        this.logicalVolumeId = logicalVolumeId;
        this.state = state;
        this.capacityGB = capacityGB;
    }

    protected boolean isStateValid() {
        if (state == State.INVALID) {
            return false;
        }
        for (State validState : State.values()) {
            if (state == validState) {
                return true;
            }
        }
        return false;
    }

    @JsonIgnore
    public boolean isValid() {
        if (logicalVolumeId >= 0 && isStateValid()) {
            return true;
        }
        return false;
    }

    public long getLogicalVolumeId() {
        return logicalVolumeId;
    }

    public State getState() {
        return state;
    }

    public long getCapacityGB() {
        return capacityGB;
    }

}
