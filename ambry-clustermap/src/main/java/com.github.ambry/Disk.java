package com.github.ambry;

import org.codehaus.jackson.annotate.JsonIgnore;

/**
 * Disk represents a disk in Ambry. Each Disk is uniquely identifiable by its diskId.
 */
public class Disk {
    public static final long INVALID_DISK_ID = -1;

    public enum State {
        INVALID,
        AVAILABLE,
        UNAVAILABLE
    }

    private long diskId;
    private String dataNodeName;
    // TODO: Is there anything like TimeUnit for capacity?
    private long capacityGB;
    private State state;

    // "simple" constructor needed for JSON SerDe.
    private Disk() {
        this.diskId = Disk.INVALID_DISK_ID;
        this.dataNodeName = null;
        this.capacityGB = 0;
        this.state = State.INVALID;
    }

    public Disk(long diskId, String dataNodeName, long capacityGB) {
        this.diskId = diskId;
        this.dataNodeName = dataNodeName;
        this.capacityGB = capacityGB;
        this.state = State.AVAILABLE;
    }

    public long getDiskId() {
        return diskId;
    }

    public String getDataNodeName() {
        return dataNodeName;
    }

    public long getCapacityGB() {
        return capacityGB;
    }

    public State getState() {
        return state;
    }

    public void setUnavailable() {
        state = State.UNAVAILABLE;
    }

    public void setAvailable() {
        state = State.AVAILABLE;
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
        if (diskId >= 0 && dataNodeName != null && capacityGB > 0 && isStateValid()) {
            return true;
        }
        return false;
    }
}