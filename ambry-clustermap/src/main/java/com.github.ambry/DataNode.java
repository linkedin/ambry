package com.github.ambry;

import org.codehaus.jackson.annotate.JsonIgnore;

// TODO: Consider moving all methods that mutate/set this object into a sub-class so that the core type is "read only".

/**
 * DataNode represents data nodes in Ambry. Each DataNode is uniquely identifiable by its hostname.
 */
public class DataNode {
   public enum State {
       INVALID,
       AVAILABLE,
       UNAVAILABLE
    }

    private String hostname; // E.g., ela4-app001.prod
    private String datacenterName;
    // TODO: ? ip address? No?
    // TODO: Port? No?
    private State state;

    // "simple" constructor needed for JSON SerDe.
    private DataNode() {
        this.hostname = null;
        this.datacenterName = null;
        this.state = State.INVALID;
    }

    public DataNode(String datacenterName, String hostname) {
        this.hostname = hostname;
        this.datacenterName = datacenterName;
        this.state = State.AVAILABLE;
    }

    public String getHostname() {
        return hostname;
    }

    public String getDatacenterName() {
        return datacenterName;
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
        if (hostname != null & datacenterName != null && isStateValid()) {
            return true;
        }
        return false;
    }

}