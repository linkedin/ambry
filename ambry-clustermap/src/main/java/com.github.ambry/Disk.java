package com.github.ambry;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;

/**
 * Disk represents a disk in Ambry. Each Disk is uniquely identifiable by its diskId.
 */
public class Disk {
  public enum State {
    AVAILABLE,
    UNAVAILABLE
  }

  private DataNode dataNode;
  private DiskId diskId;
  private State state;
  private long capacityGB;

  public Disk(DataNode dataNode, JSONObject jsonObject) throws JSONException {
    this.dataNode = dataNode;
    this.diskId = new DiskId(new JSONObject(jsonObject.getString("diskId")));
    this.state = State.valueOf(jsonObject.getString("state"));
    this.capacityGB = jsonObject.getLong("replicaCapacityGB");

    validate();
  }

  public Disk(DataNode dataNode, DiskId diskId, long capacityGB) {
    this.dataNode = dataNode;
    this.diskId = diskId;
    this.state = State.AVAILABLE;
    this.capacityGB = capacityGB;

    validate();
  }

  public DataNode getDataNode() {
    return dataNode;
  }

  public DiskId getDiskId() {
    return diskId;
  }

  public State getState() {
    return state;
  }

  public long getCapacityGB() {
    return capacityGB;
  }

  protected void validateDataNode() {
    if(dataNode == null) {
      throw new IllegalStateException("DataNode cannot be null");
    }
  }

  protected void validateCapacity() {
    if (capacityGB <=0) {
      throw new IllegalStateException("Invalid disk capacity: " + capacityGB);
    }
  }

  protected boolean isStateValid() {
    for (State validState : State.values()) {
      if (state == validState) {
        return true;
      }
    }
    return false;
  }

  protected void validateState() {
    if (!isStateValid()) {
      throw new IllegalStateException("Invalid disk state: " + state);
    }
  }

  public void validate() {
    validateDataNode();
    diskId.validate();
    validateCapacity();
    validateState();
  }

  // Returns JSON representation
  @Override
  public String toString() {
    try {
      return new JSONStringer()
              .object()
                .key("diskId")
                .value(diskId)
                .key("state")
                .value(state)
              .key("replicaCapacityGB")
              .value(capacityGB)
              .endObject()
              .toString();
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    return null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Disk disk = (Disk) o;

    if (capacityGB != disk.capacityGB) return false;
    if (!diskId.equals(disk.diskId)) return false;
    if (state != disk.state) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = diskId.hashCode();
    result = 31 * result + (int) (capacityGB ^ (capacityGB >>> 32));
    result = 31 * result + state.hashCode();
    return result;
  }
}