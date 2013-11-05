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
  private long capacityGB;
  private State state;

  public Disk(DataNode dataNode, JSONObject jsonObject) throws JSONException {
    this.dataNode = dataNode;
    this.diskId = new DiskId(new JSONObject(jsonObject.getString("diskId")));
    this.capacityGB = jsonObject.getLong("capacityGB");
    this.state = State.valueOf(jsonObject.getString("state"));
    validate();
  }

  public Disk(DataNode dataNode, DiskId diskId, long capacityGB) {
    this.dataNode = dataNode;
    this.diskId = diskId;
    this.capacityGB = capacityGB;
    this.state = State.AVAILABLE;
    validate();
  }

  public DataNode getDataNode() {
    return dataNode;
  }

  public DiskId getDiskId() {
    return diskId;
  }

  public long getCapacityGB() {
    return capacityGB;
  }

  public State getState() {
    return state;
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
                .key("capacityGB")
                .value(capacityGB)
                .key("state")
                .value(state)
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