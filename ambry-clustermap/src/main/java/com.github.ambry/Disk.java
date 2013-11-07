package com.github.ambry;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private Logger logger = LoggerFactory.getLogger(getClass());

  public Disk(DataNode dataNode, JSONObject jsonObject) throws JSONException {
    this.dataNode = dataNode;
    this.diskId = new DiskId(jsonObject.getJSONObject("diskId"));
    this.state = State.valueOf(jsonObject.getString("state"));
    this.capacityGB = jsonObject.getLong("capacityGB");

    validate();
  }

  public Disk(DataNode dataNode, String mountPath, long capacityGB) {
    this.dataNode = dataNode;
    this.diskId = new DiskId(dataNode.getDataNodeId(), mountPath);
    this.state = State.AVAILABLE;
    this.capacityGB = capacityGB;

    validate();
  }

  // Useful constructor for unit tests
  protected Disk(DataNode dataNode, DiskId diskID, long capacityGB) {
    this.dataNode = dataNode;
    this.diskId = diskID;
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
    if (dataNode == null) {
      throw new IllegalStateException("DataNode cannot be null");
    }
  }

  protected void validateCapacity() {
    if (capacityGB <= 0) {
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

  public JSONObject toJSONObject() throws JSONException {
    return new JSONObject()
            .put("diskId", diskId.toJSONObject())
            .put("state", state)
            .put("capacityGB", capacityGB);
  }

  @Override
  public String toString() {
    try {
      return toJSONObject().toString();
    } catch (JSONException e) {
      logger.warn("JSONException caught in toString:" + e.getCause());
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