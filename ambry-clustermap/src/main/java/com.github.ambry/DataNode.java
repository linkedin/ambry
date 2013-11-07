package com.github.ambry;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * DataNode represents data nodes in Ambry. Each DataNode is uniquely identifiable by its hostname.
 */
public class DataNode {
  public enum State {
    AVAILABLE,
    UNAVAILABLE
  }

  private Datacenter datacenter;
  private DataNodeId dataNodeId;
  private State state;
  private ArrayList<Disk> disks;

  private Logger logger = LoggerFactory.getLogger(getClass());

  public DataNode(Datacenter datacenter, String hostname, int port) {
    this.datacenter = datacenter;
    this.dataNodeId = new DataNodeId(hostname, port);
    this.state = State.AVAILABLE;
    this.disks = new ArrayList<Disk>();
    validate();
  }

  public DataNode(Datacenter datacenter, JSONObject jsonObject) throws JSONException {
    this.datacenter = datacenter;
    this.dataNodeId = new DataNodeId(jsonObject.getJSONObject("dataNodeId"));
    this.state = State.valueOf(jsonObject.getString("state"));
    this.disks = new ArrayList<Disk>();
    JSONArray diskJSONArray = jsonObject.getJSONArray("disks");
    for (int i = 0; i < diskJSONArray.length(); ++i) {
      this.disks.add(new Disk(this, diskJSONArray.getJSONObject(i)));
    }
    validate();
  }

  public DataNodeId getDataNodeId() {
    return dataNodeId;
  }

  public String getHostname() {
    return dataNodeId.getHostname();
  }

  public int getPort() {
    return dataNodeId.getPort();
  }

  public Datacenter getDatacenter() {
    return datacenter;
  }

  public State getState() {
    return state;
  }

  public long getCapacityGB() {
    long capacityGB = 0;
    for (Disk disk : disks) {
      capacityGB += disk.getCapacityGB();
    }
    return capacityGB;
  }

  public void addDisk(Disk disk) {
    disks.add(disk);
  }

  public List<Disk> getDisks() {
    return Collections.unmodifiableList(disks);
  }

  protected void validateDatacenter() {
    if (datacenter == null) {
      throw new IllegalStateException("Datacenter cannot be null");
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
      throw new IllegalStateException("Invalid DataNode state: " + state);
    }
  }

  public void validate() {
    validateDatacenter();
    dataNodeId.validate();
    validateState();
    for (Disk disk : disks) {
      disk.validate();
    }
  }

  public JSONObject toJSONObject() throws JSONException {
    JSONObject jsonObject = new JSONObject()
            .put("dataNodeId", dataNodeId.toJSONObject())
            .put("state", state)
            .put("disks", new JSONArray());
    for (Disk disk : disks) {
      jsonObject.accumulate("disks", disk.toJSONObject());
    }
    return jsonObject;
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

    DataNode dataNode = (DataNode) o;

    if (!dataNodeId.equals(dataNode.dataNodeId)) return false;
    if (!disks.equals(dataNode.disks)) return false;
    if (state != dataNode.state) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = dataNodeId.hashCode();
    result = 31 * result + state.hashCode();
    result = 31 * result + disks.hashCode();
    return result;
  }
}