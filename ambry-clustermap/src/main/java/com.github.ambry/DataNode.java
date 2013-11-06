package com.github.ambry;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;

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
  private String hostname; // E.g., ela4-app001.prod
  private State state;
  private ArrayList<Disk> disks;

  public DataNode(Datacenter datacenter, String hostname) {
    this.datacenter = datacenter;
    this.hostname = hostname;
    this.state = State.AVAILABLE;
    this.disks = new ArrayList<Disk>();
    validate();
  }

  public DataNode(Datacenter datacenter, JSONObject jsonObject) throws JSONException {
    this.datacenter = datacenter;
    this.hostname = jsonObject.getString("hostname");
    this.state = State.valueOf(jsonObject.getString("state"));
    this.disks = new ArrayList<Disk>();
    JSONArray diskJSONArray = jsonObject.getJSONArray("disks");
    for (int i = 0; i < diskJSONArray.length(); ++i) {
      this.disks.add(new Disk(this, new JSONObject(diskJSONArray.getString(i))));
    }
    validate();
  }

  public String getHostname() {
    return hostname;
  }

  public Datacenter getDatacenter() {
    return datacenter;
  }

  public State getState() {
    return state;
  }

  public long getCapacityGB() {
    long capacityGB = 0;
    for(Disk disk : disks) {
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
    if(datacenter == null) {
      throw new IllegalStateException("Datacenter cannot be null");
    }
  }

  protected void validateHostname() {
    // Convert to fully qualified hostname?
    if(hostname == null) {
      throw new IllegalStateException("Hostname cannot be null");
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
    validateHostname();
    validateState();
    for(Disk disk : disks) {
      disk.validate();
    }
  }

  // Returns JSON representation
  @Override
  public String toString() {
    try {
      return new JSONStringer()
              .object()
              .key("hostname")
              .value(hostname)
              .key("state")
              .value(state)
              .key("disks")
              .value(disks)
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

    DataNode dataNode = (DataNode) o;

    if (!disks.equals(dataNode.disks)) return false;
    if (!hostname.equals(dataNode.hostname)) return false;
    if (state != dataNode.state) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = hostname.hashCode();
    result = 31 * result + state.hashCode();
    result = 31 * result + disks.hashCode();
    return result;
  }


}