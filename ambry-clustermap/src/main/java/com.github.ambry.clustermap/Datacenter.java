package com.github.ambry.clustermap;


import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * A Datacenter in an Ambry cluster. A Datacenter is uniquely identifiable by its name. A Datacenter is the primary unit
 * at which Ambry hardware is organized (see {@link HardwareLayout})). A Datacenter has zero or more {@link DataNode}s.
 */
public class Datacenter {
  private final HardwareLayout hardwareLayout;
  private String name; // E.g., "ELA4"
  private ArrayList<DataNode> dataNodes;

  private Logger logger = LoggerFactory.getLogger(getClass());

  public Datacenter(HardwareLayout hardwareLayout, JSONObject jsonObject) throws JSONException {
    this.hardwareLayout = hardwareLayout;
    this.name = jsonObject.getString("name");

    this.dataNodes = new ArrayList<DataNode>(jsonObject.getJSONArray("dataNodes").length());
    for (int i = 0; i < jsonObject.getJSONArray("dataNodes").length(); ++i) {
      this.dataNodes.add(new DataNode(this, jsonObject.getJSONArray("dataNodes").getJSONObject(i)));
    }
    validate();
  }

  public HardwareLayout getHardwareLayout() {
    return hardwareLayout;
  }

  public String getName() {
    return name;
  }

  public long getCapacityInBytes() {
    long capacityInBytes = 0;
    for (DataNode dataNode : dataNodes) {
      capacityInBytes += dataNode.getCapacityInBytes();
    }
    return capacityInBytes;
  }

  public List<DataNode> getDataNodes() {
    return dataNodes;
  }

  protected void validateHardwareLayout() {
    if (hardwareLayout == null) {
      throw new IllegalStateException("HardwareLayout cannot be null");
    }
  }

  protected void validateName() {
    if (name == null) {
      throw new IllegalStateException("Datacenter name cannot be null.");
    }
    else if (name.length() == 0) {
      throw new IllegalStateException("Datacenter name cannot be zero length.");
    }
  }

  protected void validate() {
    logger.trace("begin validate.");
    validateHardwareLayout();
    validateName();
    logger.trace("complete validate.");
  }

  public JSONObject toJSONObject() throws JSONException {
    JSONObject jsonObject = new JSONObject()
            .put("name", name)
            .put("dataNodes", new JSONArray());
    for (DataNode dataNode : dataNodes) {
      jsonObject.accumulate("dataNodes", dataNode.toJSONObject());
    }
    return jsonObject;
  }

  @Override
  public String toString() {
    return "Datacenter: " + getName();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Datacenter that = (Datacenter)o;

    return name.equals(that.name);
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }
}
