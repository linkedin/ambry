package com.github.ambry.clustermap;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;


/**
 * An Ambry hardwareLayout consists of a set of {@link Datacenter}s.
 */
public class HardwareLayout {
  private String clusterName;
  private ArrayList<Datacenter> datacenters;

  private Logger logger = LoggerFactory.getLogger(getClass());

  public HardwareLayout(JSONObject jsonObject) throws JSONException {
    this.clusterName = jsonObject.getString("clusterName");

    this.datacenters = new ArrayList<Datacenter>(jsonObject.getJSONArray("datacenters").length());
    for (int i = 0; i < jsonObject.getJSONArray("datacenters").length(); ++i) {
      this.datacenters.add(i, new Datacenter(this, jsonObject.getJSONArray("datacenters").getJSONObject(i)));
    }

    validate();
  }

  public String getClusterName() {
    return clusterName;
  }

  public List<Datacenter> getDatacenters() {
    return Collections.unmodifiableList(datacenters);
  }

  public long getCapacityGB() {
    long capacityGB = 0;
    for (Datacenter datacenter : datacenters) {
      capacityGB += datacenter.getCapacityGB();
    }
    return capacityGB;
  }

  /**
   * Find DataNode by hostname and port
   * @param hostname
   * @param port
   * @return DataNode or null if not found.
   */
  public DataNode findDataNode(String hostname, int port) {
    for (Datacenter datacenter : datacenters) {
      for (DataNode dataNode : datacenter.getDataNodes()) {
        if (dataNode.getHostname().equals(hostname) && dataNode.getPort() == port) {
          return dataNode;
        }
      }
    }
    return null;
  }

  /**
   * Find Disk by hostname, port, and mount path.
   * @param hostname
   * @param port
   * @param mountPath
   * @return Disk or null if not found.
   */
  public Disk findDisk(String hostname, int port, String mountPath) {
    DataNode dataNode = findDataNode(hostname, port);
    if (dataNode != null) {
      for(Disk disk : dataNode.getDisks()) {
        if (disk.getMountPath().equals(mountPath)) {
          return disk;
        }
      }
    }
    return null;
  }

  protected void validateClusterName() {
    if (clusterName == null) {
      throw new IllegalStateException("HardwareLayout clusterName cannot be null.");
    } else if (clusterName.length() == 0) {
      throw new IllegalStateException("HardwareLayout clusterName cannot be zero length.");
    }
  }

  // Validate each hardware component (Datacenter, DataNode, and Disk) are unique
  protected void validateUniqueness() throws IllegalStateException {
    logger.trace("begin validateUniqueness.");
    HashSet<Datacenter> datacenterSet= new HashSet<Datacenter>();
    HashSet<DataNode> dataNodeSet = new HashSet<DataNode>();
    HashSet<Disk> diskSet = new HashSet<Disk>();

    for (Datacenter datacenter : datacenters) {
      if (!datacenterSet.add(datacenter)) {
        throw new IllegalStateException("Duplicate Datacenter detected: " + datacenter.toString());
      }
      for (DataNode dataNode : datacenter.getDataNodes()) {
        if (!dataNodeSet.add(dataNode)) {
          throw new IllegalStateException("Duplicate DataNode detected: " + dataNode.toString());
        }
        for (Disk disk : dataNode.getDisks()) {
          if (!diskSet.add(disk)) {
            throw new IllegalStateException("Duplicate Disk detected: " + disk.toString());
          }
        }
      }
    }
    logger.trace("complete validateUniqueness.");
  }

  protected void validate() {
    logger.trace("begin validate.");
    validateClusterName();
    validateUniqueness();
    logger.trace("complete validate.");
  }

  public JSONObject toJSONObject() throws JSONException {
    JSONObject jsonObject = new JSONObject()
            .put("clusterName", clusterName)
            .put("datacenters", new JSONArray());
    for (Datacenter datacenter : datacenters) {
      jsonObject.accumulate("datacenters", datacenter.toJSONObject());
    }
    return jsonObject;
  }

  @Override
  public String toString() {
    try {
      return toJSONObject().toString();
    } catch (JSONException e) {
      logger.error("JSONException caught in toString: {}",  e.getCause());
    }
    return null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    HardwareLayout that = (HardwareLayout) o;

    if (!clusterName.equals(that.clusterName)) return false;
    if (!datacenters.equals(that.datacenters)) return false;

    return true;
  }
}
