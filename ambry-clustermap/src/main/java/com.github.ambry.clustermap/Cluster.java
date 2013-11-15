package com.github.ambry.clustermap;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


/**
 * Cluster represents a set of datacenters in Ambry. An Ambry instance consists of exactly one Cluster.
 */
public class Cluster {
  private String name;
  private ArrayList<Datacenter> datacenters;

  // Maps used to lookup Datacenter/DataNode/Disk by identifier; Maps also used to ensure uniqueness of identifiers.
  private Map<String, Datacenter> datacenterMap;
  private Map<DataNodeId, DataNode> dataNodeMap;
  private Map<DiskId, Disk> diskMap;

  private Logger logger = LoggerFactory.getLogger(getClass());

  public Cluster(JSONObject jsonObject) throws JSONException {
    this.name = jsonObject.getString("name");
    this.datacenters = new ArrayList<Datacenter>();
    for (int i = 0; i < jsonObject.getJSONArray("datacenters").length(); ++i) {
      this.datacenters.add(new Datacenter(this, jsonObject.getJSONArray("datacenters").getJSONObject(i)));
    }

    buildMaps();
    validate();
  }

  public Cluster(String name) {
    this.name = name;
    this.datacenters = new ArrayList<Datacenter>();

    buildMaps();
    validate();
  }

  // Allocate, populate, and validate maps.
  private void buildMaps() {
    this.datacenterMap = new HashMap<String, Datacenter>();
    this.dataNodeMap = new HashMap<DataNodeId, DataNode>();
    this.diskMap = new HashMap<DiskId, Disk>();

    for (Datacenter datacenter : datacenters) {
      if (datacenterMap.put(datacenter.getName(), datacenter) != null) {
        throw new IllegalStateException("Datacenter name must be unique: " + datacenter.getName());
      }

      for (DataNode dataNode : datacenter.getDataNodes()) {
        if (dataNodeMap.put(dataNode.getDataNodeId(), dataNode) != null) {
          throw new IllegalStateException("DataNode hostname must be unique: " + dataNode.getHostname());
        }

        for (Disk disk : dataNode.getDisks()) {
          if (diskMap.put(disk.getDiskId(), disk) != null) {
            throw new IllegalStateException("Disk IDs must be unique: " + disk.getDiskId());
          }
        }
      }
    }
  }

  public String getName() {
    return name;
  }

  public long getCapacityGB() {
    long capacityGB = 0;
    for (Datacenter datacenter : datacenters) {
      capacityGB += datacenter.getCapacityGB();
    }
    return capacityGB;
  }


  protected void validateName() {
    if (name == null) {
      throw new IllegalStateException("Cluster name cannot be null.");
    }
  }

  public void validate() {
    validateName();
    for (Datacenter datacenter : datacenters) {
      datacenter.validate();
    }
  }

  public void addDatacenter(Datacenter datacenter) {
    datacenters.add(datacenter);
  }

  public List<Datacenter> getDatacenters() {
    return Collections.unmodifiableList(datacenters);
  }

  public Datacenter addNewDataCenter(String datacenterName) {
    if (datacenterMap.containsKey(datacenterName)) {
      throw new IllegalArgumentException("Datacenter name already in use. Must be unique. " + datacenterName);
    }

    Datacenter datacenter = new Datacenter(this, datacenterName);
    addDatacenter(datacenter);
    datacenterMap.put(datacenter.getName(), datacenter);
    return datacenter;
  }

  public DataNode addNewDataNode(String datacenterName, String hostname, int port) {
    DataNodeId dataNodeId = new DataNodeId(hostname, port);

    if (dataNodeMap.containsKey(dataNodeId)) {
      throw new IllegalArgumentException("DataNode hostname already in use. Must be unique. " + hostname);
    }
    if (!datacenterMap.containsKey(datacenterName)) {
      throw new IllegalArgumentException("Datacenter name does not exist for new DataNode: " + datacenterName);
    }

    Datacenter datacenter = datacenterMap.get(datacenterName);
    DataNode dataNode = new DataNode(datacenter, hostname, port);
    datacenter.addDataNode(dataNode);

    dataNodeMap.put(dataNode.getDataNodeId(), dataNode);
    return dataNode;
  }

  public Disk addNewDisk(String hostname, int port, String mountPath, long capacityGB) {
    DataNodeId dataNodeId = new DataNodeId(hostname, port);
    if (!dataNodeMap.containsKey(dataNodeId)) {
      throw new IllegalArgumentException("DataNode hostname  name does not exist for new Disk: " + hostname);
    }
    DataNode dataNode = dataNodeMap.get(dataNodeId);

    Disk disk = new Disk(dataNode, mountPath, capacityGB);
    if (diskMap.containsKey(disk.getDiskId())) {
      throw new IllegalArgumentException("Disk Id already in use. Must be unique: " + disk.getDiskId());
    }
    diskMap.put(disk.getDiskId(), disk);

    dataNode.addDisk(disk);
    return disk;
  }

  public Disk getDisk(DiskId diskId) {
    return diskMap.get(diskId);
  }

  public DataNode getDataNode(DataNodeId dataNodeId) {
    return dataNodeMap.get(dataNodeId);
  }

  public Datacenter getDatacenter(String name) {
    return datacenterMap.get(name);
  }


  public JSONObject toJSONObject() throws JSONException {
    JSONObject jsonObject = new JSONObject()
            .put("name", name)
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
      logger.warn("JSONException caught in toString:" + e.getCause());
    }
    return null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Cluster cluster = (Cluster) o;

    if (!datacenters.equals(cluster.datacenters)) return false;
    if (!name.equals(cluster.name)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + datacenters.hashCode();
    return result;
  }
}
