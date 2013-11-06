package com.github.ambry;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;

import java.util.*;


/**
 * Cluster represents a set of datacenters in Ambry. An Ambry instance consists of exactly one Cluster.
 */
public class Cluster {
  private String name; // E.g., "Alpha"
  // TODO: Add version number and/or timestamp and/or username of last writer for cluster

  // TODO: Cleaner way of using factory for DiskIds in a cluster?
  private DiskId prevDiskId;

  private ArrayList<Datacenter> datacenters;

  // Maps used to lookup Datacenter/DataNode/Disk by identifier; Maps also used to ensure uniqueness of identifiers.
  private Map<String, Datacenter> datacenterMap;
  private Map<String, DataNode> dataNodeMap;
  private Map<DiskId, Disk> diskMap;

  public Cluster(JSONObject jsonObject) throws JSONException {
    this.name = jsonObject.getString("name");
    this.prevDiskId = null;
    if(!jsonObject.isNull("prevDiskId")) {
      this.prevDiskId = new DiskId(new JSONObject(jsonObject.getString("prevDiskId")));
    }

    this.datacenters = new ArrayList<Datacenter>();
    JSONArray datacenterJSONArray = jsonObject.getJSONArray("datacenters");
    for (int i = 0; i < datacenterJSONArray.length(); ++i) {
      this.datacenters.add(new Datacenter(this, new JSONObject(datacenterJSONArray.getString(i))));
    }

    buildMaps();
    validate();
  }

  public Cluster(String name) {
    this.name = name;
    this.prevDiskId = null;
    this.datacenters = new ArrayList<Datacenter>();

    buildMaps();
    validate();
  }

  // Allocate, populate, and validate maps.
  private void buildMaps() {
    this.datacenterMap = new HashMap<String , Datacenter>();
    this.dataNodeMap = new HashMap<String, DataNode>();
    this.diskMap = new HashMap<DiskId, Disk>();

    for (Datacenter datacenter : datacenters) {
      if(datacenterMap.put(datacenter.getName(), datacenter) != null) {
        throw new IllegalStateException("Datacenter name must be unique: " + datacenter.getName());
      }

      for (DataNode dataNode : datacenter.getDataNodes()) {
        if (dataNodeMap.put(dataNode.getHostname(), dataNode) != null) {
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
    for(Datacenter datacenter : datacenters) {
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
    if (prevDiskId != null) {
      prevDiskId.validate();
    }
    for (Datacenter datacenter: datacenters) {
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

  public DataNode addNewDataNode(String datacenterName, String hostname) {
    if (dataNodeMap.containsKey(hostname)) {
      throw new IllegalArgumentException("DataNode hostname already in use. Must be unique. " + hostname);
    }
    if (!datacenterMap.containsKey(datacenterName)) {
      throw new IllegalArgumentException("Datacenter name does not exist for new DataNode: " + datacenterName);
    }

    Datacenter datacenter = datacenterMap.get(datacenterName);
    DataNode dataNode = new DataNode(datacenter, hostname);
    datacenter.addDataNode(dataNode);
    dataNodeMap.put(hostname, dataNode);
    return dataNode;
  }

  public Disk addNewDisk(String hostname, long capacityGB) {
    DiskId diskId = getNewDiskId();
    if (diskMap.containsKey(diskId)) {
      throw new IllegalArgumentException("Disk Id already in use. Must be unique. " + diskId);
    }
    if (!dataNodeMap.containsKey(hostname)) {
      throw new IllegalArgumentException("DataNode hostname  name does not exist for new Disk: " + hostname);
    }

    DataNode dataNode =  dataNodeMap.get(hostname);
    Disk disk = new Disk(dataNode, diskId, capacityGB);
    dataNode.addDisk(disk);
    diskMap.put(diskId, disk);
    return disk;
  }

  protected DiskId getNewDiskId() {
    if(prevDiskId == null) {
      prevDiskId = DiskId.getFirstDiskId();
      return prevDiskId;
    } else {
      prevDiskId = DiskId.getNewDiskId(prevDiskId);
      return prevDiskId;
    }
  }

  public Disk getDisk(DiskId diskId) {
    return diskMap.get(diskId);
  }

  public DataNode getDataNode(String hostname) {
    return dataNodeMap.get(hostname);
  }

  public Datacenter getDatacenter(String name) {
    return datacenterMap.get(name);
  }


  // Returns JSON representation
  @Override
  public String toString() {
    try {
      return new JSONStringer()
              .object()
              .key("name")
              .value(name)
              .key("prevDiskId")
              .value(prevDiskId)
              .key("datacenters")
              .value(datacenters)
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

    Cluster cluster = (Cluster) o;

    if (!datacenters.equals(cluster.datacenters)) return false;
    if (!name.equals(cluster.name)) return false;
    if (prevDiskId != null ? !prevDiskId.equals(cluster.prevDiskId) : cluster.prevDiskId != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + (prevDiskId != null ? prevDiskId.hashCode() : 0);
    result = 31 * result + datacenters.hashCode();
    return result;
  }
}
