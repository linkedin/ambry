package com.github.ambry;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Datacenter represents a datacenter in Ambry. Each Datacenter is uniquely identifiable by its name.
 */
public class Datacenter {
  private Cluster cluster;
  private String name; // E.g., "ELA4"
  private ArrayList<DataNode> dataNodes;

  public Datacenter(Cluster cluster, JSONObject jsonObject) throws JSONException {
    this.cluster = cluster;
    this.name = jsonObject.getString("name");

    this.dataNodes = new ArrayList<DataNode>();
    JSONArray dataNodeJSONArray = jsonObject.getJSONArray("dataNodes");
    for (int i = 0; i < dataNodeJSONArray.length(); ++i) {
      this.dataNodes.add(new DataNode(this, new JSONObject(dataNodeJSONArray.getString(i))));
    }
    validate();
  }

  public Datacenter(Cluster cluster, String name) {
    this.cluster = cluster;
    this.name = name;
    this.dataNodes = new ArrayList<DataNode>();
    validate();
  }

  public Cluster getCluster() {
    return cluster;
  }

  public String getName() {
    return name;
  }

  public long getCapacityGB() {
    long capacityGB = 0;
    for(DataNode dataNode : dataNodes) {
      capacityGB += dataNode.getCapacityGB();
    }
    return capacityGB;
  }


  protected void validateCluster() {
    if(cluster == null) {
      throw new IllegalStateException("Cluster cannot be null");
    }
  }

  protected void validateName() {
    if (name == null) {
      throw new IllegalStateException("Datacenter name cannot be null.");
    }
  }

  public void validate() {
    validateCluster();
    validateName();
    for (DataNode dataNode : dataNodes) {
      dataNode.validate();
    }
  }

  public void addDataNode(DataNode dataNode) {
    dataNodes.add(dataNode);
  }

  public List<DataNode> getDataNodes() {
    return Collections.unmodifiableList(dataNodes);
  }

  // Returns JSON representation
  @Override
  public String toString() {
    try {
      return new JSONStringer()
              .object()
              .key("name")
              .value(name)
              .key("dataNodes")
              .value(dataNodes)
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

    Datacenter that = (Datacenter) o;

    if (!dataNodes.equals(that.dataNodes)) return false;
    if (!name.equals(that.name)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + dataNodes.hashCode();
    return result;
  }
}
