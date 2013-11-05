package com.github.ambry;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;

/**
 *
 */
public class ReplicaId {
  PartitionId partitionId;
  DiskId diskId;

  public ReplicaId(PartitionId partitionId, DiskId diskId) {
    this.partitionId = partitionId;
    this.diskId = diskId;

    validate();
  }

  public ReplicaId(JSONObject jsonObject) throws JSONException {
    this.partitionId = new PartitionId(new JSONObject(jsonObject.getString("partitionId")));
    this.diskId = new DiskId(new JSONObject(jsonObject.getString("diskId")));

    validate();
  }

  public PartitionId getPartitionId() {
    return partitionId;
  }

  public DiskId getDiskId() {
    return diskId;
  }

  public void validate() {
    partitionId.validate();
    diskId.validate();
  }

  // Returns JSON representation
  @Override
  public String toString() {
    try {
      return new JSONStringer()
              .object()
              .key("partitionId")
              .value(partitionId)
              .key("diskId")
              .value(diskId)
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

    ReplicaId replicaId = (ReplicaId) o;

    if (!diskId.equals(replicaId.diskId)) return false;
    if (!partitionId.equals(replicaId.partitionId)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = partitionId.hashCode();
    result = 31 * result + diskId.hashCode();
    return result;
  }
}
