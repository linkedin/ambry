package com.github.ambry;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ReplicaId {
  PartitionId partitionId;
  DiskId diskId;

  private Logger logger = LoggerFactory.getLogger(getClass());

  public ReplicaId(PartitionId partitionId, DiskId diskId) {
    this.partitionId = partitionId;
    this.diskId = diskId;

    validate();
  }

  public ReplicaId(JSONObject jsonObject) throws JSONException {
    this.partitionId = new PartitionId(jsonObject.getJSONObject("partitionId"));
    this.diskId = new DiskId(jsonObject.getJSONObject("diskId"));

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

  public JSONObject toJSONObject() throws JSONException {
    return new JSONObject()
            .put("partitionId", partitionId.toJSONObject())
            .put("diskId", diskId.toJSONObject());
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
