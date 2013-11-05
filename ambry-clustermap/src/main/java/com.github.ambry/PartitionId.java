package com.github.ambry;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;

// TODO: Make an ID class and then extend DiskID and PartitionID from it?
/**
 *
 */
public class PartitionId {
  private long id;

  public PartitionId(JSONObject jsonObject) throws JSONException {
    this.id = jsonObject.getLong("id");
    validate();
  }

  public PartitionId(long id) {
    this.id = id;
    validate();
  }

  protected void validate() {
    if (id<0) {
      throw new IllegalStateException("Invalid PartitionId with id:" + id);
    }
  }

  public static PartitionId getFirstPartitionId() {
    return new PartitionId(0);
  }

  public static PartitionId getNewPartitionId(PartitionId lastPartitionId) {
    return new PartitionId(lastPartitionId.id + 1);
  }

  // Returns JSON representation
  @Override
  public String toString() {
    try {
      return new JSONStringer()
              .object()
              .key("id")
              .value(id)
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

    PartitionId partitionId = (PartitionId) o;

    if (id != partitionId.id) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return (int) (id ^ (id >>> 32));
  }
}
