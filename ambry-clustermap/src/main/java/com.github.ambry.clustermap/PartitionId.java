package com.github.ambry.clustermap;

import com.github.ambry.clustermap.PartitionIdentifier;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 *
 */
public class PartitionId implements PartitionIdentifier {
  private long id;

  private Logger logger = LoggerFactory.getLogger(getClass());

  public PartitionId(JSONObject jsonObject) throws JSONException {
    this.id = jsonObject.getLong("id");
    validate();
  }

  protected PartitionId(long id) {
    this.id = id;
    validate();
  }

  protected void validate() {
    if (id < 0) {
      throw new IllegalStateException("Invalid PartitionId with id:" + id);
    }

  }

  public static PartitionId getFirstPartitionId() {
    return new PartitionId(0);
  }

  public static PartitionId getNewPartitionId(PartitionId lastPartitionId) {
    return new PartitionId(lastPartitionId.id + 1);
  }

  public JSONObject toJSONObject() throws JSONException {
    return new JSONObject()
            .put("id", id);
  }

  public static PartitionId make(byte[] bytes) {
    return new PartitionId(ByteBuffer.wrap(bytes).getLong());
  }

  @Override
  public byte[] getBytes() {
    ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.putLong(id);
    return buffer.array();
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

    PartitionId partitionId = (PartitionId) o;

    if (id != partitionId.id) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return (int) (id ^ (id >>> 32));
  }
}
