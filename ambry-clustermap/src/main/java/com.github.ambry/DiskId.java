package com.github.ambry;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;

/**
 *
 */
public class DiskId {
  private long id;

  public DiskId(JSONObject jsonObject) throws JSONException {
    this.id = jsonObject.getLong("id");
    validate();
  }

  public DiskId(long id) {
    this.id = id;
    validate();
  }

  protected void validate() {
    if (id<0) {
      throw new IllegalStateException("Invalid DiskId with id:" + id);
    }
  }

  public static DiskId getFirstDiskId() {
    return new DiskId(0);
  }

  public static DiskId getNewDiskId(DiskId lastDiskId) {
    return new DiskId(lastDiskId.id+1);
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

    DiskId diskId = (DiskId) o;

    if (id != diskId.id) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return (int) (id ^ (id >>> 32));
  }
}
