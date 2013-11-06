package com.github.ambry;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;

/**
 *
 */
public class DiskId {
  private DataNodeId dataNodeId;
  private String mountPath;

  public DiskId(JSONObject jsonObject) throws JSONException {
    this.dataNodeId = new DataNodeId(new JSONObject(jsonObject.getString("dataNodeId")));
    this.mountPath = jsonObject.getString("mountPath");

    validate();
  }

  public DiskId(DataNodeId dataNodeId, String mountPath) {
    this.dataNodeId = dataNodeId;
    this.mountPath = mountPath;

    validate();
  }

  protected void validateMountPath() {
    if (mountPath == null) {
      throw new IllegalStateException("Mount path cannot be a null string.");
    }

    // TODO: Verify that mount path actually exists on specified host? Or should that be done by Disk? Or, more likely,
    // is that a separate verification method explicitly invoked via admin code path?
  }

  protected void validate() {
    dataNodeId.validate();
    validateMountPath();
  }

  // Returns JSON representation
  @Override
  public String toString() {
    try {
      return new JSONStringer()
              .object()
              .key("dataNodeId")
              .value(dataNodeId)
              .key("mountPath")
              .value(mountPath)
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

    if (!dataNodeId.equals(diskId.dataNodeId)) return false;
    if (!mountPath.equals(diskId.mountPath)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = dataNodeId.hashCode();
    result = 31 * result + mountPath.hashCode();
    return result;
  }
}
