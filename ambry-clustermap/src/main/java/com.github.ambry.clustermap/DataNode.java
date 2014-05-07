package com.github.ambry.clustermap;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * DataNode is uniquely identified by its hostname and port. A DataNode is in a {@link Datacenter}. A DataNode has zero
 * or more {@link Disk}s.
 */
public class DataNode implements DataNodeId {
  private static final int MinPort = 1025;
  private static final int MaxPort = 65535;

  private final Datacenter datacenter;
  private final String hostname;
  private final int port;
  private final HardwareState hardState;
  private HardwareState softState;
  private final ArrayList<Disk> disks;
  private final long rawCapacityInBytes;

  private final Logger logger = LoggerFactory.getLogger(getClass());

  public DataNode(Datacenter datacenter, JSONObject jsonObject) throws JSONException {
    if (logger.isTraceEnabled())
      logger.trace("DataNode " + jsonObject.toString());
    this.datacenter = datacenter;

    this.hostname = getFullyQualifiedDomainName(jsonObject.getString("hostname"));
    this.port = jsonObject.getInt("port");
    this.hardState = HardwareState.valueOf(jsonObject.getString("hardwareState"));
    this.softState = hardState;
    JSONArray diskJSONArray = jsonObject.getJSONArray("disks");
    this.disks = new ArrayList<Disk>(diskJSONArray.length());
    for (int i = 0; i < diskJSONArray.length(); ++i) {
      this.disks.add(new Disk(this, diskJSONArray.getJSONObject(i)));
    }
    this.rawCapacityInBytes = calculateRawCapacityInBytes();

    validate();
  }

  /**
   * Converts a hostname into a canonical hostname.
   *
   * @param unqualifiedHostname hostname to be fully qualified
   * @return canonical hostname that can be compared with DataNode.getHostname()
   */
  public static String getFullyQualifiedDomainName(String unqualifiedHostname) {
    if (unqualifiedHostname == null) {
      throw new IllegalStateException("Hostname cannot be null.");
    }
    else if (unqualifiedHostname.length() == 0) {
      throw new IllegalStateException("Hostname cannot be zero length.");
    }

    try {
      return InetAddress.getByName(unqualifiedHostname).getCanonicalHostName().toLowerCase();
    }
    catch (UnknownHostException e) {
      throw new IllegalStateException("Host (" + unqualifiedHostname
                                      + ") is unknown so cannot determine fully qualified domain name.");
    }
  }

  @Override
  public String getHostname() {
    return hostname;
  }

  @Override
  public int getPort() {
    return port;
  }

  @Override
  public HardwareState getState() {
    if (hardState == HardwareState.UNAVAILABLE) {
      return HardwareState.UNAVAILABLE;
    }
    return softState;
  }

  public boolean isSoftDown() {
    return (hardState == HardwareState.AVAILABLE && softState == HardwareState.UNAVAILABLE);
  }

  public void setSoftState(HardwareState hardwareState) {
    if (hardState == HardwareState.AVAILABLE) {
      softState = hardwareState;
    }
    else {
      logger.warn("Tried to set soft state " + this.toString() + " when hard state is not " + HardwareState.AVAILABLE);
    }
  }

  @Override
  public String getDatacenterName() {
    return getDatacenter().getName();
  }

  public Datacenter getDatacenter() {
    return datacenter;
  }

  public long getRawCapacityInBytes() {
    return rawCapacityInBytes;
  }

  private long calculateRawCapacityInBytes() {
    long capacityInBytes = 0;
    for (Disk disk : disks) {
      capacityInBytes += disk.getRawCapacityInBytes();
    }
    return capacityInBytes;
  }

  public List<Disk> getDisks() {
    return disks;
  }

  protected void validateDatacenter() {
    if (datacenter == null) {
      throw new IllegalStateException("Datacenter cannot be null.");
    }
  }

  protected void validateHostname() {
    String fqdn = getFullyQualifiedDomainName(hostname);
    if (!fqdn.equals(hostname)) {
      throw new IllegalStateException("Hostname for DataNode (" + hostname
                                      + ") does not match its fully qualified domain name: " + fqdn + ".");
    }
  }

  protected void validatePort() {
    if (port < MinPort) {
      throw new IllegalStateException("Invalid port: " + port + " is less than " + MinPort);
    }
    else if (port > MaxPort) {
      throw new IllegalStateException("Invalid port: " + port + " is less than " + MaxPort);
    }
  }

  protected void validate() {
    logger.trace("begin validate.");
    validateDatacenter();
    validateHostname();
    validatePort();
    for (Disk disk : disks) {
      disk.validate();
    }
    logger.trace("complete validate.");
  }

  public JSONObject toJSONObject() throws JSONException {
    JSONObject jsonObject = new JSONObject()
            .put("hostname", hostname)
            .put("port", port)
            .put("hardwareState", hardState)
            .put("disks", new JSONArray());
    for (Disk disk : disks) {
      jsonObject.accumulate("disks", disk.toJSONObject());
    }
    return jsonObject;
  }

  @Override
  public String toString() {
    return "DataNode[" + getHostname() + ":" + getPort() + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    DataNode dataNode = (DataNode)o;

    if (port != dataNode.port) return false;
    return hostname.equals(dataNode.hostname);
  }

  @Override
  public int hashCode() {
    int result = hostname.hashCode();
    result = 31 * result + port;
    return result;
  }
}