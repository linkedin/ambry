package com.github.ambry.clustermap;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 *
 */
public class DataNodeId {
  private String hostname;
  private int port;

  private Logger logger = LoggerFactory.getLogger(getClass());

  public DataNodeId(String hostname, int port) {
    try {
      this.hostname = InetAddress.getByName(hostname).getCanonicalHostName();
    } catch (UnknownHostException e) {
      throw new IllegalArgumentException("Specified hostname cannot be resolved into a fully qualified hostname: "
              + hostname);
    }

    this.port = port;

    validate();
  }

  public DataNodeId(JSONObject jsonObject) throws JSONException {
    this.hostname = jsonObject.getString("hostname");
    this.port = jsonObject.getInt("port");

    validate();
  }

  protected void validateHostname() {
    String fqdn;
    try {
      fqdn = InetAddress.getByName(hostname).getCanonicalHostName();
    } catch (UnknownHostException e) {
      throw new IllegalStateException("Hostname for DataNodeId is not a fully qualified domain name: " + this.hostname);
    }

    if (!fqdn.equals(hostname)) {
      throw new IllegalStateException("Hostname for DataNodeId does not match its lookup for fully qualified domain " +
              "name: " + this.hostname + " != " + fqdn);
    }
  }

  public void validate() {
    validateHostname();
    // No port validation necessary
  }

  // Returns fully qualified domain name
  public String getHostname() {
    return hostname;
  }

  public int getPort() {
    return port;
  }

  public JSONObject toJSONObject() throws JSONException {
    return new JSONObject()
            .put("hostname", hostname)
            .put("port", port);
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

    DataNodeId that = (DataNodeId) o;

    if (port != that.port) return false;
    if (!hostname.equals(that.hostname)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = hostname.hashCode();
    result = 31 * result + port;
    return result;
  }
}
