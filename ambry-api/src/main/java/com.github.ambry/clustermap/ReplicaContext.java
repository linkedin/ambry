package com.github.ambry.clustermap;

/**
 * The ReplicaContext uniquely identifies a specific replica. The ReplicaContext also provides enough information to
 * connect to the DataNode which hosts the Replica (hostname and port), as well as enough information for a DataNode to
 * open the correct file for reading & writing.
 */
public class ReplicaContext {
  private String hostname;
  private int port;
  private String mountPath;


  public ReplicaContext(String hostname, int port, String mountPath) {
    this.hostname = hostname;
    this.port = port;
    this.mountPath = mountPath;
  }

  /**
   * Get the host for the Replica
   * @return hostname of node that hosts the Replica.
   */
  public String getHostname() {
    return hostname;
  }

  /**
   * Get the port of the host.
   * @return  port for the node that hosts the Replica.
   */
  public int getPort() {
    return port;
  }

  /**
   * Get the mount path.
   * @return mount path of directory in which replica is stored on the DataNode.
   */
  public String getMountPath() {
    return mountPath;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ReplicaContext that = (ReplicaContext) o;

    if (port != that.port) return false;
    if (!hostname.equals(that.hostname)) return false;
    if (!mountPath.equals(that.mountPath)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = hostname.hashCode();
    result = 31 * result + port;
    result = 31 * result + mountPath.hashCode();
    return result;
  }
}
