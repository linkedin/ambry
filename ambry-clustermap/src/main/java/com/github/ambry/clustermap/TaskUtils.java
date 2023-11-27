package com.github.ambry.clustermap;

import com.github.ambry.utils.Pair;


public class TaskUtils {

  /**
   * Given an instance name of the form hostname_port, returns a pair of <hostname, port>.
   * Given an instance name of the form hostname, returns a pair of <hostname, defaultPort>.
   * @param instanceName Name of the instance machine
   * @param defaultPort Default port used by machines
   * @return Pair of <hostname, port>
   */
  protected static Pair<String, Integer> getHostNameAndPort(String instanceName, int defaultPort) {
    String hostname = instanceName;
    int port = defaultPort;
    int ind = instanceName.lastIndexOf("_");
    if (ind != -1) {
      try {
        port = Short.valueOf(instanceName.substring(ind + 1));
        hostname = instanceName.substring(0, ind);
      } catch (NumberFormatException e) {
        // String after "_" is not a port number, then the hostname should be the instanceName
      }
    }
    return new Pair<>(hostname, port);
  }
}
