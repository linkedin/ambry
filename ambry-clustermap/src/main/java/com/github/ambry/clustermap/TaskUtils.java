/**
 * Copyright 2023 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.clustermap;

import com.github.ambry.utils.Pair;
import java.util.Map;
import java.util.Set;


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

  protected static boolean removeConfig(Set<String> config){
    if (checkIfConfigPresent(config)) {
      config.clear();
      return true;
    }
    return false;
  }

  protected static boolean removeConfig(Map<String, DataNodeConfig.ReplicaConfig> config){
    if (checkIfConfigPresent(config)) {
      config.clear();
      return true;
    }
    return false;
  }

  protected static boolean checkIfConfigPresent(Set<String> config) {
    return config != null && !config.isEmpty();
  }

  protected static boolean checkIfConfigPresent(Map<String, DataNodeConfig.ReplicaConfig> config) {
    return config != null && !config.isEmpty();
  }
}
