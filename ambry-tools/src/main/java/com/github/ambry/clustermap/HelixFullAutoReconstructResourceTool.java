/*
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

import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpecBuilder;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.ResourceConfig;
import org.json.JSONArray;
import org.json.JSONObject;


public class HelixFullAutoReconstructResourceTool {

  private final String clusterName;
  private final String helixClusterName;
  private final String cliqueLayout;
  private final String dc;
  private final boolean dryRun;
  private final HelixAdmin admin;
  private final Map<Integer, List<String>> resourceToHosts = new HashMap<>();
  private final Map<Integer, Set<String>> resourceToPartitions = new TreeMap<>();
  private final Map<String, List<String>> preferenceLists = new HashMap<>();
  Map<String, List<String>> currentStates = new HashMap<>();
  int resourceId = 9999;
  int port = 15088;

  public static void main(String[] args) throws Exception {
    OptionParser parser = new OptionParser();

    ArgumentAcceptingOptionSpec<String> clusterNameOpt =
        parser.accepts("clusterName", "Helix cluster name").withRequiredArg()
        .describedAs("cluster_name")
        .ofType(String.class);

    ArgumentAcceptingOptionSpec<String> dcNameOpt = parser.accepts("dc", "Data center name")
        .withRequiredArg()
        .describedAs("datacenter")
        .required()
        .ofType(String.class);

    OptionSpecBuilder dryRun = parser.accepts("dryRun", "Dry Run mode");

    ArgumentAcceptingOptionSpec<String> zkLayoutPathOpt =
        parser.accepts("zkLayoutPath", "The path to the json file containing zookeeper connect info")
            .withRequiredArg()
            .describedAs("zk_connect_info_path")
            .ofType(String.class);

    ArgumentAcceptingOptionSpec<String> cliqueLayoutPathOpt =
        parser.accepts("cliqueLayoutPath", "The path to the json file containing cliques info")
            .withRequiredArg()
            .describedAs("clique_info_path")
            .ofType(String.class);

    ArgumentAcceptingOptionSpec<String> resourcesOpt = parser.accepts("resources",
            "The comma-separated resources to create. Use '--resources all' to migrate all resources")
        .withRequiredArg()
        .describedAs("resources")
        .ofType(String.class);

    OptionSet options = parser.parse(args);
    String clusterName = options.valueOf(clusterNameOpt);
    String dcName = options.valueOf(dcNameOpt);
    String zkLayoutPath = options.valueOf(zkLayoutPathOpt);
    String cliqueLayoutPath = options.valueOf(cliqueLayoutPathOpt);
    String resources = options.valueOf(resourcesOpt) == null ? "all" : options.valueOf(resourcesOpt);

    HelixFullAutoReconstructResourceTool helixFullAutoReconstructResourceTool =
        new HelixFullAutoReconstructResourceTool(clusterName, dcName, options.has(dryRun), zkLayoutPath,
            cliqueLayoutPath);
    helixFullAutoReconstructResourceTool.reconstructResources(resources);

    System.out.println("======== HelixFullAutoReconstructResourceTool completed successfully! ========");
    System.out.println("( If program doesn't exit, please use Ctrl-c to terminate. )");
    System.exit(0);
  }

  public HelixFullAutoReconstructResourceTool(String clusterName, String dc, boolean dryRun, String zkLayoutPath,
      String cliqueLayoutPath) throws IOException {
    this.clusterName = clusterName;
    this.dc = dc;
    this.dryRun = dryRun;
    this.helixClusterName = "Ambry-" + clusterName;
    this.cliqueLayout = Utils.readStringFromFile(cliqueLayoutPath);
    Map<String, ClusterMapUtils.DcZkInfo> dataCenterToZkAddress =
        ClusterMapUtils.parseDcJsonAndPopulateDcInfo(Utils.readStringFromFile(zkLayoutPath));
    String zkConnectStr = dataCenterToZkAddress.get(dc).getZkConnectStrs().get(0);
    this.admin = new HelixAdminFactory().getHelixAdmin(zkConnectStr);
  }

  /**
   * @param commaSeparatedResources comma separated list
   */
  public void reconstructResources(String commaSeparatedResources) {
    // 1. Construct clique to hosts map
    buildResourceToHostsMap();

    // 2. Get partitions in each clique
    buildResourceToPartitionsMap();

    // 3. Create resources for new cliques
    Set<String> resources = new HashSet<>();
    if (!commaSeparatedResources.equals("all")) {
      resources =
          Arrays.stream(commaSeparatedResources.replaceAll("\\p{Space}", "").split(",")).collect(Collectors.toSet());
    }
    Set<String> resourcesInHelix = new HashSet<>(admin.getResourcesInCluster(helixClusterName));
    List<String> invalidResources = new ArrayList<>();
    for (String resource : resources) {
      if (resourcesInHelix.contains(resource)) {
        invalidResources.add(resource);
      }
    }
    // Verify input resources to add are not present in helix already
    if (!invalidResources.isEmpty()) {
      throw new IllegalArgumentException("Helix cluster already contains resources " + invalidResources);
    }

    createNewResources(resources);
  }

  /**
   * Build resource to hosts map
   * Resource 1 -> [host1, host2, host3... host6]
   * Resource 2 -> [host7, host8, host9... host12]
   */
  public void buildResourceToHostsMap() {
    JSONObject root = new JSONObject(cliqueLayout);
    JSONObject dcInfo = root.getJSONObject(dc);
    JSONArray clusterInfo = dcInfo.getJSONArray(clusterName);

    Map<String, List<Integer>> hostToResources = new HashMap<>();

    // 1. Get hosts in each clique
    for (int i = 0; i < clusterInfo.length(); i++) {
      JSONArray cliqueInfo = clusterInfo.getJSONArray(i);
      resourceId++;
      resourceToHosts.put(resourceId, new ArrayList<>());
      if (cliqueInfo.length() > 2) {
        throw new IllegalStateException("Resource " + cliqueInfo + " has more than 2 cliques");
      }
      for (int j = 0; j < cliqueInfo.length(); j++) {
        JSONArray subCliqueInfo = cliqueInfo.getJSONObject(j).getJSONArray("current_clique");
        for (int k = 0; k < subCliqueInfo.length(); k++) {
          JSONObject hostInfo = subCliqueInfo.getJSONObject(k);
          String host = hostInfo.getString("host");
          resourceToHosts.get(resourceId).add(host);
          if (!hostToResources.containsKey(host)) {
            hostToResources.put(host, new ArrayList<>());
          }
          hostToResources.get(host).add(resourceId);
        }
      }
    }

    // Validate host is present only in 1 resource such as host1 -> [resource 1], host2 -> [resource 2]..
    for (Map.Entry<String, List<Integer>> entry : hostToResources.entrySet()) {
      String host = entry.getKey();
      List<Integer> resources = entry.getValue();
      if (resources.size() != 1) {
        throw new IllegalStateException(
            "Input is invalid. Host " + host + "is present in more than 1 resource " + resources);
      }
    }

    for (Map.Entry<Integer, List<String>> entry : resourceToHosts.entrySet()) {
      Integer resourceId = entry.getKey();
      List<String> hosts = entry.getValue();
      System.out.println("Resource = " + resourceId + " has " + hosts.size() + " hosts. Hosts are " + hosts);
    }
  }

  /**
   * Build resource to partitions map
   * Resource 1 -> [P1, P2,... P100]
   */
  public void buildResourceToPartitionsMap() {

    // 1. Build host to partitions map
    // Host1 -> [P1, P2... P13]
    List<String> resourcesInCluster = admin.getResourcesInCluster(helixClusterName);
    for (String resourceName : resourcesInCluster) {
      IdealState idealState = admin.getResourceIdealState(helixClusterName, resourceName);
      Map<String, List<String>> preferenceLists = idealState.getPreferenceLists();
      this.preferenceLists.putAll(preferenceLists);
      for (Map.Entry<String, List<String>> entry : preferenceLists.entrySet()) {
        String partitionName = entry.getKey();
        List<String> instances = entry.getValue();
        for (String instanceName : instances) {
          if (!currentStates.containsKey(instanceName)) {
            currentStates.put(instanceName, new ArrayList<>());
          }
          currentStates.get(instanceName).add(partitionName);
        }
      }
    }

    // 2. Build resource to partitions map by going via resources -> hosts -> partitions
    // Resource 10000 -> [P1, P2.. P100]
    Map<String, Set<Integer>> partitionToResources = new HashMap<>();
    for (Map.Entry<Integer, List<String>> entry : resourceToHosts.entrySet()) {
      int resourceId = entry.getKey();
      List<String> hosts = entry.getValue();
      resourceToPartitions.put(resourceId, new HashSet<>());
      Set<String> partitionSet = resourceToPartitions.get(resourceId);
      for (String host : hosts) {
        String instanceName = ClusterMapUtils.getInstanceName(host, port);
        if (currentStates.containsKey(instanceName)) {
          List<String> partitions = currentStates.get(instanceName);
          partitionSet.addAll(partitions);
          for (String partition : partitions) {
            if (!partitionToResources.containsKey(partition)) {
              partitionToResources.put(partition, new HashSet<>());
            }
            partitionToResources.get(partition).add(resourceId);
          }
        } else {
          throw new IllegalStateException(
              "Instance \" + instanceName + \" is not present in cluster or has empty current state");
        }
        currentStates.remove(instanceName);
      }
    }

    // Verify number of hosts in layout == number of hosts in helix cluster
    if (!currentStates.isEmpty()) {
      throw new IllegalStateException(
          "There are additional hosts in helix cluster which are not present in input layout file. Hosts = "
              + currentStates.keySet());
    }

    // Verify partition is present in only one resource. P1 -> [Resource 10000], P2 -> [Resource 10000]
    for (Map.Entry<String, Set<Integer>> entry : partitionToResources.entrySet()) {
      String partition = entry.getKey();
      Set<Integer> resources = entry.getValue();
      if (resources.size() != 1) {
        List<String> preferenceList = preferenceLists.get(partition);
        throw new IllegalStateException(
            "Partition " + partition + " is present in more than 1 resource " + resources + ". Its preference list is "
                + preferenceList);
      }
    }

    for (Map.Entry<Integer, Set<String>> entry : resourceToPartitions.entrySet()) {
      System.out.println(
          "Resource = " + entry.getKey() + ", number of partitions = " + entry.getValue().size() + ", Partitions = "
              + entry.getValue());
    }
  }

  /**
   * Create new resources (10000, 10001, 10002... ) in helix
   */
  public void createNewResources(Set<String> resources) {
    for (Map.Entry<Integer, Set<String>> entry : resourceToPartitions.entrySet()) {
      int resourceId = entry.getKey();
      Set<String> partitions = entry.getValue();
      String resource = String.valueOf(resourceId);
      if (resources.isEmpty() || resources.contains(resource)) {
        Map<String, List<String>> resourcePreferenceLists = new HashMap<>();
        for (String partition : partitions) {
          List<String> preferenceList = preferenceLists.get(partition);
          resourcePreferenceLists.put(partition, preferenceList);
        }
        buildAndCreateIdealState(resource, resourcePreferenceLists);
      }
    }
  }

  /**
   * Build and create a new IdealState for the given resource name and partition layout.
   * @param resourceName    The name of the newly created resource
   * @param preferenceLists The partition layout
   */
  private void buildAndCreateIdealState(String resourceName, Map<String, List<String>> preferenceLists) {
    IdealState idealState = buildIdealState(resourceName, preferenceLists);
    if (!dryRun) {
      admin.addResource(clusterName, resourceName, idealState);
      System.out.println(
          "Added " + preferenceLists.size() + " new partitions under resource " + resourceName + " in dc " + dc);
    } else {
      System.out.println(
          "Under DryRun mode, " + preferenceLists.size() + " new partitions are added to resource " + resourceName
              + " in dc " + dc);
    }
  }

  /**
   * Build a new IdealState for the given resource name and partition layout
   * @param resourceName    The name of the newly created resource
   * @param preferenceLists The partition layout
   * @return The new IdealState
   */
  private IdealState buildIdealState(String resourceName, Map<String, List<String>> preferenceLists) {
    IdealState idealState = new IdealState(resourceName);
    idealState.setStateModelDefRef(ClusterMapConfig.DEFAULT_STATE_MODEL_DEF);
    idealState.setRebalanceMode(IdealState.RebalanceMode.SEMI_AUTO);
    for (Map.Entry<String, List<String>> entry : preferenceLists.entrySet()) {
      String partitionName = entry.getKey();
      ArrayList<String> instances = new ArrayList<>(entry.getValue());
      Collections.shuffle(instances);
      idealState.setPreferenceList(partitionName, entry.getValue());
    }
    idealState.setNumPartitions(idealState.getPartitionSet().size());
    idealState.setReplicas(ResourceConfig.ResourceConfigConstants.ANY_LIVEINSTANCE.name());
    if (!idealState.isValid()) {
      throw new IllegalStateException("IdealState could not be validated for new resource " + resourceName);
    }
    System.out.println(
        "Building ideal state for resource " + resourceName + ". State model = " + idealState.getStateModelDefRef()
            + ", Rebalance mode = " + idealState.getRebalanceMode() + ", number of partitions = "
            + idealState.getNumPartitions() + ", replicas = " + idealState.getReplicas() + ", preference list = "
            + idealState.getPreferenceLists());
    return idealState;
  }
}
