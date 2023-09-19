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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpecBuilder;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.ResourceConfig;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HelixFullAutoReconstructResourceTool {

  private final String clusterName;
  private final String helixClusterName;
  private final String cliqueLayout;
  private final String dc;
  private final boolean dryRun;
  private final HelixAdmin helixAdmin;
  private final Map<Integer, List<String>> cliqueToHosts = new HashMap<>();
  private final Map<Integer, Set<String>> cliqueToPartitions = new HashMap<>();
  private final Map<String, List<String>> preferenceLists = new HashMap<>();
  Map<String, List<String>> currentStates = new HashMap<>();
  int cliqueId = 9999;
  int port = 15088;

  public static void main(String[] args) throws Exception {
    OptionParser parser = new OptionParser();

    ArgumentAcceptingOptionSpec<String> clusterNameOpt = parser.accepts("clusterName", "Helix cluster name")
        .withRequiredArg()
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

    OptionSet options = parser.parse(args);
    String clusterName = options.valueOf(clusterNameOpt);
    String dcName = options.valueOf(dcNameOpt);
    String zkLayoutPath = options.valueOf(zkLayoutPathOpt);
    String cliqueLayoutPath = options.valueOf(cliqueLayoutPathOpt);

    HelixFullAutoReconstructResourceTool helixFullAutoReconstructResourceTool =
        new HelixFullAutoReconstructResourceTool(clusterName, dcName, options.has(dryRun), zkLayoutPath,
            cliqueLayoutPath);
    helixFullAutoReconstructResourceTool.reconstructResources();

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
    this.helixAdmin = new HelixAdminFactory().getHelixAdmin(zkConnectStr);
  }

  public void reconstructResources() {
    // 1. Construct clique to hosts map
    buildCliqueToHostsMap();

    // 2. Get partitions in each clique
    buildCliqueToPartitionsMap();

    // 3. Create resources for new cliques
    createNewResources();
  }

  public void buildCliqueToHostsMap() {
    JSONObject root = new JSONObject(cliqueLayout);
    JSONObject dcInfo = root.getJSONObject(dc);
    JSONArray clusterInfo = dcInfo.getJSONArray(clusterName);

    // 1. Get hosts in each clique
    for (int i = 0; i < clusterInfo.length(); i++) {
      JSONArray cliqueInfo = clusterInfo.getJSONArray(i);
      cliqueId++;
      cliqueToHosts.put(cliqueId, new ArrayList<>());
      for (int j = 0; j < cliqueInfo.length(); j++) {
        JSONArray subCliqueInfo = cliqueInfo.getJSONObject(j).getJSONArray("current_clique");
        for (int k = 0; k < subCliqueInfo.length(); k++) {
          JSONObject hostInfo = subCliqueInfo.getJSONObject(k);
          String host = hostInfo.getString("host");
          cliqueToHosts.get(cliqueId).add(host);
        }
      }
    }

    for (Map.Entry<Integer, List<String>> entry : cliqueToHosts.entrySet()) {
      System.out.println("Clique = " + entry.getKey() + ", Hosts = " + entry.getValue());
    }
  }

  public void buildCliqueToPartitionsMap() {

    // 1. Get partitions in each host
    List<String> resourcesInCluster = helixAdmin.getResourcesInCluster(helixClusterName);
    for (String resourceName : resourcesInCluster) {
      IdealState idealState = helixAdmin.getResourceIdealState(helixClusterName, resourceName);
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

    // 2. Build clique to partitions set
    for (Map.Entry<Integer, List<String>> entry : cliqueToHosts.entrySet()) {
      int cliqueId = entry.getKey();
      List<String> hosts = entry.getValue();
      cliqueToPartitions.put(cliqueId, new HashSet<>());
      Set<String> partitionSet = cliqueToPartitions.get(cliqueId);
      for (String host : hosts) {
        String instanceName = ClusterMapUtils.getInstanceName(host, port);
        if (currentStates.containsKey(instanceName)) {
          List<String> partitions = currentStates.get(instanceName);
          partitionSet.addAll(partitions);
        } else {
          System.err.println("Instance " + instanceName + " is not present in cluster or has empty current state");
        }
      }
    }

    for (Map.Entry<Integer, Set<String>> entry : cliqueToPartitions.entrySet()) {
      System.out.println("Clique = " + entry.getKey() + ", Partitions = " + entry.getValue());
    }
  }

  public void createNewResources() {
    // 1. For each newly formed clique, add a resource in helix cluster
    for (Map.Entry<Integer, Set<String>> entry : cliqueToPartitions.entrySet()) {
      int cliqueId = entry.getKey();
      Set<String> partitions = entry.getValue();
      String resource = String.valueOf(cliqueId);
      Map<String, List<String>> resourcePreferenceLists = new HashMap<>();
      for (String partition : partitions) {
        List<String> preferenceList = preferenceLists.get(partition);
        resourcePreferenceLists.put(partition, preferenceList);
      }
      buildAndCreateIdealState(resource, resourcePreferenceLists);
    }
  }

  /**
   * Build and create a new IdealState for the given resource name and partition layout.
   *
   * @param resourceName    The name of the newly created resource
   * @param preferenceLists The partition layout
   */
  private void buildAndCreateIdealState(String resourceName, Map<String, List<String>> preferenceLists) {
    IdealState idealState = buildIdealState(resourceName, preferenceLists);
    if (!dryRun) {
      helixAdmin.addResource(clusterName, resourceName, idealState);
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
   *
   * @param resourceName    The name of the newly created resource
   * @param preferenceLists The partition layout
   * @return The new IdealState
   */
  private IdealState buildIdealState(String resourceName, Map<String, List<String>> preferenceLists) {
    IdealState idealState = new IdealState(resourceName);
    idealState.setStateModelDefRef(ClusterMapConfig.DEFAULT_STATE_MODEL_DEF);
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
    return idealState;
  }
}
