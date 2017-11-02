/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.tools.util.ToolUtils;
import java.util.ArrayList;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;


/**
 * This tool takes the hardware layout, partition layout and the Zk hosts information json files as input,
 * and updates the ZK hosts with the contents of the layout files. It adds all partitions and hosts that have not
 * previously been added (so, initially this will bootstrap the cluster information and on an ongoing basis, this can
 * add new nodes and partitions).
 *
 * The existing hardware and partition layout json files will be read in as is.
 *
 * The ZkLayoutPath argument containing the Zk hosts information in each datacenter should be a json of the
 * following example form:
 *
 * {
 *   "zkInfo" : [
 *     {
 *       "datacenter":"dc1",
 *       "id": "1",
 *       "zkConnectStr":"abc.example.com:2199",
 *     },
 *     {
 *       "datacenter":"dc2",
 *       "id" : "2",
 *       "zkConnectStr":"def.example.com:2300",
 *     }
 *   ]
 * }
 *
 * This tool should be run from an admin node that has access to the nodes in the hardware layout. The access is
 * required because the static {@link StaticClusterManager} that we use to parse the static layout files validates
 * these nodes.
 *
 * The tool does the following:
 * 1. Bootstraps a static cluster map by adding nodes and partitions to Helix.
 * 2. Upgrades the information with changes in the static clustermap that involve new nodes and new partitions.
 *    To avoid over-complicating things, it assumes that the existing partition assignment does not change during an
 *    upgrade. Newly added partitions can be distributed in any way (new partitions can have replicas even in
 *    previously added nodes).
 * 3. Upgrades will also update the partition states if required (READ_WRITE to SEALED or vice versa) for existing
 *    partitions.
 *
 */
public class HelixBootstrapUpgradeTool {
  static final int DEFAULT_MAX_PARTITIONS_PER_RESOURCE = 100;

  /**
   * @param args takes in three mandatory arguments: the hardware layout path, the partition layout path and the zk
   *             layout path.
   *             The Zk layout has to be of the following form:
   *             {
   *               "zkInfo" : [
   *                 {
   *                   "datacenter":"dc1",
   *                   "id": "1",
   *                   "zkConnectStr":"abc.example.com:2199",
   *                 },
   *                 {
   *                   "datacenter":"dc2",
   *                    "id": "2",
   *                   "zkConnectStr":"def.example.com:2300",
   *                 }
   *               ]
   *             }
   *
   *             Also takes in an optional argument that specifies the local datacenter name, so that can be used as
   *             the "reference" datacenter. If none provided, the tool simply chooses one of the datacenters in the
   *             layout as the reference datacenter.
   */
  public static void main(String args[]) throws Exception {
    OptionParser parser = new OptionParser();

    ArgumentAcceptingOptionSpec<String> hardwareLayoutPathOpt =
        parser.accepts("hardwareLayoutPath", "The path to the hardware layout json file")
            .withRequiredArg()
            .describedAs("hardware_layout_path")
            .ofType(String.class);

    ArgumentAcceptingOptionSpec<String> partitionLayoutPathOpt =
        parser.accepts("partitionLayoutPath", "The path to the partition layout json file")
            .withRequiredArg()
            .describedAs("partition_layout_path")
            .ofType(String.class);

    ArgumentAcceptingOptionSpec<String> zkLayoutPathOpt = parser.accepts("zkLayoutPath",
        "The path to the json file containing zookeeper connect info. This should be of the following form: \n{\n"
            + "  \"zkInfo\" : [\n" + "     {\n" + "       \"datacenter\":\"dc1\",\n"
            + "       \"zkConnectStr\":\"abc.example.com:2199\",\n" + "     },\n" + "     {\n"
            + "       \"datacenter\":\"dc2\",\n" + "       \"zkConnectStr\":\"def.example.com:2300\",\n" + "     },\n"
            + "     {\n" + "       \"datacenter\":\"dc3\",\n" + "       \"zkConnectStr\":\"ghi.example.com:2400\",\n"
            + "     }\n" + "  ]\n" + "}").
        withRequiredArg().
        describedAs("zk_connect_info_path").
        ofType(String.class);

    ArgumentAcceptingOptionSpec<String> clusterNamePrefixOpt =
        parser.accepts("clusterNamePrefix", "The prefix for the cluster in Helix to bootstrap or upgrade")
            .withRequiredArg()
            .describedAs("cluster_name_prefix")
            .ofType(String.class);

    ArgumentAcceptingOptionSpec<String> localDcOpt =
        parser.accepts("localDc", "(Optional argument) The local datacenter name")
            .withRequiredArg()
            .describedAs("local_dc")
            .ofType(String.class);

    ArgumentAcceptingOptionSpec<String> maxPartitionsInOneResourceOpt = parser.accepts("maxPartitionsInOneResource",
        "(Optional argument) The maximum number of partitions that should be grouped under a Helix resource")
        .withRequiredArg()
        .describedAs("max_partitions_in_one_resource")
        .ofType(String.class);

    OptionSet options = parser.parse(args);
    String hardwareLayoutPath = options.valueOf(hardwareLayoutPathOpt);
    String partitionLayoutPath = options.valueOf(partitionLayoutPathOpt);
    String zkLayoutPath = options.valueOf(zkLayoutPathOpt);
    String clusterNamePrefix = options.valueOf(clusterNamePrefixOpt);
    ArrayList<OptionSpec> listOpt = new ArrayList<>();
    listOpt.add(hardwareLayoutPathOpt);
    listOpt.add(partitionLayoutPathOpt);
    listOpt.add(zkLayoutPathOpt);
    listOpt.add(clusterNamePrefixOpt);
    ToolUtils.ensureOrExit(listOpt, options, parser);
    HelixBootstrapUpgradeUtil.bootstrapOrUpgrade(hardwareLayoutPath, partitionLayoutPath, zkLayoutPath,
        clusterNamePrefix, options.valueOf(localDcOpt),
        options.valueOf(maxPartitionsInOneResourceOpt) == null ? DEFAULT_MAX_PARTITIONS_PER_RESOURCE
            : Integer.valueOf(options.valueOf(maxPartitionsInOneResourceOpt)), new HelixAdminFactory());
  }
}

