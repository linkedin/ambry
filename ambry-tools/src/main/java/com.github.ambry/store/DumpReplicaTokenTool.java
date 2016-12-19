/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.store;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapManager;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.util.Properties;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Tool to assist in dumping data from replica token file in ambry
 */
public class DumpReplicaTokenTool {

  private DumpDataHelper dumpDataHelper;
  private static final Logger logger = LoggerFactory.getLogger(DumpDataTool.class);

  public DumpReplicaTokenTool(ClusterMap map) {
    dumpDataHelper = new DumpDataHelper(map);
  }

  public static void main(String args[]) {
    try {

      OptionParser parser = new OptionParser();
      ArgumentAcceptingOptionSpec<String> propsFileOpt = parser.accepts("propsFile", "Properties file path")
          .withRequiredArg()
          .describedAs("propsFile")
          .ofType(String.class);

      OptionSet options = parser.parse(args);
      String propsFilePath = options.valueOf(propsFileOpt);
      Properties properties = Utils.loadProps(propsFilePath);
      VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
      DumpReplicaTokenConfig config = new DumpReplicaTokenConfig(verifiableProperties);

      if (!new File(config.hardwareLayoutFilePath).exists() || !new File(config.partitionLayoutFilePath).exists()) {
        throw new IllegalArgumentException("Hardware or Partition Layout file does not exist");
      }
      ClusterMap map = new ClusterMapManager(config.hardwareLayoutFilePath, config.partitionLayoutFilePath,
          new ClusterMapConfig(new VerifiableProperties(new Properties())));
      DumpReplicaTokenTool dumpReplicaTokenTool = new DumpReplicaTokenTool(map);

      if (config.fileToRead != null) {
        logger.info("File to read " + config.fileToRead);
      }

      dumpReplicaTokenTool.dumpDataHelper.dumpReplicaToken(new File(config.fileToRead));
    } catch (Exception e) {
      logger.error("Closed with exception ", e);
    }
  }
}
