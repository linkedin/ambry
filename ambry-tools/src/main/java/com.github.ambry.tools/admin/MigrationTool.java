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
package com.github.ambry.tools.admin;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapManager;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.coordinator.AmbryCoordinator;
import com.github.ambry.coordinator.Coordinator;
import com.github.ambry.coordinator.CoordinatorException;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.utils.Utils;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Properties;


/**
 * Tool to migration from a source to Ambry
 */
public class MigrationTool {

  public static void directoryWalk(String path, String prefix, boolean ignorePrefix, Coordinator coordinator,
      FileWriter writer)
      throws CoordinatorException, InterruptedException {

    File root = new File(path);
    File[] list = root.listFiles();

    if (list == null) {
      return;
    }

    for (File f : list) {
      if (f.isDirectory()) {
        if (ignorePrefix || f.getName().startsWith(prefix)) {
          directoryWalk(f.getAbsolutePath(), prefix, true, coordinator, writer);
        }
      } else {
        //System.out.println( "File:" + f.getAbsoluteFile() );
        BlobProperties props = new BlobProperties(f.length(), "migration");
        byte[] usermetadata = new byte[1];
        FileInputStream stream = null;
        try {
          stream = new FileInputStream(f);
          long startMs = System.currentTimeMillis();
          String id = coordinator.putBlob(props, ByteBuffer.wrap(usermetadata), stream);
          //System.out.println("Time taken to put " + (System.currentTimeMillis() - startMs));
          writer.write("blobId|" + id + "|source|" + f.getAbsolutePath() + "\n");
        } catch (CoordinatorException e) {
          System.out.println("Error from coordinator for " + f.getAbsolutePath() + " " + e);
        } catch (FileNotFoundException e) {
          System.out.println("File not found path : " + f.getAbsolutePath() + " exception : " + e);
        } catch (IOException e) {
          System.out.println("IOException when writing to migration log " + e);
        } finally {
          try {
            if (stream != null) {
              stream.close();
            }
          } catch (Exception e) {
            System.out.println("Error while closing file stream " + e);
          }
        }
      }
    }
  }

  public static void main(String args[]) {
    FileWriter writer = null;
    AmbryCoordinator coordinator = null;
    try {
      OptionParser parser = new OptionParser();
      ArgumentAcceptingOptionSpec<String> rootDirectoryOpt =
          parser.accepts("rootDirectory", "The root folder from which all the files will be migrated").withRequiredArg()
              .describedAs("root_directory").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> folderPrefixInRootOpt =
          parser.accepts("folderPrefixInRoot", "The prefix of the folders in the root path that needs to be moved")
              .withRequiredArg().describedAs("folder_prefix_in_root").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> hardwareLayoutOpt =
          parser.accepts("hardwareLayout", "The path of the hardware layout file").withRequiredArg()
              .describedAs("hardware_layout").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> partitionLayoutOpt =
          parser.accepts("partitionLayout", "The path of the partition layout file").withRequiredArg()
              .describedAs("partition_layout").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> coordinatorConfigPathOpt =
          parser.accepts("coordinatorConfigPath", "The config for the coordinator").withRequiredArg()
              .describedAs("coordinator_config_path").ofType(String.class);

      ArgumentAcceptingOptionSpec<Boolean> verboseLoggingOpt =
          parser.accepts("enableVerboseLogging", "Enables verbose logging").withOptionalArg()
              .describedAs("Enable verbose logging").ofType(Boolean.class).defaultsTo(false);

      OptionSet options = parser.parse(args);

      ArrayList<OptionSpec<?>> listOpt = new ArrayList<OptionSpec<?>>();
      listOpt.add(rootDirectoryOpt);
      listOpt.add(folderPrefixInRootOpt);
      listOpt.add(hardwareLayoutOpt);
      listOpt.add(partitionLayoutOpt);
      listOpt.add(coordinatorConfigPathOpt);

      for (OptionSpec opt : listOpt) {
        if (!options.has(opt)) {
          System.err.println("Missing required argument \"" + opt + "\"");
          parser.printHelpOn(System.err);
          System.exit(1);
        }
      }

      String rootDirectory = options.valueOf(rootDirectoryOpt);
      String folderPrefixInRoot = options.valueOf(folderPrefixInRootOpt);
      String hardwareLayoutPath = options.valueOf(hardwareLayoutOpt);
      String partitionLayoutPath = options.valueOf(partitionLayoutOpt);
      String coordinatorConfigPath = options.valueOf(coordinatorConfigPathOpt);
      Properties props = Utils.loadProps(coordinatorConfigPath);
      VerifiableProperties vprops = new VerifiableProperties((props));
      ClusterMap map = new ClusterMapManager(hardwareLayoutPath, partitionLayoutPath, new ClusterMapConfig(vprops));
      File logFile = new File(System.getProperty("user.dir"), "migrationlog");
      writer = new FileWriter(logFile);
      boolean enableVerboseLogging = options.has(verboseLoggingOpt) ? true : false;
      if (enableVerboseLogging) {
        System.out.println("Enabled verbose logging");
      }
      coordinator = new AmbryCoordinator(vprops, map);
      while (true) {
        directoryWalk(rootDirectory, folderPrefixInRoot, false, coordinator, writer);
      }
    } catch (Exception e) {
      System.err.println("Error on exit " + e);
    } finally {
      if (writer != null) {
        try {
          writer.close();
        } catch (Exception e) {
          System.out.println("Error when closing the writer");
        }
      }
      if (coordinator != null) {
        coordinator.close();
      }
    }
  }
}
