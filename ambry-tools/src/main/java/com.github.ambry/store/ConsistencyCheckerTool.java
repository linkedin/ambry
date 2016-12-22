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
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Consistency Checker tool is used to check for consistency
 * 1) Among replicas for any given partition
 * 2) In the index file boundaries on all replicas in a partition
 */
public class ConsistencyCheckerTool {

  private static final Logger logger = LoggerFactory.getLogger(ConsistencyCheckerTool.class);
  private final ClusterMap map;
  private boolean includeAcceptableInconsistentBlobs;

  public ConsistencyCheckerTool(ClusterMap map, boolean includeAcceptableInconsistentBlobs) {
    this.map = map;
    this.includeAcceptableInconsistentBlobs = includeAcceptableInconsistentBlobs;
  }

  public static void main(String args[]) {
    try {
      OptionParser parser = new OptionParser();

      ArgumentAcceptingOptionSpec<String> hardwareLayoutOpt =
          parser.accepts("hardwareLayout", "The path of the hardware layout file")
              .withRequiredArg()
              .describedAs("hardware_layout")
              .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> partitionLayoutOpt =
          parser.accepts("partitionLayout", "The path of the partition layout file")
              .withRequiredArg()
              .describedAs("partition_layout")
              .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> rootDirectoryForPartitionOpt = parser.accepts("rootDirectoryForPartition",
          "Directory which contains all replicas for a partition which in turn will have all index files "
              + "for the respective replica, for Operation ConsistencyCheckForIndex ")
          .withRequiredArg()
          .describedAs("root_directory_partition")
          .ofType(String.class);

      ArgumentAcceptingOptionSpec<String> includeAcceptableInconsistentBlobsOpt =
          parser.accepts("includeAcceptableInconsistentBlobs", "To include acceptable inconsistent blobs")
              .withRequiredArg()
              .describedAs("Whether to output acceptable inconsistent blobs or not")
              .defaultsTo("false")
              .ofType(String.class);

      OptionSet options = parser.parse(args);

      ArrayList<OptionSpec<?>> listOpt = new ArrayList<OptionSpec<?>>();
      listOpt.add(hardwareLayoutOpt);
      listOpt.add(partitionLayoutOpt);
      listOpt.add(rootDirectoryForPartitionOpt);

      for (OptionSpec opt : listOpt) {
        if (!options.has(opt)) {
          System.err.println("Missing required argument \"" + opt + "\"");
          parser.printHelpOn(System.err);
          System.exit(1);
        }
      }

      String hardwareLayoutPath = options.valueOf(hardwareLayoutOpt);
      String partitionLayoutPath = options.valueOf(partitionLayoutOpt);
      ClusterMap map = new ClusterMapManager(hardwareLayoutPath, partitionLayoutPath,
          new ClusterMapConfig(new VerifiableProperties(new Properties())));
      String rootDirectoryForPartition = options.valueOf(rootDirectoryForPartitionOpt);
      boolean includeAcceptableInconsistentBlobs =
          Boolean.parseBoolean(options.valueOf(includeAcceptableInconsistentBlobsOpt));

      ConsistencyCheckerTool consistencyCheckerTool =
          new ConsistencyCheckerTool(map, includeAcceptableInconsistentBlobs);
      consistencyCheckerTool.checkConsistency(rootDirectoryForPartition);
    } catch (Exception e) {
      System.err.println("Consistency checker exited with Exception: " + e);
    }
  }

  private boolean checkConsistency(String directoryForConsistencyCheck) throws Exception {
    File rootDir = new File(directoryForConsistencyCheck);
    logger.info("Root directory for Partition" + rootDir);
    ArrayList<String> replicaList = populateReplicaList(rootDir);
    logger.trace("Replica List " + replicaList);
    Map<String, BlobStatus> blobIdToStatusMap = new HashMap<>();
    AtomicLong totalKeysProcessed = new AtomicLong(0);
    int replicaCount = replicaList.size();

    collectData(rootDir.listFiles(), replicaList, blobIdToStatusMap, totalKeysProcessed);
    return populateOutput(totalKeysProcessed, blobIdToStatusMap, replicaCount, includeAcceptableInconsistentBlobs);
  }

  private ArrayList<String> populateReplicaList(File rootDir) {
    ArrayList<String> replicaList = new ArrayList<String>();
    File[] replicas = rootDir.listFiles();
    for (File replicaFile : replicas) {
      replicaList.add(replicaFile.getName());
    }
    return replicaList;
  }

  private void collectData(File[] replicas, ArrayList<String> replicasList, Map<String, BlobStatus> blobIdToStatusMap,
      AtomicLong totalKeysProcessed) throws IOException, InterruptedException {
    DumpData dumpData = new DumpData(map, -1);
    IndexStats indexStats = new IndexStats();
    for (File replica : replicas) {
      try {
        File[] indexFiles = replica.listFiles(PersistentIndex.INDEX_FILE_FILTER);
        long keysProcessedforReplica = 0;
        Arrays.sort(indexFiles, PersistentIndex.INDEX_FILE_COMPARATOR);
        for (File indexFile : indexFiles) {
          keysProcessedforReplica +=
              dumpData.dumpIndex(indexFile, replica.getName(), replicasList, new ArrayList<String>(), blobIdToStatusMap,
                  indexStats, false);
        }
        logger.debug("Total keys processed for {} is {}", replica.getName(), keysProcessedforReplica);
        totalKeysProcessed.addAndGet(keysProcessedforReplica);
      } catch (Exception e) {
        logger.error("Could not complete processing for {}", replica, e);
      }
    }
    logger.debug("Total Keys Processed {}", totalKeysProcessed.get());
    logger.debug("Total Put Records {}", indexStats.getTotalPutRecords().get());
    logger.debug("Total Delete Records {}", indexStats.getTotalDeleteRecords().get());
    logger.debug("Total Duplicate Put Records {}", indexStats.getTotalDuplicatePutRecords().get());
    logger.debug("Total Delete before Put Records {}", indexStats.getTotalDeleteBeforePutRecords().get());
    logger.debug("Total Put after Delete Records {}", indexStats.getTotalPutAfterDeleteRecords().get());
    logger.debug("Total Duplicate Delete Records {}", indexStats.getTotalDuplicateDeleteRecords().get());
  }

  private boolean populateOutput(AtomicLong totalKeysProcessed, Map<String, BlobStatus> blobIdToStatusMap,
      int replicaCount, boolean includeAcceptableInconsistentBlobs) {
    logger.info("Total keys processed " + totalKeysProcessed.get());
    logger.info("\nTotal Blobs Found " + blobIdToStatusMap.size());
    long inconsistentBlobs = 0;
    long realInconsistentBlobs = 0;
    long acceptableInconsistentBlobs = 0;
    for (String blobId : blobIdToStatusMap.keySet()) {
      BlobStatus consistencyBlobResult = blobIdToStatusMap.get(blobId);
      // valid blobs : count of available replicas = total replica count or count of deleted replicas = total replica count
      // acceptable inconsistent blobs : count of deleted + count of unavailable = total replica count
      // rest are all inconsistent blobs
      boolean isValid = consistencyBlobResult.getAvailable().size() == replicaCount
          || consistencyBlobResult.getDeletedOrExpired().size() == replicaCount;
      if (!isValid) {
        inconsistentBlobs++;
        if ((consistencyBlobResult.getDeletedOrExpired().size() + consistencyBlobResult.getUnavailableList().size()
            == replicaCount)) {
          if (includeAcceptableInconsistentBlobs) {
            logger.info("Partially deleted (acceptable inconsistency) blob " + blobId + " isDeletedOrExpired "
                + consistencyBlobResult.getIsDeletedOrExpired() + "\n" + consistencyBlobResult);
          }
          acceptableInconsistentBlobs++;
        } else {
          realInconsistentBlobs++;
          logger.info(
              "Inconsistent Blob : " + blobId + " isDeletedOrExpired " + consistencyBlobResult.getIsDeletedOrExpired()
                  + "\n" + consistencyBlobResult);
        }
      }
    }
    logger.info("Total Inconsistent blobs count : " + inconsistentBlobs);
    if (includeAcceptableInconsistentBlobs) {
      logger.info("Acceptable Inconsistent blobs count : " + acceptableInconsistentBlobs);
    }
    logger.info("Real Inconsistent blobs count :" + realInconsistentBlobs);
    return realInconsistentBlobs == 0;
  }
}

