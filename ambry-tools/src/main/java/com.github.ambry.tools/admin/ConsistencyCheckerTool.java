/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
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
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.util.ArrayList;
import java.io.File;


/**
 * Consistency Checker tool is used to check for consistency
 * 1) Among replicas for any given partition
 * 2) In the index file boundaries on all replicas in a partition
 */
public class ConsistencyCheckerTool {

  public static String SUBJECT_BLOBS = "blobs";
  public static String SUBJECT_INDEX = "index";

  //For additional checks
  public static int DELETE_RECORD_SIZE = 97;

  ClusterMap map;
  FileWriter fileWriter;
  String outFile;

  public ConsistencyCheckerTool(ClusterMap map) {
    this.map = map;
  }

  public static void main(String args[]) {
    try {
      OptionParser parser = new OptionParser();

      ArgumentAcceptingOptionSpec<String> hardwareLayoutOpt =
          parser.accepts("hardwareLayout", "The path of the hardware layout file").withRequiredArg()
              .describedAs("hardware_layout").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> partitionLayoutOpt =
          parser.accepts("partitionLayout", "The path of the partition layout file").withRequiredArg()
              .describedAs("partition_layout").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> rootDirectoryForPartitionOpt = parser.accepts("rootDirectoryForPartition",
          "Directory which contains all replicas for a partition which in turn will have all index files "
              + "for the respective replica, for Operation ConsistencyCheckForIndex ").withRequiredArg()
          .describedAs("root_directory_partition").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> outFileOpt =
          parser.accepts("outFile", "Output file for \"ConsistencyCheckForIndex\" or \"ConsistencyCheckForLog\" ")
              .withRequiredArg().describedAs("outFile").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> includeAcceptableInconsistentBlobsOpt =
          parser.accepts("includeAcceptableInconsistentBlobs", "To include acceptable inconsistent blobs")
              .withRequiredArg().describedAs("Whether to output acceptable inconsistent blobs or not")
              .defaultsTo("false").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> subjectOpt =
          parser.accepts("subject", "The subject of the check (" + SUBJECT_BLOBS + ", " + SUBJECT_INDEX + " etc)")
              .withRequiredArg()
              .describedAs("To select the subject of the check (" + SUBJECT_BLOBS + ", " + SUBJECT_INDEX + " etc)")
              .defaultsTo(SUBJECT_BLOBS).ofType(String.class);

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
      String outFile = options.valueOf(outFileOpt);
      boolean includeAcceptableInconsistentBlobs =
          Boolean.parseBoolean(options.valueOf(includeAcceptableInconsistentBlobsOpt));
      String subject = options.valueOf(subjectOpt);

      ConsistencyCheckerTool consistencyCheckerTool = null;
      if (subject.equals(SUBJECT_INDEX)) {
        consistencyCheckerTool = new IndexConsistencyCheckerTool(map);
      } else {
        consistencyCheckerTool = new BlobConsistencyCheckerTool(map, includeAcceptableInconsistentBlobs);
      }

      consistencyCheckerTool.setupOutputFile(outFile);
      consistencyCheckerTool.checkConsistency(rootDirectoryForPartition);
      consistencyCheckerTool.shutdown();
    } catch (Exception e) {
      System.err.println("Consistency checker exited with Exception: " + e);
    }
  }

  public void setupOutputFile(String outFile) {
    try {
      if (outFile != null) {
        this.outFile = outFile;
        fileWriter = new FileWriter(new File(outFile));
      }
    } catch (IOException IOException) {
      System.out.println("IOException while trying to create File " + this.outFile);
    }
  }

  public boolean checkConsistency(String directoryForConsistencyCheck)
      throws Exception {
    return false;
  }

  public void shutdown() {
    try {
      if (outFile != null) {
        fileWriter.flush();
        fileWriter.close();
      }
    } catch (IOException IOException) {
      System.out.println("IOException while trying to close File " + outFile);
    }
  }

  protected void logOutput(String msg) {
    try {
      if (outFile == null) {
        System.out.println(msg);
      } else {
        fileWriter.write(msg + "\n");
      }
    } catch (IOException e) {
      System.out.println("IOException while trying to write to File");
    }
  }
}

class BlobConsistencyCheckerTool extends ConsistencyCheckerTool {

  private boolean includeAcceptableInconsistentBlobs;

  public BlobConsistencyCheckerTool(ClusterMap map, boolean includeAcceptableInconsistentBlobs) {
    super(map);
    this.includeAcceptableInconsistentBlobs = includeAcceptableInconsistentBlobs;
  }

  @Override
  public boolean checkConsistency(String directoryForConsistencyCheck)
      throws Exception {
    File rootDir = new File(directoryForConsistencyCheck);
    logOutput("Root directory for Partition" + rootDir);
    ArrayList<String> replicaList = populateReplicaList(rootDir);
    logOutput("Replica List " + replicaList);
    ConcurrentHashMap<String, BlobStatus> blobIdToStatusMap = new ConcurrentHashMap<String, BlobStatus>();
    AtomicLong totalKeysProcessed = new AtomicLong(0);
    int replicaCount = replicaList.size();

    doCheck(rootDir.listFiles(), replicaList, blobIdToStatusMap, totalKeysProcessed);
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

  private void doCheck(File[] replicas, ArrayList<String> replicasList,
      ConcurrentHashMap<String, BlobStatus> blobIdToStatusMap, AtomicLong totalKeysProcessed)
      throws IOException, InterruptedException {
    DumpData dumpData = new DumpData(outFile, fileWriter, map);
    CountDownLatch countDownLatch = new CountDownLatch(replicas.length);
    IndexStats indexStats = new IndexStats();
    for (File replica : replicas) {
      Thread thread = new Thread(
          new ReplicaProcessorForBlobs(replica, replicasList, blobIdToStatusMap, totalKeysProcessed, dumpData,
              countDownLatch, indexStats));
      thread.start();
      thread.join();
    }
    countDownLatch.await();
    logOutput("Total Keys Processed " + totalKeysProcessed.get());
    logOutput("Total Put Records " + indexStats.getTotalPutRecords().get());
    logOutput("Total Delete Records " + indexStats.getTotalDeleteRecords().get());
    logOutput("Total Duplicate Put Records " + indexStats.getTotalDuplicatePutRecords().get());
    logOutput("Total Delete before Put Records " + indexStats.getTotalDeleteBeforePutRecords().get());
    logOutput("Total Put after Delete Records " + indexStats.getTotalPutAfterDeleteRecords().get());
    logOutput("Total Duplicate Delete Records " + indexStats.getTotalDuplicateDeleteRecords().get());
  }

  private boolean populateOutput(AtomicLong totalKeysProcessed, ConcurrentHashMap<String, BlobStatus> blobIdToStatusMap,
      int replicaCount, boolean includeAcceptableInconsistentBlobs) {
    logOutput("Total keys processed " + totalKeysProcessed.get());
    logOutput("\nTotal Blobs Found " + blobIdToStatusMap.size());
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
            logOutput("Partially deleted (acceptable inconsistency) blob " + blobId + " isDeletedOrExpired "
                + consistencyBlobResult.getIsDeletedOrExpired() + "\n" + consistencyBlobResult);
          }
          acceptableInconsistentBlobs++;
        } else {
          realInconsistentBlobs++;
          logOutput(
              "Inconsistent Blob : " + blobId + " isDeletedOrExpired " + consistencyBlobResult.getIsDeletedOrExpired()
                  + "\n" + consistencyBlobResult);
        }
      }
    }
    logOutput("Total Inconsistent blobs count : " + inconsistentBlobs);
    if (includeAcceptableInconsistentBlobs) {
      logOutput("Acceptable Inconsistent blobs count : " + acceptableInconsistentBlobs);
    }
    logOutput("Real Inconsistent blobs count :" + realInconsistentBlobs);

    if (realInconsistentBlobs > 0) {
      return false;
    }
    return true;
  }

  class ReplicaProcessorForBlobs implements Runnable {

    File rootDirectory;
    ArrayList<String> replicaList;
    ConcurrentHashMap<String, BlobStatus> blobIdToStatusMap;
    AtomicLong totalKeysProcessed;
    DumpData dumpData;
    CountDownLatch countDownLatch;
    IndexStats indexStats;

    public ReplicaProcessorForBlobs(File rootDirectory, ArrayList<String> replicaList,
        ConcurrentHashMap<String, BlobStatus> blobIdToStatusMap, AtomicLong totalKeysProcessed, DumpData dumpData,
        CountDownLatch countDownLatch, IndexStats indexStats) {
      this.rootDirectory = rootDirectory;
      this.replicaList = replicaList;
      this.blobIdToStatusMap = blobIdToStatusMap;
      this.totalKeysProcessed = totalKeysProcessed;
      this.dumpData = dumpData;
      this.countDownLatch = countDownLatch;
      this.indexStats = indexStats;
    }

    public void run() {
      File[] indexFiles = rootDirectory.listFiles();
      long keysProcessedforReplica = 0;
      for (File indexFile : indexFiles) {
        keysProcessedforReplica += dumpData
            .dumpIndex(indexFile, rootDirectory.getName(), replicaList, new ArrayList<String>(), blobIdToStatusMap,
                indexStats, true);
      }
      logOutput("Total keys processed for " + rootDirectory.getName() + " " + keysProcessedforReplica);
      totalKeysProcessed.addAndGet(keysProcessedforReplica);
      countDownLatch.countDown();
    }
  }
}

class IndexConsistencyCheckerTool extends ConsistencyCheckerTool {

  public IndexConsistencyCheckerTool(ClusterMap map) {
    super(map);
  }

  @Override
  public boolean checkConsistency(String directoryForConsistencyCheck)
      throws Exception {
    boolean isConsistent = true;
    File rootDir = new File(directoryForConsistencyCheck);
    logOutput("Root directory for Partition is " + rootDir);
    for (File replicaFolder : rootDir.listFiles()) {
      try {
        isConsistent = checkSingleReplica(replicaFolder) && isConsistent;
      } catch (Exception e) {
        logOutput("Exception while performing operation: " + e);
      }
    }
    return isConsistent;
  }

  public boolean checkSingleReplica(File replicaFolder)
      throws Exception {
    boolean isConsistent = true;
    File[] indexFiles = replicaFolder.listFiles();
    Arrays.sort(indexFiles, new IndexFileNameComparator());

    logOutput("------- Processing " + replicaFolder.getName() + " -------");
    String lastProcessedIndexFileName = null;
    long lastLogOffsetEnd = 0;
    for (File indexFile : indexFiles) {
      long logOffsetStart = getLogOffsetStart(indexFile);
      isConsistent = validateValues(logOffsetStart, lastLogOffsetEnd, indexFile.getName(), lastProcessedIndexFileName);
      lastLogOffsetEnd = getLogOffsetEnd(indexFile);
      lastProcessedIndexFileName = indexFile.getName();
    }

    return isConsistent;
  }

  private boolean validateValues(long logOffsetStart, long lastLogOffsetEnd, String currentIndexFileName,
      String lastProcessedIndexFileName) {
    long difference = logOffsetStart - lastLogOffsetEnd;
    if (difference > 0) {
      logOutput(
          "Difference of " + difference + " between " + lastProcessedIndexFileName + " and " + currentIndexFileName
              + ". (" + lastLogOffsetEnd + ", " + logOffsetStart + ")");
      if (difference % DELETE_RECORD_SIZE != 0) {
        logOutput("The difference " + difference + " is not a multiple of delete record size");
      }
      return false;
    }
    return true;
  }

  private long getLogOffsetStart(File indexFile) {
    String name = indexFile.getName();
    return Long.parseLong(name.substring(0, name.indexOf("_")));
  }

  private long getLogOffsetEnd(File indexFile)
      throws Exception {
    long fileEndPointer;
    DataInputStream stream = new DataInputStream(new FileInputStream(indexFile));
    short version = stream.readShort();
    if (version == 0) {
      stream.skipBytes((2 * Integer.SIZE) / Byte.SIZE);
      fileEndPointer = stream.readLong();
    } else {
      throw new Exception("Version read from " + indexFile.getName() + " is " + version + " (not 0)");
    }
    return fileEndPointer;
  }

  static class IndexFileNameComparator implements Comparator {
    public int compare(Object o1, Object o2) {
      String s1 = ((File) o1).getName();
      String s2 = ((File) o2).getName();

      long n1 = Long.parseLong(s1.substring(0, s1.indexOf("_")));
      long n2 = Long.parseLong(s2.substring(0, s2.indexOf("_")));

      return Long.compare(n1, n2);
    }
  }
}

