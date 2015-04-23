package com.github.ambry.tools.admin;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapManager;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import java.io.FileWriter;
import java.io.IOException;
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
 * Consistency Checker took is used to check for consistency among replicas for any given partition
 */
public class ConsistencyCheckerTool {

  String outFile;
  FileWriter fileWriter;

  public void init(String outFile) {
    try {
      if (outFile != null) {
        this.outFile = outFile;
        fileWriter = new FileWriter(new File(outFile));
      }
    } catch (IOException IOException) {
      System.out.println("IOException while trying to create File " + this.outFile);
    }
  }

  public static void main(String args[]) {

    ConsistencyCheckerTool consistencyCheckerTool = new ConsistencyCheckerTool();

    try {
      OptionParser parser = new OptionParser();

      ArgumentAcceptingOptionSpec<String> hardwareLayoutOpt =
          parser.accepts("hardwareLayout", "The path of the hardware layout file").withRequiredArg()
              .describedAs("hardware_layout").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> partitionLayoutOpt =
          parser.accepts("partitionLayout", "The path of the partition layout file").withRequiredArg()
              .describedAs("partition_layout").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> rootDirectoryForPartitionOpt = parser.accepts("rootDirectoryForPartition",
          "Directory which contains all replicas for a partition which in turn will have all index filesfor the respective replica, for Operation ConsistencyCheckForIndex ")
          .withRequiredArg().describedAs("root_directory_partition").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> outFileOpt =
          parser.accepts("outFile", "Output file for \"ConsistencyCheckForIndex\" or \"ConsistencyCheckForLog\" ")
              .withRequiredArg().describedAs("outFile").ofType(String.class);

      ArgumentAcceptingOptionSpec<String> includeAcceptableInconsistentBlobsOpt =
          parser.accepts("includeAcceptableInconsistentBlobs", "To include acceptable inconsistent blobs")
              .withRequiredArg().describedAs("Whether to output acceptable inconsistent blobs or not")
              .defaultsTo("false").ofType(String.class);

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

      consistencyCheckerTool.init(outFile);
      consistencyCheckerTool.consistencyCheck(map, rootDirectoryForPartition, includeAcceptableInconsistentBlobs);
      consistencyCheckerTool.shutdown();
    } catch (Exception e) {
      consistencyCheckerTool.logOutput("Closed with error " + e);
    }
  }

  private void consistencyCheck(ClusterMap map, String directoryForConsistencyCheck,
      boolean includeAcceptableInconsistentBlobs)
      throws IOException, InterruptedException {
    File rootDir = new File(directoryForConsistencyCheck);
    ArrayList<String> replicaList = populateReplicaList(rootDir);
    logOutput("Replica List " + replicaList);
    ConcurrentHashMap<String, BlobStatus> blobIdToStatusMap = new ConcurrentHashMap<String, BlobStatus>();
    AtomicLong totalKeysProcessed = new AtomicLong(0);
    int replicaCount = replicaList.size();
    checkForConsistency(rootDir.listFiles(), map, replicaList, blobIdToStatusMap, totalKeysProcessed);
    populateOutput(totalKeysProcessed, blobIdToStatusMap, replicaCount, includeAcceptableInconsistentBlobs);
  }

  private ArrayList<String> populateReplicaList(File rootDir) {
    logOutput("Root directory for Partition" + rootDir);
    ArrayList<String> replicaList = new ArrayList<String>();
    File[] replicas = rootDir.listFiles();
    for (File replicaFile : replicas) {
      replicaList.add(replicaFile.getName());
    }
    return replicaList;
  }

  private void checkForConsistency(File[] replicas, ClusterMap map, ArrayList<String> replicasList,
      ConcurrentHashMap<String, BlobStatus> blobIdToStatusMap, AtomicLong totalKeysProcessed)
      throws IOException, InterruptedException {
    DumpData dumpData = new DumpData(outFile, fileWriter, map);
    CountDownLatch countDownLatch = new CountDownLatch(replicas.length);
    for (File replica : replicas) {
      Thread thread = new Thread(new ReplicaProcessorThread(map, replica, replicasList, blobIdToStatusMap, totalKeysProcessed, dumpData,
          countDownLatch));
      thread.start();
      thread.join();
    }
    countDownLatch.await();
  }

  public void logOutput(String msg) {
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

  private void populateOutput(AtomicLong totalKeysProcessed, ConcurrentHashMap<String, BlobStatus> blobIdToStatusMap,
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

  class ReplicaProcessorThread implements Runnable {

    ClusterMap map;
    File rootDirectory;
    ArrayList<String> replicaList;
    ConcurrentHashMap<String, BlobStatus> blobIdToStatusMap;
    AtomicLong totalKeysProcessed;
    DumpData dumpData;
    CountDownLatch countDownLatch;

    public ReplicaProcessorThread(ClusterMap map, File rootDirectory, ArrayList<String> replicaList,
        ConcurrentHashMap<String, BlobStatus> blobIdToStatusMap, AtomicLong totalKeysProcessed, DumpData dumpData,
        CountDownLatch countDownLatch) {
      this.map = map;
      this.rootDirectory = rootDirectory;
      this.replicaList = replicaList;
      this.blobIdToStatusMap = blobIdToStatusMap;
      this.totalKeysProcessed = totalKeysProcessed;
      this.dumpData = dumpData;
      this.countDownLatch = countDownLatch;
    }

    public void run() {
      File[] indexFiles = rootDirectory.listFiles();
      long keysProcessedforReplica = 0;
      for (File indexFile : indexFiles) {
        keysProcessedforReplica += dumpData
            .dumpIndex(indexFile, rootDirectory.getName(), replicaList, new ArrayList<String>(), blobIdToStatusMap);
      }
      logOutput("Total keys processed for " + rootDirectory.getName() + " " + keysProcessedforReplica);
      totalKeysProcessed.addAndGet(keysProcessedforReplica);
      countDownLatch.countDown();
    }
  }
}
