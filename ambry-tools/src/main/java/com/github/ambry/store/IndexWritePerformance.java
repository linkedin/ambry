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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.clustermap.ClusterAgentsFactory;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.BlobIdFactory;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Throttler;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;


/**
 * Tests the memory boundaries and latencies of the index structure
 * during writes
 */
public class IndexWritePerformance {

  public static void main(String args[]) {
    FileWriter writer = null;
    try {
      OptionParser parser = new OptionParser();
      ArgumentAcceptingOptionSpec<Integer> numberOfIndexesOpt =
          parser.accepts("numberOfIndexes", "The number of indexes to create")
              .withRequiredArg()
              .describedAs("number_of_indexes")
              .ofType(Integer.class);

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

      ArgumentAcceptingOptionSpec<Integer> numberOfWritersOpt =
          parser.accepts("numberOfWriters", "The number of writers that write to a random index concurrently")
              .withRequiredArg()
              .describedAs("The number of writers")
              .ofType(Integer.class)
              .defaultsTo(4);

      ArgumentAcceptingOptionSpec<Integer> writesPerSecondOpt =
          parser.accepts("writesPerSecond", "The rate at which writes need to be performed")
              .withRequiredArg()
              .describedAs("The number of writes per second")
              .ofType(Integer.class)
              .defaultsTo(1000);

      ArgumentAcceptingOptionSpec<Boolean> verboseLoggingOpt =
          parser.accepts("enableVerboseLogging", "Enables verbose logging")
              .withOptionalArg()
              .describedAs("Enable verbose logging")
              .ofType(Boolean.class)
              .defaultsTo(false);

      OptionSet options = parser.parse(args);

      ArrayList<OptionSpec> listOpt = new ArrayList<>();
      listOpt.add(numberOfIndexesOpt);
      listOpt.add(hardwareLayoutOpt);
      listOpt.add(partitionLayoutOpt);

      ToolUtils.ensureOrExit(listOpt, options, parser);

      int numberOfIndexes = options.valueOf(numberOfIndexesOpt);
      int numberOfWriters = options.valueOf(numberOfWritersOpt);
      int writesPerSecond = options.valueOf(writesPerSecondOpt);
      boolean enableVerboseLogging = options.has(verboseLoggingOpt);
      if (enableVerboseLogging) {
        System.out.println("Enabled verbose logging");
      }
      final AtomicLong totalTimeTakenInNs = new AtomicLong(0);
      final AtomicLong totalWrites = new AtomicLong(0);
      String hardwareLayoutPath = options.valueOf(hardwareLayoutOpt);
      String partitionLayoutPath = options.valueOf(partitionLayoutOpt);

      ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(new Properties()));
      ClusterMap map =
          ((ClusterAgentsFactory) Utils.getObj(clusterMapConfig.clusterMapClusterAgentsFactory, clusterMapConfig,
              hardwareLayoutPath, partitionLayoutPath)).getClusterMap();
      StoreKeyFactory factory = new BlobIdFactory(map);

      File logFile = new File(System.getProperty("user.dir"), "writeperflog");
      writer = new FileWriter(logFile);

      MetricRegistry metricRegistry = new MetricRegistry();
      StoreMetrics metrics = new StoreMetrics(metricRegistry);
      DiskSpaceAllocator diskSpaceAllocator =
          new DiskSpaceAllocator(false, null, 0, new StorageManagerMetrics(metricRegistry));
      Properties props = new Properties();
      props.setProperty("store.index.memory.size.bytes", "2097152");
      props.setProperty("store.segment.size.in.bytes", "10");
      StoreConfig config = new StoreConfig(new VerifiableProperties(props));
      Log log = new Log(System.getProperty("user.dir"), 10, diskSpaceAllocator, config, metrics);

      ScheduledExecutorService s = Utils.newScheduler(numberOfWriters, "index", false);

      ArrayList<BlobIndexMetrics> indexWithMetrics = new ArrayList<BlobIndexMetrics>(numberOfIndexes);
      for (int i = 0; i < numberOfIndexes; i++) {
        File indexFile = new File(System.getProperty("user.dir"), Integer.toString(i));
        if (indexFile.exists()) {
          for (File c : indexFile.listFiles()) {
            c.delete();
          }
        } else {
          indexFile.mkdir();
        }
        System.out.println("Creating index folder " + indexFile.getAbsolutePath());
        writer.write("logdir-" + indexFile.getAbsolutePath() + "\n");
        indexWithMetrics.add(
            new BlobIndexMetrics(indexFile.getAbsolutePath(), s, log, enableVerboseLogging, totalWrites,
                totalTimeTakenInNs, totalWrites, config, writer, factory));
      }

      final CountDownLatch latch = new CountDownLatch(numberOfWriters);
      final AtomicBoolean shutdown = new AtomicBoolean(false);
      // attach shutdown handler to catch control-c
      Runtime.getRuntime().addShutdownHook(new Thread() {
        public void run() {
          try {
            System.out.println("Shutdown invoked");
            shutdown.set(true);
            latch.await();
            System.out.println(
                "Total writes : " + totalWrites.get() + "  Total time taken : " + totalTimeTakenInNs.get()
                    + " Nano Seconds  Average time taken per write "
                    + ((double) totalWrites.get() / totalTimeTakenInNs.get()) / SystemTime.NsPerSec + " Seconds");
          } catch (Exception e) {
            System.out.println("Error while shutting down " + e);
          }
        }
      });

      Throttler throttler = new Throttler(writesPerSecond, 100, true, SystemTime.getInstance());
      Thread[] threadIndexPerf = new Thread[numberOfWriters];
      for (int i = 0; i < numberOfWriters; i++) {
        threadIndexPerf[i] = new Thread(new IndexWritePerfRun(indexWithMetrics, throttler, shutdown, latch, map));
        threadIndexPerf[i].start();
      }
      for (int i = 0; i < numberOfWriters; i++) {
        threadIndexPerf[i].join();
      }
    } catch (StoreException e) {
      System.err.println("Index creation error on exit " + e.getMessage());
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
    }
  }

  public static class IndexWritePerfRun implements Runnable {
    private final ArrayList<BlobIndexMetrics> indexesWithMetrics;
    private final Throttler throttler;
    private final AtomicBoolean isShutdown;
    private final CountDownLatch latch;
    private final ClusterMap map;
    private final short blobIdVersion;

    public IndexWritePerfRun(ArrayList<BlobIndexMetrics> indexesWithMetrics, Throttler throttler,
        AtomicBoolean isShutdown, CountDownLatch latch, ClusterMap map) {
      this.indexesWithMetrics = indexesWithMetrics;
      this.throttler = throttler;
      this.isShutdown = isShutdown;
      this.latch = latch;
      this.map = map;
      Properties props = new Properties();
      props.setProperty("router.hostname", "localhost");
      props.setProperty("router.datacenter.name", "localDC");
      blobIdVersion = new RouterConfig(new VerifiableProperties(props)).routerBlobidCurrentVersion;
    }

    public void run() {
      try {
        System.out.println("Starting write index performance");
        System.out.flush();
        while (!isShutdown.get()) {

          // choose a random index
          int indexToUse = new Random().nextInt(indexesWithMetrics.size());
          // Does not matter what partition we use
          PartitionId partition = map.getWritablePartitionIds(null).get(0);
          indexesWithMetrics.get(indexToUse)
              .addToIndexRandomData(new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, map.getLocalDatacenterId(),
                  Account.UNKNOWN_ACCOUNT_ID, Container.UNKNOWN_CONTAINER_ID, partition, false,
                  BlobId.BlobDataType.DATACHUNK));
          throttler.maybeThrottle(1);
        }
      } catch (Exception e) {
        System.out.println("Exiting write index perf thread " + e);
      } finally {
        latch.countDown();
      }
    }
  }
}
