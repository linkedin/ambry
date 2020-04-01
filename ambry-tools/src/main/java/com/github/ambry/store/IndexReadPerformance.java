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
import com.github.ambry.clustermap.ClusterAgentsFactory;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.BlobIdFactory;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Throttler;
import com.github.ambry.utils.Utils;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;


/**
 * Reads from a file and populates the indexes and issues
 * random read requests and tracks performance . This test reads 10000 ids
 * at a time from the index write performance log and does random reads from that list.
 * After 2 minutes it replaces the input with the next 10000 set.
 */
public class IndexReadPerformance {

  static class IndexPayload {
    private BlobIndexMetrics index;
    private HashSet<String> ids;

    public IndexPayload(BlobIndexMetrics index, HashSet<String> ids) {
      this.index = index;
      this.ids = ids;
    }

    public BlobIndexMetrics getIndex() {
      return index;
    }

    public HashSet<String> getIds() {
      return ids;
    }
  }

  public static void main(String args[]) {
    try {
      OptionParser parser = new OptionParser();
      ArgumentAcceptingOptionSpec<String> logToReadOpt =
          parser.accepts("logToRead", "The log that needs to be replayed for traffic")
              .withRequiredArg()
              .describedAs("log_to_read")
              .ofType(String.class);

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

      ArgumentAcceptingOptionSpec<Integer> numberOfReadersOpt =
          parser.accepts("numberOfReaders", "The number of readers that read to a random index concurrently")
              .withRequiredArg()
              .describedAs("The number of readers")
              .ofType(Integer.class)
              .defaultsTo(4);

      ArgumentAcceptingOptionSpec<Integer> readsPerSecondOpt =
          parser.accepts("readsPerSecond", "The rate at which reads need to be performed")
              .withRequiredArg()
              .describedAs("The number of reads per second")
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
      listOpt.add(logToReadOpt);
      listOpt.add(hardwareLayoutOpt);
      listOpt.add(partitionLayoutOpt);

      ToolUtils.ensureOrExit(listOpt, options, parser);

      String logToRead = options.valueOf(logToReadOpt);
      int numberOfReaders = options.valueOf(numberOfReadersOpt);
      int readsPerSecond = options.valueOf(readsPerSecondOpt);
      boolean enableVerboseLogging = options.has(verboseLoggingOpt);
      if (enableVerboseLogging) {
        System.out.println("Enabled verbose logging");
      }
      String hardwareLayoutPath = options.valueOf(hardwareLayoutOpt);
      String partitionLayoutPath = options.valueOf(partitionLayoutOpt);
      ClusterMapConfig clusterMapConfig = new ClusterMapConfig(new VerifiableProperties(new Properties()));
      ClusterMap map =
          ((ClusterAgentsFactory) Utils.getObj(clusterMapConfig.clusterMapClusterAgentsFactory, clusterMapConfig,
              hardwareLayoutPath, partitionLayoutPath)).getClusterMap();
      StoreKeyFactory factory = new BlobIdFactory(map);

      // Read the log and get the index directories and create the indexes
      final BufferedReader br = new BufferedReader(new FileReader(logToRead));
      final HashMap<String, IndexPayload> hashes = new HashMap<String, IndexPayload>();
      String line;
      MetricRegistry metricRegistry = new MetricRegistry();
      StoreMetrics metrics = new StoreMetrics(metricRegistry);
      ScheduledExecutorService s = Utils.newScheduler(numberOfReaders, "index", true);
      DiskSpaceAllocator diskSpaceAllocator =
          new DiskSpaceAllocator(false, null, 0, new StorageManagerMetrics(metricRegistry));
      Properties props = new Properties();
      props.setProperty("store.index.memory.size.bytes", "1048576");
      props.setProperty("store.segment.size.in.bytes", "1000");
      StoreConfig config = new StoreConfig(new VerifiableProperties(props));
      Log log = new Log(System.getProperty("user.dir"), 1000, diskSpaceAllocator, config, metrics);

      final AtomicLong totalTimeTaken = new AtomicLong(0);
      final AtomicLong totalReads = new AtomicLong(0);
      final CountDownLatch latch = new CountDownLatch(numberOfReaders);
      final AtomicBoolean shutdown = new AtomicBoolean(false);
      // attach shutdown handler to catch control-c
      Runtime.getRuntime().addShutdownHook(new Thread() {
        public void run() {
          try {
            System.out.println("Shutdown invoked");
            shutdown.set(true);
            latch.await();
            System.out.println("Total reads : " + totalReads.get() + "  Total time taken : " + totalTimeTaken.get()
                + " Nano Seconds  Average time taken per read "
                + ((double) totalReads.get() / totalTimeTaken.get()) / SystemTime.NsPerSec + " Seconds");
          } catch (Exception e) {
            System.out.println("Error while shutting down " + e);
          }
        }
      });
      ScheduledExecutorService scheduleReadLog = Utils.newScheduler(1, true);
      while ((line = br.readLine()) != null) {
        if (line.startsWith("logdir")) {
          String[] logdirs = line.split("-");
          BlobIndexMetrics metricIndex =
              new BlobIndexMetrics(logdirs[1], s, log, enableVerboseLogging, totalReads, totalTimeTaken, totalReads,
                  config, null, factory);
          hashes.put(logdirs[1], new IndexPayload(metricIndex, new HashSet<String>()));
        } else {
          break;
        }
      }

      // read next batch of ids after 2 minutes
      scheduleReadLog.scheduleAtFixedRate(new Runnable() {
        @Override
        public void run() {
          populateIds(br, hashes);
        }
      }, 0, 120, TimeUnit.SECONDS);

      Throttler throttler = new Throttler(readsPerSecond, 100, true, SystemTime.getInstance());
      Thread[] threadIndexPerf = new Thread[numberOfReaders];
      for (int i = 0; i < numberOfReaders; i++) {
        threadIndexPerf[i] = new Thread(new IndexReadPerfRun(hashes, throttler, shutdown, latch, map));
        threadIndexPerf[i].start();
      }

      for (int i = 0; i < numberOfReaders; i++) {
        threadIndexPerf[i].join();
      }

      br.close();
    } catch (Exception e) {
      System.out.println("Exiting process with exception " + e);
    }
  }

  public static void populateIds(BufferedReader reader, HashMap<String, IndexPayload> hashes) {
    try {
      // read the first 10000 or less blob ids from the log
      synchronized (hashes) {
        System.out.println("Reading ids");
        String line;
        int count = 0;
        for (Map.Entry<String, IndexPayload> payload : hashes.entrySet()) {
          payload.getValue().getIds().clear();
        }
        while ((line = reader.readLine()) != null) {
          if (line.startsWith("blobid")) {
            String[] ids = line.split("-");
            HashSet<String> idsForIndex = hashes.get(ids[1]).getIds();
            idsForIndex.add(ids[2] + "-" + ids[3] + "-" + ids[4] + "-" + ids[5] + "-" + ids[6]);
            System.out.println("Reading id " + ids[2]);
          }
          if (count == 10000) {
            break;
          }
          count++;
        }
      }
    } catch (Exception e) {
      System.out.print("Error when reading from log " + e);
    }
  }

  public static class IndexReadPerfRun implements Runnable {
    private HashMap<String, IndexPayload> hashes;
    private Throttler throttler;
    private AtomicBoolean isShutdown;
    private CountDownLatch latch;
    private ClusterMap map;

    public IndexReadPerfRun(HashMap<String, IndexPayload> hashes, Throttler throttler, AtomicBoolean isShutdown,
        CountDownLatch latch, ClusterMap map) {
      this.hashes = hashes;
      this.throttler = throttler;
      this.isShutdown = isShutdown;
      this.latch = latch;
      this.map = map;
    }

    public void run() {
      try {
        System.out.println("Starting index read performance");
        System.out.flush();
        while (!isShutdown.get()) {

          synchronized (hashes) {
            // choose a random index
            int indexToUse = new Random().nextInt(hashes.size());
            IndexPayload index = (IndexPayload) hashes.values().toArray()[indexToUse];
            if (index.getIds().size() == 0) {
              continue;
            }
            int idToUse = new Random().nextInt(index.getIds().size());
            String idToLookup = (String) index.getIds().toArray()[idToUse];

            if (index.getIndex().findKey(new BlobId(idToLookup, map)) == null) {
              System.out.println("Error id not found in index " + idToLookup);
            } else {
              System.out.println("found id " + idToLookup);
            }
            throttler.maybeThrottle(1);
          }
          Thread.sleep(1000);
        }
      } catch (Exception e) {
        System.out.println("Exiting index read perf thread " + e.getCause());
      } finally {
        latch.countDown();
      }
    }
  }
}
