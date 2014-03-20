package com.github.ambry.tools.perf;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapManager;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.coordinator.AmbryCoordinator;
import com.github.ambry.coordinator.Coordinator;
import com.github.ambry.coordinator.CoordinatorException;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Throttler;
import com.github.ambry.utils.Utils;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.File;
import java.io.FileWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Generates load for write performance test
 */
public class ServerWritePerformance {
  public static void main(String args[]) {
    FileWriter writer = null;
    AmbryCoordinator coordinator = null;
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

      ArgumentAcceptingOptionSpec<Integer> numberOfWritersOpt =
              parser.accepts("numberOfWriters", "The number of writers that issue put request")
                    .withRequiredArg()
                    .describedAs("The number of writers")
                    .ofType(Integer.class)
                    .defaultsTo(4);

      ArgumentAcceptingOptionSpec<Integer> minBlobSizeOpt =
              parser.accepts("minBlobSizeInBytes", "The minimum size of the blob that can be put")
                    .withRequiredArg()
                    .describedAs("The minimum blob size in bytes")
                    .ofType(Integer.class)
                    .defaultsTo(51200);

      ArgumentAcceptingOptionSpec<Integer> maxBlobSizeOpt =
              parser.accepts("maxBlobSizeInBytes", "The maximum size of the blob that can be put")
                    .withRequiredArg()
                    .describedAs("The maximum blob size in bytes")
                    .ofType(Integer.class)
                    .defaultsTo(4194304);

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

      ArgumentAcceptingOptionSpec<String> coordinatorConfigPathOpt =
              parser.accepts("coordinatorConfigPath", "The config for the coordinator")
                    .withRequiredArg()
                    .describedAs("coordinator_config_path")
                    .ofType(String.class);

      OptionSet options = parser.parse(args);

      ArrayList<OptionSpec<?>> listOpt = new ArrayList<OptionSpec<?>>();
      listOpt.add(hardwareLayoutOpt);
      listOpt.add(partitionLayoutOpt);
      listOpt.add(coordinatorConfigPathOpt);

      for(OptionSpec opt : listOpt) {
        if(!options.has(opt)) {
          System.err.println("Missing required argument \"" + opt + "\"");
          parser.printHelpOn(System.err);
          System.exit(1);
        }
      }

      int numberOfWriters = options.valueOf(numberOfWritersOpt);
      int writesPerSecond = options.valueOf(writesPerSecondOpt);
      boolean enableVerboseLogging = options.has(verboseLoggingOpt) ? true : false;
      String coordinatorConfigPath = options.valueOf(coordinatorConfigPathOpt);
      int minBlobSize = options.valueOf(minBlobSizeOpt);
      int maxBlobSize = options.valueOf(maxBlobSizeOpt);
      if (enableVerboseLogging)
        System.out.println("Enabled verbose logging");
      final AtomicLong totalTimeTaken = new AtomicLong(0);
      final AtomicLong totalWrites = new AtomicLong(0);
      String hardwareLayoutPath = options.valueOf(hardwareLayoutOpt);
      String partitionLayoutPath = options.valueOf(partitionLayoutOpt);
      ClusterMap map = new ClusterMapManager(hardwareLayoutPath, partitionLayoutPath);

      File logFile = new File(System.getProperty("user.dir"), "writeperflog");
      writer = new FileWriter(logFile);

      final CountDownLatch latch = new CountDownLatch(numberOfWriters);
      final AtomicBoolean shutdown = new AtomicBoolean(false);
      // attach shutdown handler to catch control-c
      Runtime.getRuntime().addShutdownHook(new Thread() {
        public void run() {
          try {
            System.out.println("Shutdown invoked");
            shutdown.set(true);
            latch.await();
            System.out.println("Total writes : " + totalWrites.get() + "  Total time taken : " + totalTimeTaken.get() +
                    " Nano Seconds  Average time taken per write " +
                    ((double)totalWrites.get() / totalTimeTaken.get()) / SystemTime.NsPerSec + " Seconds");
          }
          catch (Exception e) {
            System.out.println("Error while shutting down " + e);
          }
        }
      });

      Throttler throttler = new Throttler(writesPerSecond, 100, true, SystemTime.getInstance());
      Thread[] threadIndexPerf = new Thread[numberOfWriters];

      Properties props = Utils.loadProps(coordinatorConfigPath);
      coordinator = new AmbryCoordinator(new VerifiableProperties(props), map);
      coordinator.start();

      for (int i = 0; i < numberOfWriters; i++) {
        threadIndexPerf[i] = new Thread(new ServerWritePerfRun(throttler,
                                                               shutdown,
                                                               latch,
                                                               minBlobSize,
                                                               maxBlobSize,
                                                               writer,
                                                               totalTimeTaken,
                                                               totalWrites,
                                                               enableVerboseLogging,
                                                               coordinator));
        threadIndexPerf[i].start();
      }
      for (int i = 0; i < numberOfWriters; i++) {
        threadIndexPerf[i].join();
      }
    }
    catch (Exception e) {
      System.err.println("Error on exit " + e);
    }
    finally {
      if (coordinator != null) {
        coordinator.shutdown();
      }
      if (writer != null) {
        try {
          writer.close();
        }
        catch (Exception e) {
          System.out.println("Error when closing the writer");
        }
      }
    }
  }

  public static class ServerWritePerfRun implements Runnable {

    private Throttler throttler;
    private AtomicBoolean isShutdown;
    private CountDownLatch latch;
    private Coordinator coordinator;
    private Random rand = new Random();
    private int minBlobSize;
    private int maxBlobSize;
    private FileWriter writer;
    private AtomicLong totalTimeTaken;
    private AtomicLong totalWrites;
    private boolean enableVerboseLogging;

    public ServerWritePerfRun(Throttler throttler,
                              AtomicBoolean isShutdown,
                              CountDownLatch latch,
                              int minBlobSize,
                              int maxBlobSize,
                              FileWriter writer,
                              AtomicLong totalTimeTaken,
                              AtomicLong totalWrites,
                              boolean enableVerboseLogging,
                              AmbryCoordinator coordinator) {
      this.throttler = throttler;
      this.isShutdown = isShutdown;
      this.latch = latch;
      this.coordinator = coordinator;
      this.minBlobSize = minBlobSize;
      this.maxBlobSize = maxBlobSize;
      this.writer = writer;
      this.totalTimeTaken = totalTimeTaken;
      this.totalWrites = totalWrites;
      this.enableVerboseLogging = enableVerboseLogging;
    }

    public void run() {
      try {
        System.out.println("Starting write server performance");
        long numberOfPuts = 0;
        long timePassedInMs = 0;
        while (!isShutdown.get()) {

          int randomNum = rand.nextInt((maxBlobSize - minBlobSize) + 1) + minBlobSize;
          byte[] blob = new byte[randomNum];
          byte[] usermetadata = new byte[new Random().nextInt(1024)];
          BlobProperties props = new BlobProperties(randomNum, "test");
          long startTime = System.currentTimeMillis();
          try {
            String id = coordinator.putBlob(props, ByteBuffer.wrap(usermetadata), new ByteBufferInputStream(ByteBuffer.wrap(blob)));
            long endTime = System.currentTimeMillis();
            writer.write("Blob-" + id + "\n");
            totalWrites.incrementAndGet();
            if (enableVerboseLogging)
              System.out.println("Time taken to put " + (endTime - startTime) + " for blob of size " + blob.length);
            timePassedInMs += (endTime - startTime);
            numberOfPuts++;
            if (timePassedInMs >= 1000) {
              System.out.println("Number of puts " + numberOfPuts + " in " + timePassedInMs + " ms");
              numberOfPuts = 0;
              timePassedInMs = 0;
            }
            totalTimeTaken.addAndGet(endTime - startTime);
            throttler.maybeThrottle(1);
          }
          catch (CoordinatorException e) {
            System.out.println("Exception when putting blob " + e);
          }
        }
      }
      catch (Exception e) {
        System.out.println("Exiting server write perf thread " + e);
      }
      finally {
        latch.countDown();
      }
    }
  }
}
