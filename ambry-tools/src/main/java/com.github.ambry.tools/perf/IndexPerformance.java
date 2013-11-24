package com.github.ambry.tools.perf;

import com.github.ambry.metrics.MetricsRegistryMap;
import com.github.ambry.metrics.ReadableMetricsRegistry;
import com.github.ambry.shared.BlobId;
import com.github.ambry.store.*;
import com.github.ambry.utils.Scheduler;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Throttler;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.File;
import java.util.ArrayList;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tests the memory boundaries and latencies of the index structure
 */
public class IndexPerformance {

  static class BlobIndexMetrics extends BlobIndex {
    private Object lock = new Object();
    private boolean enableVerboseLogging;
    private AtomicLong totalWrites;
    private AtomicLong totalTimeTaken;

    public BlobIndexMetrics(String datadir, Scheduler scheduler, Log log, boolean enableVerboseLogging,
                            AtomicLong totalWrites, AtomicLong totalTimeTaken)  throws IndexCreationException {
      super(datadir, scheduler, log);
      this.enableVerboseLogging = enableVerboseLogging;
      this.totalWrites = totalWrites;
      this.totalTimeTaken = totalTimeTaken;
    }

    public void AddToIndexRandomData() {
      BlobId id = new BlobId("1" + UUID.randomUUID());
      BlobIndex.BlobIndexEntry entry = new BlobIndex.BlobIndexEntry(id, new BlobIndex.BlobIndexValue(1000, 1000, (byte)1, 1000));
      long startTimeInMs = System.nanoTime();
      synchronized (lock) {
        long offset = getCurrentEndOffset();
        AddToIndex(entry, offset + entry.getValue().getSize());
      }
      long endTimeInMs = System.nanoTime();
      if (enableVerboseLogging)
        System.out.println("Time taken to add to the index - " + (endTimeInMs - startTimeInMs));
      totalWrites.incrementAndGet();
      totalTimeTaken.addAndGet(endTimeInMs - startTimeInMs);
    }

    public void AddToIndexRandomData(int count) {
      ArrayList<BlobIndex.BlobIndexEntry> list = new ArrayList<BlobIndex.BlobIndexEntry>(count);
      for (int i = 0; i < list.size(); i++) {
        BlobId id = new BlobId("1" + UUID.randomUUID());
        BlobIndex.BlobIndexEntry entry = new BlobIndex.BlobIndexEntry(id, new BlobIndex.BlobIndexValue(1000, 1000, (byte)1, 1000));
        list.add(entry);
      }
      long startTimeInMs = System.currentTimeMillis();
      synchronized (lock) {
        AddToIndex(list, getCurrentEndOffset() + 1000);
      }
      long endTimeInMs = System.currentTimeMillis();
      System.out.println("Time taken to add to the index all the entries - " + (endTimeInMs - startTimeInMs));

    }

    public boolean exist(StoreKey key) {
      long startTimeMs = System.currentTimeMillis();
      boolean exist = exist(key);
      long endTimeMs = System.currentTimeMillis();
      System.out.println("Time to find an entry exist - " + (endTimeMs - startTimeMs));
      return exist;
    }

    public void markAsDeleted(StoreKey id, long logEndOffset) throws StoreException {
      long startTimeMs = System.currentTimeMillis();
      markAsDeleted(id, logEndOffset);
      long endTimeMs = System.currentTimeMillis();
      System.out.println("Time to delete an entry - " + (endTimeMs - startTimeMs));
    }

    public void updateTTL(StoreKey id, long ttl, long logEnfOffset) throws StoreException {
      long startTimeMs = System.currentTimeMillis();
      updateTTL(id, ttl, logEnfOffset);
      long endTimeMs = System.currentTimeMillis();
      System.out.println("Time to update ttl - " + (endTimeMs - startTimeMs));
    }
  }

  class MockScheduler extends Scheduler {
    public MockScheduler(int noOfThreads, String threadNamePrefix, boolean isDaemon) {
      super(noOfThreads, threadNamePrefix, isDaemon);
    }

    @Override
    public void schedule(final String name, final Runnable func, long delay, long period, TimeUnit unit) {

    }

  }

  public static void main(String args[]) {
    try {
      OptionParser parser = new OptionParser();
      ArgumentAcceptingOptionSpec<Integer> numberOfIndexesOpt = parser.accepts("numberOfIndexes", "The number of indexes to create")
                                                                      .withRequiredArg()
                                                                      .describedAs("number_of_indexes")
                                                                      .ofType(Integer.class);

      ArgumentAcceptingOptionSpec<Integer> numberOfWritersOpt = parser.accepts("numberOfWriters", "The number of writers that write to a random index concurrently")
                                                                      .withRequiredArg()
                                                                      .describedAs("The number of writers")
                                                                      .ofType(Integer.class)
                                                                      .defaultsTo(4);

      ArgumentAcceptingOptionSpec<Integer> writesPerSecondOpt = parser.accepts("writesPerSecond", "The rate at which writes need to be performed")
                                                                      .withRequiredArg()
                                                                      .describedAs("The number of writes per second")
                                                                      .ofType(Integer.class)
                                                                      .defaultsTo(1000);

      ArgumentAcceptingOptionSpec<Boolean> verboseLoggingOpt = parser.accepts("enableVerboseLogging", "Enables verbose logging")
                                                                     .withOptionalArg()
                                                                     .describedAs("Enable verbose logging")
                                                                     .ofType(Boolean.class)
                                                                     .defaultsTo(false);

      OptionSet options = parser.parse(args);

      ArrayList<OptionSpec<?>> listOpt = new ArrayList<OptionSpec<?>>();
      listOpt.add(numberOfIndexesOpt);

      for(OptionSpec opt : listOpt) {
        if(!options.has(opt)) {
          System.err.println("Missing required argument \"" + opt + "\"");
          parser.printHelpOn(System.err);
          System.exit(1);
        }
      }

      int numberOfIndexes = options.valueOf(numberOfIndexesOpt);
      int numberOfWriters = options.valueOf(numberOfWritersOpt);
      int writesPerSecond = options.valueOf(writesPerSecondOpt);
      boolean enableVerboseLogging = options.has(verboseLoggingOpt) ? true : false;
      if (enableVerboseLogging)
        System.out.println("Enabled verbose logging");
      final AtomicLong totalTimeTaken = new AtomicLong(0);
      final AtomicLong totalWrites = new AtomicLong(0);

      File f = File.createTempFile("ambry", ".tmp");
      File indexFile = new File(f.getParent(), "index_current");
      System.out.println("deleting index file " + indexFile.getAbsolutePath());
      indexFile.delete();
      ReadableMetricsRegistry registry = new MetricsRegistryMap();
      Metrics metrics = new Metrics("test", registry);
      Log log = new Log(f.getParent(), metrics);
      Scheduler s = new Scheduler(numberOfWriters, "index", false);

      ArrayList<BlobIndexMetrics> indexWithMetrics = new ArrayList<BlobIndexMetrics>(numberOfIndexes);
      for (int i = 0; i < numberOfIndexes; i++) {
        indexWithMetrics.add(new BlobIndexMetrics(f.getParent(), s, log, enableVerboseLogging, totalWrites, totalTimeTaken));
      }

      final CountDownLatch latch = new CountDownLatch(4);
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
      for (int i = 0; i < numberOfWriters; i++) {
        threadIndexPerf[i] = new Thread(new IndexPerfRun(indexWithMetrics, throttler, shutdown, latch));
        threadIndexPerf[i].start();
      }
    }
    catch (IndexCreationException e) {
      System.err.println("Index creation error on exit " + e.getMessage());
    }
    catch (Exception e) {
      System.err.println("Error on exit " + e);
    }
  }

  public static class IndexPerfRun implements Runnable {
    private ArrayList<BlobIndexMetrics> indexesWithMetrics;
    private Throttler throttler;
    private AtomicBoolean isShutdown;
    private CountDownLatch latch;

    public IndexPerfRun(ArrayList<BlobIndexMetrics> indexesWithMetrics, Throttler throttler,
                        AtomicBoolean isShutdown, CountDownLatch latch) {
      this.indexesWithMetrics = indexesWithMetrics;
      this.throttler = throttler;
      this.isShutdown = isShutdown;
      this.latch = latch;
    }
    public void run() {
      try {
        System.out.println("Starting index performance");
        System.out.flush();
        while (!isShutdown.get()) {

          // choose a random index
          int indexToUse = new Random().nextInt(indexesWithMetrics.size());
          indexesWithMetrics.get(indexToUse).AddToIndexRandomData();
          throttler.maybeThrottle(1);
        }
      }
      catch (Exception e) {
        System.out.println("Exiting index perf thread " + e);
      }
      finally {
        latch.countDown();
      }
    }

  }

}
