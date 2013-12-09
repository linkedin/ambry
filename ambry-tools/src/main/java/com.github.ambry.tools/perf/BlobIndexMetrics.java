package com.github.ambry.tools.perf;

import com.github.ambry.config.StoreConfig;
import com.github.ambry.shared.BlobId;
import com.github.ambry.store.*;
import com.github.ambry.utils.Scheduler;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

class BlobIndexMetrics extends BlobPersistantIndex {
  private Object lock = new Object();
  private boolean enableVerboseLogging;
  private AtomicLong totalWrites;
  private AtomicLong totalTimeTaken;
  private AtomicLong lastOffsetUsed;
  private AtomicLong totalReads;
  private FileWriter writer;
  private String datadir;

  public BlobIndexMetrics(String datadir, Scheduler scheduler, Log log, boolean enableVerboseLogging,
                          AtomicLong totalWrites, AtomicLong totalTimeTaken, AtomicLong totalReads,
                          StoreConfig config, FileWriter writer, StoreKeyFactory factory)  throws StoreException {
    super(datadir, scheduler, log, config, factory);
    this.enableVerboseLogging = enableVerboseLogging;
    this.lastOffsetUsed = new AtomicLong(0);
    this.totalWrites = totalWrites;
    this.totalTimeTaken = totalTimeTaken;
    this.totalReads = totalReads;
    this.writer = writer;
    this.datadir = datadir;
  }

  public void AddToIndexRandomData(BlobId id) throws StoreException {

    synchronized (lock) {
      long startTimeInMs = System.currentTimeMillis();
      long size = new Random().nextInt(10000);
      if (size < 0)
        size = size * -1;
      BlobIndexEntry entry = new BlobIndexEntry(id, new BlobIndexValue(size, lastOffsetUsed.get(), (byte)1, 1000));
      lastOffsetUsed.addAndGet(size);
      long offset = getCurrentEndOffset();
      addToIndex(entry, offset + entry.getValue().getSize());
      long endTimeInMs = System.currentTimeMillis();
      BlobIndexValue value = findKey(id);
      if (value == null)
        System.out.println("error: value is null");
      if (enableVerboseLogging)
        System.out.println("Time taken to add to the index in Ms - " + (endTimeInMs - startTimeInMs));
      totalTimeTaken.addAndGet(endTimeInMs - startTimeInMs);
      try {
        if (writer != null)
          writer.write("blobid-" + datadir + "-" + id + "\n");
      }
      catch (Exception e) {
        System.out.println("error while logging");
      }
    }
    totalWrites.incrementAndGet();
    if (totalWrites.get() % 1000 == 0)
      System.out.println("number of indexes created " + indexes.size());
  }

  public void AddToIndexRandomData(List<BlobId> ids) throws StoreException {
    ArrayList<BlobIndexEntry> list = new ArrayList<BlobIndexEntry>(ids.size());
    for (int i = 0; i < list.size(); i++) {
      BlobIndexEntry entry = new BlobIndexEntry(ids.get(i), new BlobIndexValue(1000, 1000, (byte)1, 1000));
      list.add(entry);
      try {
        if (writer != null)
          writer.write("blobid-" + datadir + "-" + ids.get(i) + "\n");
      }
      catch (Exception e) {
        System.out.println("error while logging");
      }
    }
    long startTimeInMs = System.currentTimeMillis();
    synchronized (lock) {
      addToIndex(list, getCurrentEndOffset() + 1000);
    }
    long endTimeInMs = System.currentTimeMillis();
    System.out.println("Time taken to add to the index all the entries - " + (endTimeInMs - startTimeInMs));

  }

  public boolean exist(StoreKey key) throws StoreException {
    System.out.println("data dir " + datadir + " searching id " + key.toString());
    long startTimeMs = System.currentTimeMillis();
    boolean exist = super.exist(key);
    long endTimeMs = System.currentTimeMillis();
    System.out.println("Time to find an entry exist - " + (endTimeMs - startTimeMs));
    totalTimeTaken.addAndGet(endTimeMs - startTimeMs);
    totalReads.incrementAndGet();
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
