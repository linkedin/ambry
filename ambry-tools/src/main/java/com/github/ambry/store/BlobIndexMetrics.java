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
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.messageformat.BlobStoreHardDelete;
import com.github.ambry.messageformat.BlobStoreRecovery;
import com.github.ambry.utils.SystemTime;
import java.io.FileWriter;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;


class BlobIndexMetrics extends PersistentIndex {
  private Object lock = new Object();
  private boolean enableVerboseLogging;
  private AtomicLong totalWrites;
  private AtomicLong totalTimeTaken;
  private AtomicLong lastOffsetUsed;
  private AtomicLong totalReads;
  private FileWriter writer;
  private String datadir;

  public BlobIndexMetrics(String datadir, ScheduledExecutorService scheduler, Log log, boolean enableVerboseLogging,
      AtomicLong totalWrites, AtomicLong totalTimeTaken, AtomicLong totalReads, StoreConfig config, FileWriter writer,
      StoreKeyFactory factory) throws StoreException {
    super(datadir, datadir, scheduler, log, config, factory, new BlobStoreRecovery(), new BlobStoreHardDelete(),
        new DiskIOScheduler(null), new StoreMetrics(new MetricRegistry()), SystemTime.getInstance(), UUID.randomUUID(),
        null);
    this.enableVerboseLogging = enableVerboseLogging;
    this.lastOffsetUsed = new AtomicLong(0);
    this.totalWrites = totalWrites;
    this.totalTimeTaken = totalTimeTaken;
    this.totalReads = totalReads;
    this.writer = writer;
    this.datadir = datadir;
  }

  public void addToIndexRandomData(BlobId id) throws StoreException {

    synchronized (lock) {
      long startTimeInMs = System.currentTimeMillis();
      long size = new Random().nextInt(10000);
      IndexEntry entry = new IndexEntry(id,
          new IndexValue(size, new Offset("", lastOffsetUsed.get()), (byte) 1, 1000, SystemTime.getInstance().seconds(),
              Account.UNKNOWN_ACCOUNT_ID, Container.UNKNOWN_CONTAINER_ID, (short) 0));
      lastOffsetUsed.addAndGet(size);
      long offset = getCurrentEndOffset().getOffset();
      addToIndex(entry, new FileSpan(new Offset("", offset), new Offset("", offset + entry.getValue().getSize())));
      long endTimeInMs = System.currentTimeMillis();
      IndexValue value = findKey(id);
      if (value == null) {
        System.out.println("error: value is null");
      }
      if (enableVerboseLogging) {
        System.out.println("Time taken to add to the index in Ms - " + (endTimeInMs - startTimeInMs));
      }
      totalTimeTaken.addAndGet(endTimeInMs - startTimeInMs);
      try {
        if (writer != null) {
          writer.write("blobid-" + datadir + "-" + id + "\n");
        }
      } catch (Exception e) {
        System.out.println("error while logging");
      }
    }
    totalWrites.incrementAndGet();
    if (totalWrites.get() % 1000 == 0) {
      System.out.println("number of indexes created " + getIndexSegments().size());
    }
  }
}
