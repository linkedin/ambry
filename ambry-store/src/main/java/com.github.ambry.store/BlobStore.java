package com.github.ambry.store;

import com.github.ambry.config.StoreConfig;
import com.github.ambry.metrics.ReadableMetricsRegistry;
import com.github.ambry.utils.FileLock;
import com.github.ambry.utils.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.io.IOException;

/**
 * The blob store that controls the log and index
 */
public class BlobStore implements Store {

  private Log log;
  private BlobPersistantIndex index;
  private final String dataDir;
  private final Scheduler scheduler;
  private Logger logger = LoggerFactory.getLogger(getClass());
  /* A lock that prevents concurrent writes to the log */
  private Object lock = new Object();
  private final StoreMetrics metrics;
  private boolean started;
  private StoreConfig config;
  private long capacityGB;
  private static String LockFile = ".lock";
  private FileLock fileLock;
  private StoreKeyFactory factory;

  public BlobStore(StoreConfig config,
                   Scheduler scheduler,
                   ReadableMetricsRegistry registry,
                   String dataDir,
                   long capacityGB,
                   StoreKeyFactory factory) {
    this.dataDir = dataDir;
    this.scheduler = scheduler;
    metrics = new StoreMetrics(this.dataDir, registry);
    this.config = config;
    this.capacityGB = capacityGB;
    this.factory = factory;
  }

  @Override
  public void start() throws StoreException {
    if (started)
      throw new StoreException("Store already started", StoreErrorCodes.Store_Already_Started);
    try {
      // Check if the data dir exist. If it does not exist, create it
      File dataFile = new File(dataDir);
      if (!dataFile.exists()) {
        logger.info("data directory {} not found. creating it",  dataDir);
        boolean created = dataFile.mkdir();
        if (!created)
          throw new StoreException("Failed to create directory for data dir " + dataDir,
                                   StoreErrorCodes.Initialization_Error);
      }
      if(!dataFile.isDirectory() || !dataFile.canRead())
        throw new StoreException(dataFile.getAbsolutePath() + " is either not a directory or is not readable",
                                 StoreErrorCodes.Initialization_Error);

      // lock the directory
      fileLock = new FileLock(new File(dataDir, LockFile));
      if(!fileLock.tryLock())
        throw new StoreException("Failed to acquire lock on file " + dataDir +
                                 ". Another process or thread is using this directory.",
                                 StoreErrorCodes.Initialization_Error);
      log = new Log(dataDir, metrics, capacityGB);
      index = new BlobPersistantIndex(dataDir, scheduler, log, config, factory);
      // set the log end offset to the recovered offset from the index after initializing it
      log.setLogEndOffset(index.getCurrentEndOffset());
      started = true;
    }
    catch (Exception e) {
      logger.error("Error while starting store for directory {} with exception {}",dataDir, e);
      throw new StoreException("Error while starting store for dir " + dataDir, StoreErrorCodes.Initialization_Error);
    }
  }

  @Override
  public StoreInfo get(List<? extends StoreKey> ids) throws StoreException {
    try {
      checkStarted();
      List<BlobReadOptions> readOptions = new ArrayList<BlobReadOptions>(ids.size());
      List<MessageInfo> messageInfo = new ArrayList<MessageInfo>(ids.size());
      for (StoreKey key : ids) {
        BlobReadOptions readInfo = index.getBlobReadInfo(key);
        readOptions.add(readInfo);
        messageInfo.add(new MessageInfo(key, readInfo.getSize(), readInfo.getTTL()));
      }
      MessageReadSet readSet = log.getView(readOptions);
      metrics.reads.inc(1);
      return new StoreInfo(readSet, messageInfo);
    }
    catch (IOException e) {
      throw new StoreException("io error while trying to fetch blobs : " + e, StoreErrorCodes.IOError);
    }
  }

  @Override
  public void put(MessageWriteSet messageSetToWrite) throws StoreException {
    synchronized (lock) {
      checkStarted();
      try {
        // if any of the keys alreadys exist in the store, we fail
        for (MessageInfo info : messageSetToWrite.getMessageSetInfo()) {
          if (index.exist(info.getStoreKey())) {
            throw new StoreException("key {} already exist in store. cannot be overwritten", StoreErrorCodes.Already_Exist);
          }
        }
        long writeStartOffset = log.getLogEndOffset();
        messageSetToWrite.writeTo(log);
        List<MessageInfo> messageInfo = messageSetToWrite.getMessageSetInfo();
        ArrayList<BlobIndexEntry> indexEntries = new ArrayList<BlobIndexEntry>(messageInfo.size());
        for (MessageInfo info : messageInfo) {
          BlobIndexValue value = new BlobIndexValue(info.getSize(),
                                                                        writeStartOffset,
                                                                        (byte)0, info.getTimeToLiveInMs());
          BlobIndexEntry entry = new BlobIndexEntry(info.getStoreKey(), value);
          indexEntries.add(entry) ;
          writeStartOffset += info.getSize();
        }
        index.addToIndex(indexEntries, log.getLogEndOffset());
        metrics.writes.inc(1);
      }
      catch (IOException e) {
        throw new StoreException("io error while trying to fetch blobs : " + e, StoreErrorCodes.IOError);
      }
    }
  }

  @Override
  public void delete(MessageWriteSet messageSetToDelete) throws StoreException {
    synchronized (lock) {
      checkStarted();
      try {
        messageSetToDelete.writeTo(log);
        List<MessageInfo> infoList = messageSetToDelete.getMessageSetInfo();
        for (MessageInfo info : infoList) {
          index.markAsDeleted(info.getStoreKey(), log.getLogEndOffset());
        }
        metrics.deletes.inc(1);
      }
      catch (IOException e) {
        throw new StoreException("io error while trying to delete blobs : " + e, StoreErrorCodes.IOError);
      }
    }
  }

  @Override
  public void updateTTL(MessageWriteSet messageSetToUpdateTTL) throws StoreException {
    synchronized (lock) {
      checkStarted();
      try {
        messageSetToUpdateTTL.writeTo(log);
        List<MessageInfo> infoList = messageSetToUpdateTTL.getMessageSetInfo();
        for (MessageInfo info : infoList) {
          index.updateTTL(info.getStoreKey(), info.getTimeToLiveInMs(), log.getLogEndOffset());
        }
      }
      catch (IOException e) {
        throw new StoreException("io error while trying to update ttl for blobs : " + e, StoreErrorCodes.IOError);
      }
    }
  }

  @Override
  public void shutdown() throws StoreException {
    synchronized (lock) {
      checkStarted();
      try {
        index.close();
        log.close();
        started = false;
      }
      catch (Exception e) {
        logger.error("Shutdown of store failed for directory {}", dataDir);
      }
      finally {
        try {
          fileLock.destroy();
        }
        catch (IOException e) {
          logger.error("IO Exception while trying to close the file lock {}", e);
        }
      }
    }
  }

  private void checkStarted() throws StoreException {
    if (!started)
      throw new StoreException("Store not started", StoreErrorCodes.Store_Not_Started);
  }
}
