package com.github.ambry.store;

import com.codahale.metrics.Timer;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.utils.FileLock;
import com.github.ambry.utils.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.codahale.metrics.MetricRegistry;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.io.IOException;

/**
 * The blob store that controls the log and index
 */
public class BlobStore implements Store {

  private Log log;
  private BlobPersistentIndex index;
  private final String dataDir;
  private final Scheduler scheduler;
  private Logger logger = LoggerFactory.getLogger(getClass());
  /* A lock that prevents concurrent writes to the log */
  private Object lock = new Object();
  private boolean started;
  private StoreConfig config;
  private long capacityInBytes;
  private static String LockFile = ".lock";
  private FileLock fileLock;
  private StoreKeyFactory factory;
  private MessageStoreRecovery recovery;
  private StoreMetrics metrics;


  public BlobStore(StoreConfig config,
                   Scheduler scheduler,
                   MetricRegistry registry,
                   String dataDir,
                   long capacityInBytes,
                   StoreKeyFactory factory,
                   MessageStoreRecovery recovery) {
    this.metrics = new StoreMetrics(dataDir, registry);
    this.dataDir = dataDir;
    this.scheduler = scheduler;
    this.config = config;
    this.capacityInBytes = capacityInBytes;
    this.factory = factory;
    this.recovery = recovery;
  }

  @Override
  public void start() throws StoreException {
    synchronized (lock) {
      if (started)
        throw new StoreException("Store already started", StoreErrorCodes.Store_Already_Started);
      final Timer.Context context = metrics.storeStartTime.time();
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
        log = new Log(dataDir, capacityInBytes, metrics);
        index = new BlobPersistentIndex(dataDir, scheduler, log, config, factory, recovery, metrics);
        // set the log end offset to the recovered offset from the index after initializing it
        log.setLogEndOffset(index.getCurrentEndOffset());
        metrics.initializeCapacityUsedMetric(log);
        started = true;
      }
      catch (Exception e) {
        logger.error("Error while starting store for directory " + dataDir, e);
        throw new StoreException("Error while starting store for dir " + dataDir, e, StoreErrorCodes.Initialization_Error);
      }
      finally {
        context.stop();
      }
    }
  }

  @Override
  public StoreInfo get(List<? extends StoreKey> ids) throws StoreException {
    // allows concurrent gets
    final Timer.Context context = metrics.getResponse.time();
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
      return new StoreInfo(readSet, messageInfo);
    }
    catch (IOException e) {
      throw new StoreException("io error while trying to fetch blobs : ", e, StoreErrorCodes.IOError);
    }
    finally {
      context.stop();
    }
  }

  @Override
  public void put(MessageWriteSet messageSetToWrite) throws StoreException {
    synchronized (lock) {
      final Timer.Context context = metrics.putResponse.time();
      checkStarted();
      try {
        if (messageSetToWrite.getMessageSetInfo().size() == 0)
          throw new IllegalArgumentException("Message write set cannot be empty");
        // if any of the keys alreadys exist in the store, we fail
        for (MessageInfo info : messageSetToWrite.getMessageSetInfo()) {
          if (index.exists(info.getStoreKey())) {
            throw new StoreException("key already exist in store. cannot be overwritten", StoreErrorCodes.Already_Exist);
          }
        }
        long writeStartOffset = log.getLogEndOffset();
        messageSetToWrite.writeTo(log);
        List<MessageInfo> messageInfo = messageSetToWrite.getMessageSetInfo();
        ArrayList<BlobIndexEntry> indexEntries = new ArrayList<BlobIndexEntry>(messageInfo.size());
        for (MessageInfo info : messageInfo) {
          BlobIndexValue value = new BlobIndexValue(info.getSize(),
                                                    writeStartOffset,
                                                    (byte)0,
                                                    info.getTimeToLiveInMs());
          BlobIndexEntry entry = new BlobIndexEntry(info.getStoreKey(), value);
          indexEntries.add(entry) ;
          writeStartOffset += info.getSize();
        }
        FileSpan fileSpan = new FileSpan(log.getLogEndOffset() - messageInfo.get(0).getSize(), log.getLogEndOffset());
        index.addToIndex(indexEntries, fileSpan);
      }
      catch (IOException e) {
        throw new StoreException("io error while trying to fetch blobs : ", e, StoreErrorCodes.IOError);
      }
      finally {
        context.stop();
      }
    }
  }

  @Override
  public void delete(MessageWriteSet messageSetToDelete) throws StoreException {
    synchronized (lock) {
      final Timer.Context context = metrics.deleteResponse.time();
      checkStarted();
      try {
        messageSetToDelete.writeTo(log);
        List<MessageInfo> infoList = messageSetToDelete.getMessageSetInfo();
        for (MessageInfo info : infoList) {
          FileSpan fileSpan = new FileSpan(log.getLogEndOffset() - info.getSize(), log.getLogEndOffset());
          index.markAsDeleted(info.getStoreKey(), fileSpan);
        }
      }
      catch (IOException e) {
        throw new StoreException("io error while trying to delete blobs : " + e, StoreErrorCodes.IOError);
      }
      finally {
        context.stop();
      }
    }
  }

  @Override
  public void updateTTL(MessageWriteSet messageSetToUpdateTTL) throws StoreException {
    synchronized (lock) {
      final Timer.Context context = metrics.ttlResponse.time();
      checkStarted();
      try {
        messageSetToUpdateTTL.writeTo(log);
        List<MessageInfo> infoList = messageSetToUpdateTTL.getMessageSetInfo();
        for (MessageInfo info : infoList) {
          FileSpan fileSpan = new FileSpan(log.getLogEndOffset() - info.getSize(), log.getLogEndOffset());
          index.updateTTL(info.getStoreKey(), info.getTimeToLiveInMs(), fileSpan);
        }
      }
      catch (IOException e) {
        throw new StoreException("io error while trying to update ttl for blobs : " + e, StoreErrorCodes.IOError);
      }
      finally {
        context.stop();
      }
    }
  }

  @Override
  public FindInfo findEntriesSince(FindToken token, long maxTotalSizeOfEntries) throws StoreException {
    // TODO add metrics
    return index.findEntriesSince(token, maxTotalSizeOfEntries);
  }

  @Override
  public Set<StoreKey> findMissingKeys(List<StoreKey> keys) throws StoreException {
    // TODO add metrics
    return index.findMissingKeys(keys);
  }

  @Override
  public boolean isKeyDeleted(StoreKey key) throws StoreException {
    // TODO add metrics
    BlobIndexValue value = index.findKey(key);
    if (value == null)
      throw new StoreException("Key " + key + " not found in store. Cannot check if it is deleted",
                               StoreErrorCodes.ID_Not_Found);
    return value.isFlagSet(BlobIndexValue.Flags.Delete_Index);
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
        logger.error("Shutdown of store failed for directory " + dataDir, e);
      }
      finally {
        try {
          fileLock.destroy();
        }
        catch (IOException e) {
          logger.error("IO Exception while trying to close the file lock", e);
        }
      }
    }
  }

  private void checkStarted() throws StoreException {
    if (!started)
      throw new StoreException("Store not started", StoreErrorCodes.Store_Not_Started);
  }
}
