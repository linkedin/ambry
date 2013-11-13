package com.github.ambry.store;

import com.github.ambry.config.StoreConfig;
import com.github.ambry.utils.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.io.IOException;

/**
 * The blob store that controls the log and index
 */
public class BlobStore implements Store {

  private Log log;
  private BlobIndex index;
  private final String dataDir;
  private final Scheduler scheduler;
  private Logger logger = LoggerFactory.getLogger(getClass());
  /* A lock that prevents concurrent writes to the log */
  private Object lock = new Object();

  public BlobStore(StoreConfig config, Scheduler scheduler) {
    this.dataDir = config.storeDataDir;
    this.scheduler = scheduler;
  }

  @Override
  public void start() throws StoreException {
    try {
      log = new Log(dataDir);
      index = new BlobIndex(dataDir, scheduler, log);
      // set the log end offset to the recovered offset from the index after initializing it
      log.setLogEndOffset(index.getCurrentEndOffset());
    }
    catch (Exception e) {
      logger.error("Error while starting store for directory {} with exception {}",dataDir, e);
      throw new StoreException("Error while starting store for dir " + dataDir, StoreErrorCodes.Initialization_Error);
    }
  }

  @Override
  public StoreInfo get(List<? extends StoreKey> ids) throws StoreException {
    try {
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
      throw new StoreException("io error while trying to fetch blobs : " + e, StoreErrorCodes.IOError);
    }
  }

  @Override
  public void put(MessageWriteSet messageSetToWrite) throws StoreException {
    synchronized (lock) {
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
        ArrayList<BlobIndex.BlobIndexEntry> indexEntries = new ArrayList<BlobIndex.BlobIndexEntry>(messageInfo.size());
        for (MessageInfo info : messageInfo) {
          BlobIndex.BlobIndexValue value = new BlobIndex.BlobIndexValue(info.getSize(), writeStartOffset, (byte)0, info.getTimeToLive());
          BlobIndex.BlobIndexEntry entry = new BlobIndex.BlobIndexEntry(info.getStoreKey(), value);
          indexEntries.add(entry) ;
          writeStartOffset += info.getSize();
        }
        index.AddToIndex(indexEntries, log.getLogEndOffset());
      }
      catch (IOException e) {
        throw new StoreException("io error while trying to fetch blobs : " + e, StoreErrorCodes.IOError);
      }
    }
  }

  @Override
  public void delete(MessageWriteSet messageSetToDelete) throws StoreException {
    synchronized (lock) {
      try {
        messageSetToDelete.writeTo(log);
        List<MessageInfo> infoList = messageSetToDelete.getMessageSetInfo();
        for (MessageInfo info : infoList) {
          index.markAsDeleted(info.getStoreKey(), log.getLogEndOffset());
        }
      }
      catch (IOException e) {
        throw new StoreException("io error while trying to delete blobs : " + e, StoreErrorCodes.IOError);
      }
    }
  }

  @Override
  public void updateTTL(MessageWriteSet messageSetToUpdateTTL) throws StoreException {
    synchronized (lock) {
      try {
        messageSetToUpdateTTL.writeTo(log);
        List<MessageInfo> infoList = messageSetToUpdateTTL.getMessageSetInfo();
        for (MessageInfo info : infoList) {
          index.updateTTL(info.getStoreKey(), info.getTimeToLive(), log.getLogEndOffset());
        }
      }
      catch (IOException e) {
        throw new StoreException("io error while trying to update ttl for blobs : " + e, StoreErrorCodes.IOError);
      }
    }
  }

  @Override
  public void shutdown() {
    synchronized (lock) {
      try {
        index.close();
        log.close();
      }
      catch (Exception e) {
        logger.error("Shutdown of store failed for directory {}", dataDir);
      }
    }
  }
}
