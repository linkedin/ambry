package com.github.ambry.store;

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

  public BlobStore(String dataDir, Scheduler scheduler) throws IOException, IndexCreationException {
    this.dataDir = dataDir;
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
      logger.error("Error while starting store for directory {}",dataDir);
      throw new StoreException("Error while starting store for dir " + dataDir, StoreErrorCodes.Initialization_Error);
    }
  }

  @Override
  public MessageReadSet get(List<? extends StoreKey> ids) throws StoreException {
    try {
      List<BlobReadOptions> readOptions = new ArrayList<BlobReadOptions>();
      for (StoreKey key : ids) {
        BlobReadOptions readInfo = index.getBlobReadInfo(key);
        readOptions.add(readInfo);
      }
      return log.getView(readOptions);
    }
    catch (IOException e) {
      throw new StoreException("io error while trying to fetch blobs : " + e, StoreErrorCodes.IOError);
    }
  }

  @Override
  public void put(MessageWriteSet messageSetToWrite) throws StoreException {
    synchronized (lock) {
      try {
        long writeStartOffset = log.getLogEndOffset();
        messageSetToWrite.writeTo(log);
        List<MessageInfo> messageInfo = messageSetToWrite.getMessageSetInfo();
        ArrayList<BlobIndexEntry> indexEntries = new ArrayList<BlobIndexEntry>(messageInfo.size());
        for (MessageInfo info : messageInfo) {
          BlobIndexValue value = new BlobIndexValue(info.getSize(), writeStartOffset, (byte)0, info.getTTL());
          BlobIndexEntry entry = new BlobIndexEntry(info.getKey(), value);
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
          index.markAsDeleted(info.getKey(), log.getLogEndOffset());
        }
      }
      catch (IOException e) {
        throw new StoreException("io error while trying to fetch blobs : " + e, StoreErrorCodes.IOError);
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
