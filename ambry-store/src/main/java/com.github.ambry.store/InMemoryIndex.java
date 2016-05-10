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

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.utils.CrcInputStream;
import com.github.ambry.utils.CrcOutputStream;
import com.github.ambry.utils.Scheduler;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


/**
 * An in memory index implementation that is responsible for adding and modifying index entries,
 * recovering an index from the log and commit and recover index to disk . This class
 * is not thread safe and expects the caller to do appropriate synchronization.
 */
public class InMemoryIndex {
  protected ConcurrentHashMap<StoreKey, IndexValue> index = new ConcurrentHashMap<StoreKey, IndexValue>();
  protected Scheduler scheduler;
  private AtomicLong logEndOffset;
  private File indexFile;
  private static final String indexFileName = "index_current";
  private static final String Clean_Shutdown_Filename = "cleanshutdown";
  private IndexPersistor persistor;
  private StoreKeyFactory factory;
  private InMemoryJournal journal;
  private Log log;
  private UUID sessionId;
  private boolean cleanShutdown;
  private long logEndOffsetOnStartup;
  private Logger logger = LoggerFactory.getLogger(getClass());
  private Time time;
  public static final Short version = 0;

  // metrics
  private final Timer recoveryTime;
  private final Timer indexFlushTime;
  private final Counter nonzeroMessageRecovery;

  /**
   * Reads the index from disk, performs recovery if required and starts background
   * task to schedule index persistence
   * @param datadir The data directory to use to store the index
   * @param scheduler The scheduler that runs regular background tasks
   * @param log The log that is represented by this index
   * @param config The store configs for this index
   * @param factory The factory used to create store keys
   * @param recovery The recovery handle to perform recovery on startup
   * @throws StoreException
   */
  public InMemoryIndex(String datadir, Scheduler scheduler, Log log, StoreConfig config, StoreKeyFactory factory,
      MessageStoreRecovery recovery, MetricRegistry registry, Time time)
      throws StoreException {
    try {
      this.recoveryTime = registry.timer(MetricRegistry.name(InMemoryIndex.class, "indexRecoveryTime"));
      this.indexFlushTime = registry.timer(MetricRegistry.name(InMemoryIndex.class, "indexFlushTime"));
      this.nonzeroMessageRecovery =
          registry.counter(MetricRegistry.name(InMemoryIndex.class, "nonZeroMessageRecovery"));
      this.journal = new InMemoryJournal(datadir, config.storeIndexMaxNumberOfInmemElements,
          config.storeMaxNumberOfEntriesToReturnFromJournal);
      logEndOffset = new AtomicLong(0);
      this.time=time;
      this.log = log;
      this.factory = factory;
      // check if file exist and recover from it
      indexFile = new File(datadir, indexFileName);
      persistor = new IndexPersistor();
      if (indexFile.exists()) {
        try {
          persistor.read();
        } catch (StoreException e) {
          if (e.getErrorCode() == StoreErrorCodes.Index_Creation_Failure
              || e.getErrorCode() == StoreErrorCodes.Index_Version_Error) {
            // we just log the error here and retain the index so far created.
            // subsequent recovery process will add the missed out entries
            logger.error("Error while reading from index {}", e);
          } else {
            throw e;
          }
        }
      }
      // do recovery
      final Timer.Context context = recoveryTime.time();
      List<MessageInfo> messagesRecovered = recovery.recover(log, logEndOffset.get(), log.sizeInBytes(), factory);
      if (messagesRecovered.size() > 0) {
        nonzeroMessageRecovery.inc(1);
      }
      long runningOffset = logEndOffset.get();
      // iterate through the recovered messages and restore the state of the index
      for (MessageInfo info : messagesRecovered) {
        IndexValue value = index.get(info.getStoreKey());
        // if the key already exist, update the delete state or ttl value if required
        if (value != null) {
          logger.info("Message already exists with key {}", info.getStoreKey());
          verifyFileEndOffset(new FileSpan(logEndOffset.get(), logEndOffset.get() + info.getSize()));
          if (info.isDeleted()) {
            FileSpan fileSpan = new FileSpan(logEndOffset.get(), logEndOffset.get() + info.getSize());
            markAsDeleted(info.getStoreKey(), fileSpan);
          } else {
            throw new StoreException("Illegal message state during restore. ", StoreErrorCodes.Initialization_Error);
          }
          logger.info("Updated message with key {} size {} ttl {} deleted {}", info.getStoreKey(), value.getSize(),
              value.getTimeToLiveInMs(), info.isDeleted());
        } else {
          // add a new entry to the index
          IndexValue newValue = new IndexValue(info.getSize(), runningOffset, info.getExpirationTimeInMs());
          verifyFileEndOffset(new FileSpan(logEndOffset.get(), logEndOffset.get() + info.getSize()));
          FileSpan fileSpan = new FileSpan(logEndOffset.get(), logEndOffset.get() + info.getSize());
          addToIndex(new IndexEntry(info.getStoreKey(), newValue), fileSpan);
          logger.info("Adding new message to index with key {} size {} ttl {} deleted {}", info.getStoreKey(),
              info.getSize(), info.getExpirationTimeInMs(), info.isDeleted());
        }
        runningOffset += info.getSize();
      }
      log.setLogEndOffset(runningOffset);
      logEndOffsetOnStartup = log.getLogEndOffset();
      context.stop();
      logger.info("read index from file {}", datadir);
      this.sessionId = UUID.randomUUID();
      // delete the shutdown file
      File cleanShutdownFile = new File(datadir, Clean_Shutdown_Filename);
      if (cleanShutdownFile.exists()) {
        cleanShutdown = true;
        cleanShutdownFile.delete();
      }

      // start scheduler thread to persist index in the background
      this.scheduler = scheduler;
      this.scheduler.schedule("index persistor", persistor,
          config.storeDataFlushDelaySeconds + new Random().nextInt(SystemTime.SecsPerMin),
          config.storeDataFlushIntervalSeconds, TimeUnit.SECONDS);
    } catch (Exception e) {
      throw new StoreException("Error while creating index", e, StoreErrorCodes.Index_Creation_Failure);
    }
  }

  /**
   * Adds a new entry to the index
   * @param entry The entry to be added to the index
   * @param fileSpan The file span that this entry represents in the log
   * @throws StoreException
   */
  public void addToIndex(IndexEntry entry, FileSpan fileSpan) {
    verifyFileEndOffset(fileSpan);
    index.put(entry.getKey(), entry.getValue());
    this.logEndOffset.set(fileSpan.getEndOffset());
    journal.addEntry(entry.getValue().getOffset(), entry.getKey());
  }

  /**
   * Adds a set of entries to the index
   * @param entries The entries to be added to the index
   * @param fileSpan The file span that the entries represent in the log
   * @throws StoreException
   */
  public void addToIndex(ArrayList<IndexEntry> entries, FileSpan fileSpan) {
    verifyFileEndOffset(fileSpan);
    for (IndexEntry entry : entries) {
      index.put(entry.getKey(), entry.getValue());
      journal.addEntry(entry.getValue().getOffset(), entry.getKey());
    }
    this.logEndOffset.set(fileSpan.getEndOffset());
  }

  /**
   * Indicates if a key is present in the index
   * @param key The key to do the exist check against
   * @return True, if the key exist in the index. False, otherwise.
   * @throws StoreException
   */
  public boolean exists(StoreKey key) {
    return index.containsKey(key);
  }

  /**
   * Marks the index entry represented by the key for delete
   * @param id The id of the entry that needs to be deleted
   * @param fileSpan The file range represented by this entry in the log
   * @throws StoreException
   */
  public void markAsDeleted(StoreKey id, FileSpan fileSpan)
      throws StoreException {
    verifyFileEndOffset(fileSpan);
    IndexValue value = index.get(id);
    if (value == null) {
      logger.error("id {} not present in index. marking id as deleted failed", id);
      throw new StoreException("id not present in index : " + id, StoreErrorCodes.ID_Not_Found);
    }
    value.setFlag(IndexValue.Flags.Delete_Index);
    value.setNewOffset(fileSpan.getStartOffset());
    value.setNewSize(fileSpan.getEndOffset() - fileSpan.getStartOffset());
    index.put(id, value);
    this.logEndOffset.set(fileSpan.getEndOffset());
    journal.addEntry(fileSpan.getStartOffset(), id);
  }

  /**
   * Returns the blob read info for a given key
   * @param id The id of the entry whose info is required
   * @return The blob read info that contains the information for the given key
   * @throws StoreException
   */
  public BlobReadOptions getBlobReadInfo(StoreKey id)
      throws StoreException {
    IndexValue value = index.get(id);
    if (value == null) {
      logger.error("id {} not present in index. cannot find blob", id);
      throw new StoreException("id not present in index " + id, StoreErrorCodes.ID_Not_Found);
    } else if (value.isFlagSet(IndexValue.Flags.Delete_Index)) {
      logger.error("id {} has been deleted", id);
      throw new StoreException("id has been deleted in index " + id, StoreErrorCodes.ID_Deleted);
    } else if (isExpired(value)) {
      logger.error("id {} has expired ttl {}", id, value.getTimeToLiveInMs());
      throw new StoreException("id not present in index " + id, StoreErrorCodes.TTL_Expired);
    }
    return new BlobReadOptions(value.getOffset(), value.getSize(), value.getTimeToLiveInMs(), id);
  }

  private boolean isExpired(IndexValue value){
    return value.getTimeToLiveInMs() != Utils.Infinite_Time && time.milliseconds() > value.getTimeToLiveInMs();
  }

  /**
   * Returns the list of keys that are not found in the index from the given input keys
   * @param keys The list of keys that needs to be tested against the index
   * @return The list of keys that are not found in the index
   * @throws StoreException
   */
  public Set<StoreKey> findMissingKeys(List<StoreKey> keys) {
    Set<StoreKey> missingKeys = new HashSet<StoreKey>();
    for (StoreKey key : keys) {
      if (!exists(key)) {
        missingKeys.add(key);
      }
    }
    return missingKeys;
  }

  /**
   * Finds all the entries from the given start token(inclusive). The token defines the start position in the index from
   * where entries needs to be fetched
   * @param token The token that signifies the start position in the index from where entries need to be retrieved
   * @param maxTotalSizeOfEntries The maximum total size of entries that needs to be returned. The api will try to
   *                              return a list of entries whose total size is close to this value. The size can exceed
   *                              in cases where summing a blob could exceed the max value.
   * @return The FindInfo state that contains both the list of entries and the new findtoken to start the next iteration
   */
  public FindInfo findEntriesSince(FindToken token, long maxTotalSizeOfEntries)
      throws StoreException {
    long logEndOffsetBeforeFind = log.getLogEndOffset();
    StoreFindToken storeToken = (StoreFindToken) token;
    // validate token
    if (storeToken.getSessionId() == null || storeToken.getSessionId().compareTo(sessionId) != 0) {
      // the session has changed. check if we had an unclean shutdown on startup
      if (!cleanShutdown) {
        // if we had an unclean shutdown and the token offset is larger than the logEndOffsetOnStartup
        // we reset the token to logEndOffsetOnStartup
        if (storeToken.getOffset() > logEndOffsetOnStartup) {
          storeToken = new StoreFindToken(logEndOffsetOnStartup, sessionId);
        }
      } else if (storeToken.getOffset() > logEndOffsetOnStartup) {
        logger.error("Invalid token. Provided offset is outside the log range after clean shutdown");
        // if the shutdown was clean, the offset should always be lesser or equal to the logEndOffsetOnStartup
        throw new IllegalArgumentException(
            "Invalid token. Provided offset is outside the log range after clean shutdown");
      }
    }
    boolean inclusive = false;
    long startOffset = storeToken.getOffset();
    if (storeToken.getOffset() == -1) {
      startOffset = 0;
      inclusive = true;
    }
    List<JournalEntry> entries = journal.getEntriesSince(startOffset, inclusive);
    List<MessageInfo> messageEntries = new ArrayList<MessageInfo>();
    if (entries == null) {
      // read the entire index and return it
      if (index.entrySet().size() > 0) {
        long largestOffset = 0;
        long lastEntrySize = 0;
        for (Map.Entry<StoreKey, IndexValue> entry : index.entrySet()) {
          messageEntries.add(new MessageInfo(entry.getKey(), entry.getValue().getSize(),
              entry.getValue().isFlagSet(IndexValue.Flags.Delete_Index), entry.getValue().getTimeToLiveInMs()));
          if (entry.getValue().getOffset() > largestOffset) {
            largestOffset = entry.getValue().getOffset();
            lastEntrySize = entry.getValue().getSize();
          }
        }
        StoreFindToken storeFindToken = new StoreFindToken(largestOffset, sessionId);
        storeFindToken.setBytesRead(largestOffset + lastEntrySize);
        return new FindInfo(messageEntries, storeFindToken);
      } else {
        storeToken.setBytesRead(logEndOffsetBeforeFind);
        return new FindInfo(messageEntries, storeToken);
      }
    } else {
      long endOffset = storeToken.getOffset();
      long currentTotalSize = 0;
      long lastEntrySize = 0;
      for (JournalEntry entry : entries) {
        IndexValue value = index.get(entry.getKey());
        messageEntries.add(
            new MessageInfo(entry.getKey(), value.getSize(), value.isFlagSet(IndexValue.Flags.Delete_Index),
                value.getTimeToLiveInMs()));
        endOffset = entry.getOffset();
        currentTotalSize += value.getSize();
        lastEntrySize = value.getSize();
        if (currentTotalSize >= maxTotalSizeOfEntries) {
          break;
        }
      }
      eliminateDuplicates(messageEntries);
      if (messageEntries.size() > 0) {
        // if we have messageEntries, then the total bytes read is sum of endOffset and the size of the last message entry
        StoreFindToken storeFindToken = new StoreFindToken(endOffset, sessionId);
        storeFindToken.setBytesRead(endOffset + lastEntrySize);
        return new FindInfo(messageEntries, storeFindToken);
      } else {
        // if there are no messageEntries, total bytes read is equivalent to the logEndOffsetBeforeFind
        StoreFindToken storeFindToken = new StoreFindToken(endOffset, sessionId);
        storeFindToken.setBytesRead(logEndOffsetBeforeFind);
        return new FindInfo(messageEntries, new StoreFindToken(endOffset, sessionId));
      }
    }
  }

  /**
   * We can have duplicate entries in the message entries since updates can happen to the same key. For example,
   * insert a key followed by a delete. This would create two entries in the journal. A single findInfo
   * could read both the entries. The findInfo should return as clean information as possible. This method removes
   * the oldest duplicate in the list.
   * @param messageEntries The message entry list where duplicates need to be removed
   */
  private void eliminateDuplicates(List<MessageInfo> messageEntries) {
    Set<StoreKey> setToFindDuplicate = new HashSet<StoreKey>();
    ListIterator<MessageInfo> messageEntriesIterator = messageEntries.listIterator(messageEntries.size());
    while (messageEntriesIterator.hasPrevious()) {
      MessageInfo messageInfo = messageEntriesIterator.previous();
      if (setToFindDuplicate.contains(messageInfo.getStoreKey())) {
        messageEntriesIterator.remove();
      } else {
        setToFindDuplicate.add(messageInfo.getStoreKey());
      }
    }
  }

  /**
   * Closes the index
   * @throws StoreException
   */
  public void close()
      throws StoreException, IOException {
    persistor.write();
    File cleanShutdownFile = new File(indexFile.getAbsolutePath(), Clean_Shutdown_Filename);
    try {
      cleanShutdownFile.createNewFile();
    } catch (IOException e) {
      logger.error("Index " + indexFile.getAbsolutePath() + " error while creating clean shutdown file ", e);
    }
  }

  /**
   * Returns the current end offset that the index represents in the log
   * @return The end offset in the log that this index currently represents
   */
  protected long getCurrentEndOffset() {
    return logEndOffset.get();
  }

  /**
   * Ensures that the provided fileendoffset satisfies constraints
   * @param fileSpan The filespan that needs to be verified
   */
  private void verifyFileEndOffset(FileSpan fileSpan) {
    if (this.logEndOffset.get() > fileSpan.getStartOffset() || fileSpan.getStartOffset() > fileSpan.getEndOffset()) {
      logger.error("File span offsets provided to the index does not meet constraints "
          + "logEndOffset {} inputFileStartOffset {} inputFileEndOffset {}", logEndOffset.get(),
          fileSpan.getStartOffset(), fileSpan.getEndOffset());
      throw new IllegalArgumentException("File span offsets provided to the index does not meet constraints " +
          "logEndOffset " + logEndOffset.get() +
          " inputFileStartOffset " + fileSpan.getStartOffset() +
          " inputFileEndOffset " + fileSpan.getEndOffset());
    }
  }

  /**
   * Persists the entire in memory index to a file. It does a safe persistence by writing to a temp file
   * and swapping the contents.
   */
  class IndexPersistor implements Runnable {

    private Object lock = new Object();
    private Short version = 0;
    private int Crc_Size = 8;
    private int Log_End_Offset_Size = 8;

    /**
     * Writes the index to a temp file and does an atomic swap with the current index file
     *  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
     * | version | fileendpointer |   key 1  | value 1  |  ...  |   key n   | value n   | crc      |
     * |(2 bytes)|   (8 bytes)    | (n bytes)| (n bytes)|       | (n bytes) | (n bytes) | (8 bytes)|
     *  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
     *  version         - the index format version
     *  fileendpointer  - the log end pointer that pertains to the index being persisted
     *  key n / value n - the key and value entries contained in this index segment
     *  crc             - the crc of the index segment content
     * @throws StoreException
     * @throws IOException
     */
    public void write()
        throws StoreException, IOException {
      logger.info("writing index to disk for {}", indexFile.getPath());
      // write to temp file and then swap with the existing file
      DataOutputStream writer = null;

      synchronized (lock) {
        final Timer.Context context = indexFlushTime.time();
        try {
          // write to temp file and then swap with the existing file
          File temp = new File(indexFile.getAbsolutePath() + ".tmp");
          FileOutputStream fileStream = new FileOutputStream(temp);
          CrcOutputStream crc = new CrcOutputStream(fileStream);
          writer = new DataOutputStream(crc);

          // before iterating the map, get the current file end pointer
          long fileEndPointer = logEndOffset.get();

          // flush the log to ensure everything till the fileEndPointer is flushed
          log.flush();

          // write the current version
          writer.writeShort(InMemoryIndex.version);
          writer.writeLong(fileEndPointer);

          // write the entries
          for (Map.Entry<StoreKey, IndexValue> entry : index.entrySet()) {
            writer.write(entry.getKey().toBytes());
            writer.write(entry.getValue().getBytes().array());
          }

          long crcValue = crc.getValue();
          writer.writeLong(crcValue);

          // flush and overwrite old file
          fileStream.getChannel().force(true);
          // replace current index file with temp file atomically
          // TODO how to handle the return type
          temp.renameTo(indexFile);
        } catch (IOException e) {
          logger.error("IO error while persisting index to disk {}", indexFile.getAbsoluteFile());
          throw new StoreException("IO error while persisting index to disk " + indexFile.getAbsolutePath(), e,
              StoreErrorCodes.IOError);
        } finally {
          if (writer != null) {
            writer.close();
          }
          context.stop();
        }
      }
      logger.info("Completed writing index to file");
    }

    public void run() {
      try {
        write();
      } catch (Exception e) {
        logger.info("Error while persisting the index to disk {}", e);
      }
    }

    /**
     * Reads the index from the file and populates the in memory map
     * @throws IOException
     * @throws StoreException
     */
    public void read()
        throws IOException, StoreException {
      logger.info("Reading index from file ", indexFile.getPath());
      synchronized (lock) {
        indexFile.createNewFile();
        CrcInputStream crcStream = new CrcInputStream(new FileInputStream(indexFile));
        DataInputStream stream = new DataInputStream(crcStream);
        try {
          short version = stream.readShort();
          switch (version) {
            case 0:
              long endOffset = stream.readLong();
              logEndOffset.set(endOffset);
              while (stream.available() > Crc_Size) {
                StoreKey key = factory.getStoreKey(stream);
                byte[] value = new byte[IndexValue.Index_Value_Size_In_Bytes];
                stream.read(value);
                IndexValue blobValue = new IndexValue(ByteBuffer.wrap(value));
                if (blobValue.getOffset() < logEndOffset.get()) {
                  index.put(key, blobValue);
                } else {
                  logger.info(
                      "Ignoring index entry outside the log end offset that was not synced logEndOffset {} key {}",
                      logEndOffset.get(), key);
                }
              }
              long crc = crcStream.getValue();
              if (crc != stream.readLong()) {
                index.clear();
                logEndOffset.set(0);
                throw new StoreException("Crc check does not match", StoreErrorCodes.Index_Creation_Failure);
              }
              break;
            default:
              throw new StoreException("Invalid version in index file", StoreErrorCodes.Index_Version_Error);
          }
        } catch (IOException e) {
          throw new StoreException("IO error while reading from file " + indexFile.getAbsolutePath(), e,
              StoreErrorCodes.IOError);
        } finally {
          stream.close();
        }
      }
    }
  }
}
