package com.github.ambry.store;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.utils.*;
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
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An in memory index implementation that is responsible for adding and modifying index entries,
 * recovering an index from the log and commit and recover index to disk . This class
 * is not thread safe and expects the caller to do appropriate synchronization.
 */
public class BlobIndex {
  protected ConcurrentHashMap<StoreKey, BlobIndexValue> index = new ConcurrentHashMap<StoreKey, BlobIndexValue>();
  protected Scheduler scheduler;
  private AtomicLong logEndOffset;
  private File indexFile;
  private static final String indexFileName = "index_current";
  private IndexPersistor persistor;
  private StoreKeyFactory factory;
  private BlobJournal journal;
  private Log log;
  private Logger logger = LoggerFactory.getLogger(getClass());
  public static final Short version = 0;

  // metrics
  private final MetricRegistry registry;
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
  public BlobIndex(String datadir,
                   Scheduler scheduler,
                   Log log,
                   StoreConfig config,
                   StoreKeyFactory factory,
                   MessageStoreRecovery recovery,
                   MetricRegistry registry) throws StoreException  {
    try {
      this.registry = registry;
      this.recoveryTime = registry.timer(MetricRegistry.name(BlobIndex.class, "indexRecoveryTime"));
      this.indexFlushTime = registry.timer(MetricRegistry.name(BlobIndex.class, "indexFlushTime"));
      this.nonzeroMessageRecovery = registry.counter(MetricRegistry.name(BlobIndex.class, "nonZeroMessageRecovery"));
      this.journal = new BlobJournal(config.storeIndexMaxNumberOfInmemElements,
                                     config.storeMaxNumberOfEntriesToReturnForFind);
      logEndOffset = new AtomicLong(0);
      //indexJournal = new BlobJournal();
      this.log = log;
      this.factory = factory;
      // check if file exist and recover from it
      indexFile = new File(datadir, indexFileName);
      persistor = new IndexPersistor();
      if (indexFile.exists()) {
        try {
          persistor.read();
        }
        catch (StoreException e) {
          if (e.getErrorCode() == StoreErrorCodes.Index_Creation_Failure ||
              e.getErrorCode() == StoreErrorCodes.Index_Version_Error) {
            // we just log the error here and retain the index so far created.
            // subsequent recovery process will add the missed out entries
            logger.error("Error while reading from index {}", e);
          }
          else
            throw e;
        }
      }
      // do recovery
      final Timer.Context context = recoveryTime.time();
      List<MessageInfo> messagesRecovered = recovery.recover(log, logEndOffset.get(), log.sizeInBytes(), factory);
      if (messagesRecovered.size() > 0)
        nonzeroMessageRecovery.inc(1);
      long runningOffset = logEndOffset.get();
      // iterate through the recovered messages and restore the state of the index
      for (MessageInfo info : messagesRecovered) {
        BlobIndexValue value = index.get(info.getStoreKey());
        // if the key already exist, update the delete state or ttl value if required
        if (value != null) {
          logger.info("Message already exists with key {}", info.getStoreKey());
          verifyFileEndOffset(new FileSpan(logEndOffset.get(), logEndOffset.get() + info.getSize()));
          if (info.isDeleted()) {
            FileSpan fileSpan = new FileSpan(logEndOffset.get(), logEndOffset.get() + info.getSize());
            markAsDeleted(info.getStoreKey(), fileSpan);
          }
          else if (info.getTimeToLiveInMs() == BlobIndexValue.TTL_Infinite) {
            FileSpan fileSpan = new FileSpan(logEndOffset.get(), logEndOffset.get() + info.getSize());
            updateTTL(info.getStoreKey(), BlobIndexValue.TTL_Infinite, fileSpan);
          }
          else
            throw new StoreException("Illegal message state during restore. ", StoreErrorCodes.Initialization_Error);
          logger.info("Updated message with key {} size {} ttl {} deleted {}",
                      info.getStoreKey(), value.getSize(), value.getTimeToLiveInMs(), info.isDeleted());
        }
        else {
          // add a new entry to the index
          BlobIndexValue newValue = new BlobIndexValue(info.getSize(), runningOffset, info.getTimeToLiveInMs());
          verifyFileEndOffset(new FileSpan(logEndOffset.get(), logEndOffset.get() + info.getSize()));
          FileSpan fileSpan = new FileSpan(logEndOffset.get(), logEndOffset.get() + info.getSize());
          addToIndex(new BlobIndexEntry(info.getStoreKey(), newValue), fileSpan);
          logger.info("Adding new message to index with key {} size {} ttl {} deleted {}",
                  info.getStoreKey(), info.getSize(), info.getTimeToLiveInMs(), info.isDeleted());
        }
        runningOffset += info.getSize();
      }
      context.stop();
      logger.info("read index from file {}", datadir);

      // start scheduler thread to persist index in the background
      this.scheduler = scheduler;
      this.scheduler.schedule("index persistor",
                              persistor,
                              config.storeDataFlushDelaySeconds + new Random().nextInt(SystemTime.SecsPerMin),
                              config.storeDataFlushIntervalSeconds,
                              TimeUnit.SECONDS);
    }
    catch (Exception e) {
      throw new StoreException("Error while creating index", e, StoreErrorCodes.Index_Creation_Failure);
    }
  }

  /**
   * Adds a new entry to the index
   * @param entry The entry to be added to the index
   * @param fileSpan The file span that this entry represents in the log
   * @throws StoreException
   */
  public void addToIndex(BlobIndexEntry entry, FileSpan fileSpan) {
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
  public void addToIndex(ArrayList<BlobIndexEntry> entries, FileSpan fileSpan) {
    verifyFileEndOffset(fileSpan);
    for (BlobIndexEntry entry : entries) {
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
  public void markAsDeleted(StoreKey id, FileSpan fileSpan) throws StoreException {
    verifyFileEndOffset(fileSpan);
    BlobIndexValue value = index.get(id);
    if (value == null) {
      logger.error("id {} not present in index. marking id as deleted failed", id);
      throw new StoreException("id not present in index : " + id, StoreErrorCodes.ID_Not_Found);
    }
    value.setFlag(BlobIndexValue.Flags.Delete_Index);
    index.put(id, value);
    this.logEndOffset.set(fileSpan.getEndOffset());
    journal.addEntry(fileSpan.getStartOffset(), id);
  }

  /**
   * Updates the ttl for the index entry represented by the key
   * @param id The id of the entry that needs its ttl to be updated
   * @param ttl The new ttl value that needs to be set
   * @param fileSpan The file range represented by this entry in the log
   * @throws StoreException
   */
  public void updateTTL(StoreKey id, long ttl, FileSpan fileSpan) throws StoreException {
    verifyFileEndOffset(fileSpan);
    BlobIndexValue value = index.get(id);
    if (value == null) {
      logger.error("id {} not present in index. updating ttl failed", id);
      throw new StoreException("id not present in index : " + id, StoreErrorCodes.ID_Not_Found);
    }
    value.setTimeToLive(ttl);
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
  public BlobReadOptions getBlobReadInfo(StoreKey id) throws StoreException {
    BlobIndexValue value = index.get(id);
    if (value == null) {
      logger.error("id {} not present in index. cannot find blob", id);
      throw new StoreException("id not present in index " + id, StoreErrorCodes.ID_Not_Found);
    }
    else if (value.isFlagSet(BlobIndexValue.Flags.Delete_Index)) {
      logger.error("id {} has been deleted", id);
      throw new StoreException("id has been deleted in index " + id, StoreErrorCodes.ID_Deleted);
    }
    else if (value.getTimeToLiveInMs() != BlobIndexValue.TTL_Infinite &&
             SystemTime.getInstance().milliseconds() > value.getTimeToLiveInMs()) {
      logger.error("id {} has expired ttl {}", id, value.getTimeToLiveInMs());
      throw new StoreException("id not present in index " + id, StoreErrorCodes.TTL_Expired);
    }
    return new BlobReadOptions(value.getOffset(), value.getSize(), value.getTimeToLiveInMs());
  }

  /**
   * Returns the list of keys that are not found in the index from the given input keys
   * @param keys The list of keys that needs to be tested against the index
   * @return The list of keys that are not found in the index
   * @throws StoreException
   */
  public List<StoreKey> findMissingEntries(List<StoreKey> keys) {
    List<StoreKey> missingEntries = new ArrayList<StoreKey>();
    for (StoreKey key : keys) {
      if (!exists(key))
        missingEntries.add(key);
    }
    return missingEntries;
  }

  /**
   * Finds all the entries from the given start token. The token defines the start position in the index from
   * where entries needs to be fetched
   * @param token The token that signifies the start position in the index from where entries need to be retrieved
   * @return The FindInfo state that contains both the list of entries and the new findtoken to start the next iteration
   */
  public FindInfo findEntriesSince(FindToken token) throws StoreException {
    StoreFindToken storeToken = (StoreFindToken)token;
    List<JournalEntry> entries = journal.getEntriesSince(storeToken.getOffset());
    List<MessageInfo> messageEntries = new ArrayList<MessageInfo>();
    if (entries == null) {
      // read the entire index and return it
      long largestOffset = 0;
      for (Map.Entry<StoreKey, BlobIndexValue> entry : index.entrySet()) {
        messageEntries.add(new MessageInfo(entry.getKey(),
                                           entry.getValue().getSize(),
                                           entry.getValue().isFlagSet(BlobIndexValue.Flags.Delete_Index),
                                           entry.getValue().getTimeToLiveInMs()));
        if (entry.getValue().getOffset() > largestOffset)
          largestOffset = entry.getValue().getOffset();
      }
      return new FindInfo(messageEntries, new StoreFindToken(largestOffset));
    }
    else {
      for (JournalEntry entry : entries) {
        BlobIndexValue value = index.get(entry.getKey());
        messageEntries.add(new MessageInfo(entry.getKey(),
                                           value.getSize(),
                                           value.isFlagSet(BlobIndexValue.Flags.Delete_Index),
                                           value.getTimeToLiveInMs()));
      }
      return new FindInfo(messageEntries, new StoreFindToken(entries.get(entries.size() - 1).getOffset()));
    }
  }

  /**
   * Closes the index
   * @throws StoreException
   */
  public void close() throws StoreException, IOException {
    persistor.write();
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
      logger.error("File span offsets provided to the index does not meet constraints " +
                   "logEndOffset {} inputFileStartOffset {} inputFileEndOffset {}",
                   logEndOffset.get(), fileSpan.getStartOffset(), fileSpan.getEndOffset());
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
    public void write() throws StoreException, IOException {
      logger.info("writing index to disk for {}", indexFile.getPath());
      // write to temp file and then swap with the existing file
      DataOutputStream writer = null;

      synchronized(lock) {
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
          writer.writeShort(BlobIndex.version);
          writer.writeLong(fileEndPointer);

          // write the entries
          for (Map.Entry<StoreKey, BlobIndexValue> entry : index.entrySet()) {
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
        }
        catch (IOException e) {
          logger.error("IO error while persisting index to disk {}", indexFile.getAbsoluteFile());
          throw new StoreException("IO error while persisting index to disk " +
                                   indexFile.getAbsolutePath(), e, StoreErrorCodes.IOError);
        }
        finally {
          if (writer != null)
            writer.close();
          context.stop();
        }
      }
      logger.info("Completed writing index to file");
    }

    public void run() {
      try {
        write();
      }
      catch (Exception e) {
        logger.info("Error while persisting the index to disk {}", e);
      }
    }

    /**
     * Reads the index from the file and populates the in memory map
     * @throws IOException
     * @throws StoreException
     */
    public void read() throws IOException, StoreException {
      logger.info("Reading index from file {}", indexFile.getPath());
      synchronized(lock) {
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
                byte[] value = new byte[BlobIndexValue.Index_Value_Size_In_Bytes];
                stream.read(value);
                BlobIndexValue blobValue = new BlobIndexValue(ByteBuffer.wrap(value));
                if (blobValue.getOffset() < logEndOffset.get()) {
                  index.put(key, blobValue);
                  journal.addEntry(blobValue.getOffset(), key);
                }
                else
                  logger.info("Ignoring index entry outside the log end offset that was not synced logEndOffset {} key {}",
                          logEndOffset.get(), key);
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
        }
        catch (IOException e) {
          throw new StoreException("IO error while reading from file " +
                  indexFile.getAbsolutePath(), e, StoreErrorCodes.IOError);
        }
        finally {
          stream.close();
        }
      }
    }
  }
}
