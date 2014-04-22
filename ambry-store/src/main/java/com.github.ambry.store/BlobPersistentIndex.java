package com.github.ambry.store;

import com.codahale.metrics.Timer;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.DataInputStream;
import java.io.FileOutputStream;
import java.io.DataOutputStream;
import java.io.RandomAccessFile;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Represents a segment of an index. The segment is represented by a
 * start offset and an end offset and has the keys sorted. The segment
 * can either be read only and memory mapped or writable and in memory.
 * The segment uses a bloom filter to optimize reads from disk. If the
 * index is read only, a key is searched by doing a binary search on
 * the memory mapped file. If the index is in memory, a normal map
 * lookup is performed to find key.
 */
class IndexSegmentInfo {
  private AtomicLong startOffset;
  private AtomicLong endOffset;
  private File indexFile;
  private ReadWriteLock rwLock;
  private MappedByteBuffer mmap = null;
  private AtomicBoolean mapped;
  private Logger logger = LoggerFactory.getLogger(getClass());
  private AtomicLong sizeWritten;
  private StoreKeyFactory factory;
  private IFilter bloomFilter;
  private final static int Key_Size_Invalid_Value = -1;
  private final static int Value_Size_Invalid_Value = -1;
  private final static int Version_Size = 2;
  private final static int Key_Size = 4;
  private final static int Value_Size = 4;
  private final static int Crc_Size = 8;
  private final static int Log_End_Offset_Size = 8;
  private final static int Index_Size_Excluding_Entries =
          Version_Size + Key_Size + Value_Size + Log_End_Offset_Size + Crc_Size;
  private int keySize;
  private int valueSize;
  private File bloomFile;
  private int prevNumOfEntriesWritten = 0;
  private AtomicInteger numberOfItems;
  protected ConcurrentSkipListMap<StoreKey, BlobIndexValue> index = null;
  private final StoreMetrics metrics;


  /**
   * Creates a new segment
   * @param dataDir The data directory to use for this segment
   * @param startOffset The start offset in the log that this segment represents
   * @param factory The store key factory used to create new store keys
   * @param keySize The key size that this segment supports
   * @param valueSize The value size that this segment supports
   * @param config The store config used to initialize the index segment
   */
  public IndexSegmentInfo(String dataDir,
                          long startOffset,
                          StoreKeyFactory factory,
                          int keySize,
                          int valueSize,
                          StoreConfig config,
                          StoreMetrics metrics) {
    // create a new file with the start offset
    indexFile = new File(dataDir, startOffset + "_" + BlobPersistentIndex.Index_File_Name_Suffix);
    bloomFile = new File(dataDir, startOffset + "_" + BlobPersistentIndex.Bloom_File_Name_Suffix);
    this.rwLock = new ReentrantReadWriteLock();
    this.startOffset = new AtomicLong(startOffset);
    this.endOffset = new AtomicLong(-1);
    index = new ConcurrentSkipListMap<StoreKey, BlobIndexValue>();
    mapped = new AtomicBoolean(false);
    sizeWritten = new AtomicLong(0);
    this.factory = factory;
    this.keySize = keySize;
    this.valueSize = valueSize;
    bloomFilter = FilterFactory.getFilter(config.storeIndexMaxNumberOfInmemElements,
                                          config.storeIndexBloomMaxFalsePositiveProbability);
    numberOfItems = new AtomicInteger(0);
    this.metrics = metrics;
  }

  /**
   * Initializes an existing segment. Memory maps the segment or reads the segment into memory. Also reads the
   * persisted bloom filter from disk.
   * @param indexFile The index file that the segment needs to be initialized from
   * @param isMapped Indicates if the segment needs to be memory mapped
   * @param factory The store key factory used to create new store keys
   * @param config The store config used to initialize the index segment
   * @param metrics The store metrics used to track metrics
   * @throws StoreException
   */
  public IndexSegmentInfo(File indexFile,
                          boolean isMapped,
                          StoreKeyFactory factory,
                          StoreConfig config,
                          StoreMetrics metrics,
                          BlobJournal journal) throws StoreException {
    try {
      int startIndex = indexFile.getName().indexOf("_", 0);
      String startOffsetValue = indexFile.getName().substring(0, startIndex);
      startOffset = new AtomicLong(Long.parseLong(startOffsetValue));
      endOffset = new AtomicLong(-1);
      this.indexFile = indexFile;
      this.rwLock = new ReentrantReadWriteLock();
      this.factory = factory;
      sizeWritten = new AtomicLong(0);
      numberOfItems = new AtomicInteger(0);
      mapped = new AtomicBoolean(false);
      if (isMapped) {
        map(false);
        // Load the bloom filter for this index
        // We need to load the bloom filter only for mapped indexes
        bloomFile = new File(indexFile.getParent(), startOffset + "_" + BlobPersistentIndex.Bloom_File_Name_Suffix);
        CrcInputStream crcBloom = new CrcInputStream(new FileInputStream(bloomFile));
        DataInputStream stream = new DataInputStream(crcBloom);
        bloomFilter = FilterFactory.deserialize(stream);
        long crcValue = crcBloom.getValue();
        if (crcValue != stream.readLong()) {
          // TODO metrics
          // we don't recover the filter. we just by pass the filter. Crc corrections will be done
          // by the scrubber
          bloomFilter = null;
          logger.error("Error validating crc for bloom filter for {}", bloomFile.getAbsolutePath());
        }
        stream.close();
      }
      else {
        index = new ConcurrentSkipListMap<StoreKey, BlobIndexValue>();
        bloomFilter = FilterFactory.getFilter(config.storeIndexMaxNumberOfInmemElements,
                                              config.storeIndexBloomMaxFalsePositiveProbability);
        bloomFile = new File(indexFile.getParent(), startOffset + "_" + BlobPersistentIndex.Bloom_File_Name_Suffix);
        try {
          readFromFile(indexFile, journal);
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
    }
    catch (Exception e) {
      logger.error("Error while loading index from file {}", e);
      throw new StoreException("Error while loading index from file Exception: " + e, StoreErrorCodes.Index_Creation_Failure);
    }
    this.metrics = metrics;
  }

  /**
   * The start offset that this segment represents
   * @return The start offset that this segment represents
   */
  public long getStartOffset() {
    return startOffset.get();
  }

  /**
   * The end offset that this segment represents
   * @return The end offset that this segment represents
   */
  public long getEndOffset() {
    return endOffset.get();
  }

  /**
   * Returns if this segment is mapped or not
   * @return True, if the segment is readonly and mapped. False, otherwise
   */
  public boolean isMapped() {
    return mapped.get();
  }

  /**
   * The underlying file that this segment represents
   * @return The file that this segment represents
   */
  public File getFile() {
    return indexFile;
  }

  /**
   * The key size in this segment
   * @return The key size in this segment
   */
  public int getKeySize() {
    return keySize;
  }

  /**
   * The value size in this segment
   * @return The value size in this segment
   */
  public int getValueSize() {
    return valueSize;
  }

  /**
   * Finds an entry given a key. It finds from the in memory map or
   * does a binary search on the mapped persistent segment
   * @param keyToFind The key to find
   * @return The blob index value that represents the key or null if not found
   * @throws StoreException
   */
  public BlobIndexValue find(StoreKey keyToFind) throws StoreException {
    try {
      rwLock.readLock().lock();
      if (!(mapped.get())) {
        return index.get(keyToFind);
      }
      else {
        boolean bloomSaysYes = false;
        // check bloom filter first
        if (bloomFilter == null || bloomFilter.isPresent(ByteBuffer.wrap(keyToFind.toBytes()))) {
          bloomSaysYes = true;
          metrics.bloomPositiveCount.inc(1);
          logger.trace(bloomFilter == null ?
                       "Bloom filter empty. Searching file with start offset {} and for key {} " :
                       "Found in bloom filter for index with start offset {} and for key {} ",
                       startOffset.get(),
                       keyToFind);
          // binary search on the mapped file
          ByteBuffer duplicate = mmap.duplicate();
          int low = 0;
          int high  = numberOfEntries(duplicate) - 1;
          logger.trace("binary search low : {} high : {}", low, high);
          while(low <= high) {
            int mid = (int)(Math.ceil(high/2.0 + low/2.0));
            StoreKey found = getKeyAt(duplicate, mid);
            logger.trace("Binary search - key found on iteration {}", found);
            int result = found.compareTo(keyToFind);
            if(result == 0) {
              byte[] buf = new byte[valueSize];
              duplicate.get(buf);
              return new BlobIndexValue(ByteBuffer.wrap(buf));
            }
            else if(result < 0)
              low = mid + 1;
            else
              high = mid - 1;
          }
        }
        if (bloomSaysYes)
          metrics.bloomFalsePositiveCount.inc(1);
        return null;
      }
    }
    catch (IOException e) {
      logger.error("IO error while searching the index {}", indexFile.getAbsoluteFile());
      throw new StoreException("IO error while searching the index " +
              indexFile.getAbsolutePath(), e, StoreErrorCodes.IOError);
    }
    finally {
      rwLock.readLock().unlock();
    }
  }

  private int numberOfEntries(ByteBuffer mmap) {
    return (mmap.capacity() - Index_Size_Excluding_Entries) / (keySize + valueSize);
  }

  private StoreKey getKeyAt(ByteBuffer mmap, int index) throws IOException {
    mmap.position(Version_Size + Key_Size + Value_Size + Log_End_Offset_Size + (index * (keySize + valueSize)));
    return factory.getStoreKey(new DataInputStream(new ByteBufferInputStream(mmap)));
  }

  private int findIndex(StoreKey keyToFind, ByteBuffer mmap) throws IOException {
    // binary search on the mapped file
    int low = 0;
    int high  = numberOfEntries(mmap) - 1;
    logger.trace("binary search low : {} high : {}", low, high);
    while (low <= high) {
      int mid = (int)(Math.ceil(high/2.0 + low/2.0));
      StoreKey found = getKeyAt(mmap, mid);
      logger.trace("Binary search - key found on iteration {}", found);
      int result = found.compareTo(keyToFind);
      if(result == 0) {
        return mid;
      }
      else if(result < 0)
        low = mid + 1;
      else
        high = mid - 1;
    }
    return -1;
  }

  /**
   * Adds an entry into the segment. The operation works only if the segment is read/write
   * @param entry The entry that needs to be added to the segment.
   * @param fileEndOffset The file end offset that this entry represents.
   * @throws StoreException
   */
  public void addEntry(BlobIndexEntry entry, long fileEndOffset) throws StoreException {
    try {
      rwLock.readLock().lock();
      if (mapped.get())
        throw new StoreException("Cannot add to a mapped index " +
                indexFile.getAbsolutePath(), StoreErrorCodes.Illegal_Index_Operation);
      logger.trace("Inserting key {} value offset {} size {} ttl {} originalMessageOffset {} fileEndOffset {}",
              entry.getKey(),
              entry.getValue().getOffset(),
              entry.getValue().getSize(),
              entry.getValue().getTimeToLiveInMs(),
              entry.getValue().getOriginalMessageOffset(),
              fileEndOffset);
      index.put(entry.getKey(), entry.getValue());
      sizeWritten.addAndGet(entry.getKey().sizeInBytes() + entry.getValue().getSize());
      numberOfItems.incrementAndGet();
      bloomFilter.add(ByteBuffer.wrap(entry.getKey().toBytes()));
      endOffset.set(fileEndOffset);
      if (keySize == Key_Size_Invalid_Value) {
        keySize = entry.getKey().sizeInBytes();
        logger.info("Setting key size to {} for index with start offset {}", keySize, startOffset);
      }
      if (valueSize == Value_Size_Invalid_Value) {
        valueSize = entry.getValue().getBytes().capacity();
        logger.info("Setting value size to {} for index with start offset {}", valueSize, startOffset);
      }
    }
    finally {
      rwLock.readLock().unlock();
    }
  }

  /**
   * Adds a list of entries into the segment. The operation works only if the segment is read/write
   * @param entries The entries that needs to be added to the segment.
   * @param fileEndOffset The file end offset of the last entry in the entries list
   * @throws StoreException
   */
  public void addEntries(ArrayList<BlobIndexEntry> entries, long fileEndOffset) throws StoreException {
    try {
      rwLock.readLock().lock();
      if (mapped.get())
        throw new StoreException("Cannot add to a mapped index" +
                indexFile.getAbsolutePath(), StoreErrorCodes.Illegal_Index_Operation);
      if (entries.size() == 0)
        throw new IllegalArgumentException("No entries to add to the index. The entries provided is empty");
      for (BlobIndexEntry entry : entries) {
        logger.trace("Inserting key {} value offset {} size {} ttl {} originalMessageOffset {} fileEndOffset {}",
                entry.getKey(),
                entry.getValue().getOffset(),
                entry.getValue().getSize(),
                entry.getValue().getTimeToLiveInMs(),
                entry.getValue().getOriginalMessageOffset(),
                fileEndOffset);
        index.put(entry.getKey(), entry.getValue());
        sizeWritten.addAndGet(entry.getKey().sizeInBytes() + BlobIndexValue.Index_Value_Size_In_Bytes);
        numberOfItems.incrementAndGet();
        bloomFilter.add(ByteBuffer.wrap(entry.getKey().toBytes()));
      }
      endOffset.set(fileEndOffset);
      if (keySize == Key_Size_Invalid_Value) {
        keySize = entries.get(0).getKey().sizeInBytes();
        logger.info("Setting key size to {} for index with start offset {}", keySize, startOffset);
      }
      if (valueSize == Value_Size_Invalid_Value) {
        valueSize = entries.get(0).getValue().getBytes().capacity();
        logger.info("Setting value size to {} for index with start offset {}", valueSize, startOffset);
      }
    }
    finally {
      rwLock.readLock().unlock();
    }
  }

  /**
   * The total size in bytes written to this segment so far
   * @return The total size in bytes written to this segment so far
   */
  public long getSizeWritten() {
    try {
      rwLock.readLock().lock();
      if (mapped.get())
        throw new UnsupportedOperationException("Operation supported only on umapped indexes");
      return sizeWritten.get();
    }
    finally {
      rwLock.readLock().unlock();
    }
  }

  /**
   * The number of items contained in this segment
   * @return The number of items contained in this segment
   */
  public int getNumberOfItems() {
    try {
      rwLock.readLock().lock();
      if (mapped.get())
        throw new UnsupportedOperationException("Operation supported only on unmapped indexes");
      return numberOfItems.get();
    }
    finally {
      rwLock.readLock().unlock();
    }
  }

  /**
   * Writes the index to a persistent file. Writes the data in the following format
   *  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
   * | version | keysize | valuesize | fileendpointer |   key 1  | value 1  |  ...  |   key n   | value n   | crc      |
   * |(2 bytes)|(4 bytes)| (4 bytes) |    (8 bytes)   | (n bytes)| (n bytes)|       | (n bytes) | (n bytes) | (8 bytes)|
   *  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
   *  version         - the index format version
   *  keysize         - the size of the key in this index segment
   *  valuesize       - the size of the value in this index segment
   *  fileendpointer  - the log end pointer that pertains to the index being persisted
   *  key n / value n - the key and value entries contained in this index segment
   *  crc             - the crc of the index segment content
   *
   * @param fileEndPointer
   * @throws IOException
   * @throws StoreException
   */
  public void writeIndexToFile(long fileEndPointer) throws IOException, StoreException {
    if (prevNumOfEntriesWritten != index.size()) {
      File temp = new File(getFile().getAbsolutePath() + ".tmp");
      FileOutputStream fileStream = new FileOutputStream(temp);
      CrcOutputStream crc = new CrcOutputStream(fileStream);
      DataOutputStream writer = new DataOutputStream(crc);
      try {
        rwLock.readLock().lock();

        // write the current version
        writer.writeShort(BlobPersistentIndex.version);
        // write key, value size and file end pointer for this index
        writer.writeInt(this.keySize);
        writer.writeInt(this.valueSize);
        writer.writeLong(fileEndPointer);

        int numOfEntries = 0;
        // write the entries
        for (Map.Entry<StoreKey, BlobIndexValue> entry : index.entrySet()) {
          writer.write(entry.getKey().toBytes());
          writer.write(entry.getValue().getBytes().array());
          logger.trace("Index {} writing key {} offset {} size {}",
                       getFile().getAbsolutePath(), entry.getKey(), entry.getValue().getOffset(), entry.getValue().getOffset());
          numOfEntries++;
        }
        prevNumOfEntriesWritten = numOfEntries;
        long crcValue = crc.getValue();
        writer.writeLong(crcValue);

        // flush and overwrite old file
        fileStream.getChannel().force(true);
        // swap temp file with the original file
        temp.renameTo(getFile());
      }
      catch (IOException e) {
        logger.error("IO error while persisting index to disk " + indexFile.getAbsoluteFile());
        throw new StoreException("IO error while persisting index to disk " +
                indexFile.getAbsolutePath(), e, StoreErrorCodes.IOError);
      }
      finally {
        writer.close();
        rwLock.readLock().unlock();
      }
      logger.debug("Completed writing index to file {}", indexFile.getAbsolutePath());
    }
  }

  /**
   * Memory maps the segment of index. Optionally, it also persist the bloom filter to disk
   * @param persistBloom True, if the bloom filter needs to be persisted. False otherwise.
   * @throws IOException
   * @throws StoreException
   */
  public void map(boolean  persistBloom) throws IOException, StoreException {
    RandomAccessFile raf = new RandomAccessFile(indexFile, "r");
    rwLock.writeLock().lock();
    try {
      mmap = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, indexFile.length());
      mmap.position(0);
      short version = mmap.getShort();
      switch (version) {
        case 0:
          this.keySize = mmap.getInt();
          this.valueSize = mmap.getInt();
          this.endOffset.set(mmap.getLong());
          break;
        default:
          throw new StoreException("Unknown version in index file", StoreErrorCodes.Index_Version_Error);
      }
      mapped.set(true);
      index = null;
    } finally {
      raf.close();
      rwLock.writeLock().unlock();
    }
    // we should be fine reading bloom filter here without synchronization as the index is read only
    // we only persist the bloom filter once during its entire lifetime
    if (persistBloom) {
      CrcOutputStream crcStream = new CrcOutputStream(new FileOutputStream(bloomFile));
      DataOutputStream stream = new DataOutputStream(crcStream);
      FilterFactory.serialize(bloomFilter, stream);
      long crcValue = crcStream.getValue();
      stream.writeLong(crcValue);
    }
  }

  /**
   * Reads the index segment from file into an in memory representation
   * @param fileToRead The file to read the index segment from
   * @throws StoreException
   * @throws IOException
   */
  private void readFromFile(File fileToRead, BlobJournal journal) throws StoreException, IOException {
    logger.info("Reading index from file {}", indexFile.getPath());
    index.clear();
    CrcInputStream crcStream = new CrcInputStream(new FileInputStream(fileToRead));
    DataInputStream stream = new DataInputStream(crcStream);
    try {
      short version = stream.readShort();
      switch (version) {
        case 0:
          this.keySize = stream.readInt();
          this.valueSize = stream.readInt();
          this.endOffset.set(stream.readLong());
          while (stream.available() > Crc_Size) {
            StoreKey key = factory.getStoreKey(stream);
            byte[] value = new byte[BlobIndexValue.Index_Value_Size_In_Bytes];
            stream.read(value);
            BlobIndexValue blobValue = new BlobIndexValue(ByteBuffer.wrap(value));
            // ignore entries that have offsets outside the log end offset that this index represents
            if (blobValue.getOffset() < endOffset.get()) {
              index.put(key, blobValue);
              logger.trace("Index {} putting key {} in index offset {} size {}",
                           indexFile.getPath(), key, blobValue.getOffset(), blobValue.getSize());
              // regenerate the bloom filter for in memory indexes
              bloomFilter.add(ByteBuffer.wrap(key.toBytes()));
              // add to the journal
              journal.addEntry(blobValue.getOffset(), key);
              sizeWritten.addAndGet(key.sizeInBytes() + BlobIndexValue.Index_Value_Size_In_Bytes);
              numberOfItems.incrementAndGet();
            }
            else {
              logger.info("Index {} ignoring index entry outside the log end offset that was not synced logEndOffset {} key {}" +
                          indexFile.getPath(), endOffset.get(), key);
            }
          }
          long crc = crcStream.getValue();
          if (crc != stream.readLong()) {
            // reset structures
            this.keySize = Key_Size_Invalid_Value;
            this.valueSize = Value_Size_Invalid_Value;
            this.endOffset.set(0);
            index.clear();
            bloomFilter.clear();
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

  /**
   * Gets all the entries upto maxEntries from the start of a given key(inclusive).
   * @param key The key from where to start retrieving entries.
   *            If the key is null, all entries are retrieved upto maxentries
   * @param maxTotalSizeOfEntriesInBytes The max total size of entries to retreive
   * @param entries The input entries list that needs to be filled. The entries list can have existing entries
   * @param currentTotalSizeOfEntriesInBytes The current total size in bytes of the entries
   * @throws IOException
   */
  public void getEntriesSince(StoreKey key,
                              long maxTotalSizeOfEntriesInBytes,
                              List<MessageInfo> entries,
                              AtomicLong currentTotalSizeOfEntriesInBytes) throws IOException {
    if (mapped.get()) {
      int index = 0;
      if (key != null)
        index = findIndex(key, mmap.duplicate());
      if (index != -1) {
        ByteBuffer readBuf = mmap.duplicate();
        int totalEntries = numberOfEntries(readBuf);
        while (currentTotalSizeOfEntriesInBytes.get() < maxTotalSizeOfEntriesInBytes && index < totalEntries) {
          StoreKey newKey = getKeyAt(readBuf, index);
          byte[] buf = new byte[valueSize];
          readBuf.get(buf);
          if (key == null || newKey.compareTo(key) != 0) {
            BlobIndexValue newValue = new BlobIndexValue(ByteBuffer.wrap(buf));
            MessageInfo info = new MessageInfo(newKey,
                                               newValue.getSize(),
                                               newValue.isFlagSet(BlobIndexValue.Flags.Delete_Index),
                                               newValue.getTimeToLiveInMs());
            entries.add(info);
            currentTotalSizeOfEntriesInBytes.addAndGet(newValue.getSize());
          }
          index++;
        }
      }
    }
    else {
      ConcurrentNavigableMap<StoreKey, BlobIndexValue> tempMap = index;
      if (key != null)
        tempMap = index.tailMap(key, true);
      for (Map.Entry<StoreKey, BlobIndexValue> entry : tempMap.entrySet()) {
        if (key == null || entry.getKey().compareTo(key) != 0) {
          MessageInfo info = new MessageInfo(entry.getKey(),
                                             entry.getValue().getSize(),
                                             entry.getValue().isFlagSet(BlobIndexValue.Flags.Delete_Index),
                                             entry.getValue().getTimeToLiveInMs());
          entries.add(info);
          currentTotalSizeOfEntriesInBytes.addAndGet(entry.getValue().getSize());
          if (currentTotalSizeOfEntriesInBytes.get() >= maxTotalSizeOfEntriesInBytes) {
            break;
          }
        }
      }
    }
  }
}

/**
 * A persistent index implementation that is responsible for adding and modifying index entries,
 * recovering an index from the log and commit and recover index to disk . This class
 * is not thread safe and expects the caller to do appropriate synchronization.
 **/
public class BlobPersistentIndex {

  private long maxInMemoryIndexSizeInBytes;
  private int maxInMemoryNumElements;
  private Log log;
  private String dataDir;
  private Logger logger = LoggerFactory.getLogger(getClass());
  private IndexPersistor persistor;
  private StoreKeyFactory factory;
  private StoreConfig config;
  private BlobJournal journal;
  private UUID sessionId;
  private boolean cleanShutdown;
  private long logEndOffsetOnStartup;
  private static final String Clean_Shutdown_Filename = "cleanshutdown";
  private final StoreMetrics metrics;

  protected Scheduler scheduler;
  protected ConcurrentSkipListMap<Long, IndexSegmentInfo> indexes = new ConcurrentSkipListMap<Long, IndexSegmentInfo>();

  public static final String Index_File_Name_Suffix = "index";
  public static final String Bloom_File_Name_Suffix = "bloom";
  public static final Short version = 0;

  private class IndexFilter implements FilenameFilter {
    @Override
    public boolean accept(File dir, String name) {
      return name.endsWith(Index_File_Name_Suffix);
    }
  }

  /**
   * Creates a new persistent index
   * @param datadir The directory to use to store the index files
   * @param scheduler The scheduler that runs regular background tasks
   * @param log The log that is represented by this index
   * @param config The store configs for this index
   * @param factory The factory used to create store keys
   * @param recovery The recovery handle to perform recovery on startup
   * @throws StoreException
   */
  public BlobPersistentIndex(String datadir,
                             Scheduler scheduler,
                             Log log,
                             StoreConfig config,
                             StoreKeyFactory factory,
                             MessageStoreRecovery recovery,
                             StoreMetrics metrics) throws StoreException {
    try {
      this.scheduler = scheduler;
      this.metrics = metrics;
      this.log = log;
      File indexDir = new File(datadir);
      File[] indexFiles = indexDir.listFiles(new IndexFilter());
      this.factory = factory;
      this.config = config;
      persistor = new IndexPersistor();
      journal = new BlobJournal(datadir, config.storeIndexMaxNumberOfInmemElements, config.storeMaxNumberOfEntriesToReturnFromJournal);
      Arrays.sort(indexFiles, new Comparator<File>() {
        @Override
        public int compare(File o1, File o2) {
          if (o1 == null || o2 == null)
            throw new NullPointerException("arguments to compare two files is null");
          // File name pattern for index is offset_name. We extract the offset from
          // name to compare
          int o1Index = o1.getName().indexOf("_", 0);
          long o1Offset = Long.parseLong(o1.getName().substring(0, o1Index));
          int o2Index = o2.getName().indexOf("_", 0);
          long o2Offset = Long.parseLong(o2.getName().substring(0, o2Index));
          if (o1Offset == o2Offset)
            return 0;
          else if (o1Offset < o2Offset)
            return -1;
          else
            return 1;
        }
      });

      for (int i = 0; i < indexFiles.length; i++) {
        boolean map = false;
        // We map all the indexes except the most recent two indexes.
        // The recent indexes would go through recovery after they have been
        // read into memory
        if (i < indexFiles.length - 2)
          map = true;
        IndexSegmentInfo info = new IndexSegmentInfo(indexFiles[i], map, factory, config, metrics, journal);
        logger.info("Loaded index {}", indexFiles[i]);
        indexes.put(info.getStartOffset(), info);
      }
      this.dataDir = datadir;

      // perform recovery if required
      final Timer.Context context = metrics.recoveryTime.time();
      if (indexes.size() > 0) {
        IndexSegmentInfo lastSegment = indexes.lastEntry().getValue();
        Map.Entry<Long, IndexSegmentInfo> entry = indexes.lowerEntry(lastSegment.getStartOffset());
        if (entry != null) {
          recover(entry.getValue(), lastSegment.getStartOffset(), recovery);
        }
        // recover last segment
        recover(indexes.lastEntry().getValue(), log.sizeInBytes(), recovery);
      }
      else
        recover(null, log.sizeInBytes(), recovery);
      context.stop();

      // set the log end offset to the recovered offset from the index after initializing it
      log.setLogEndOffset(getCurrentEndOffset());
      logEndOffsetOnStartup = log.getLogEndOffset();

      this.maxInMemoryIndexSizeInBytes = config.storeIndexMaxMemorySizeBytes;
      this.maxInMemoryNumElements = config.storeIndexMaxNumberOfInmemElements;
      this.sessionId = UUID.randomUUID();
      // delete the shutdown file
      File cleanShutdownFile = new File(datadir, Clean_Shutdown_Filename);
      if (cleanShutdownFile.exists()) {
        cleanShutdown = true;
        cleanShutdownFile.delete();
      }

      // start scheduler thread to persist index in the background
      this.scheduler = scheduler;
      this.scheduler.schedule("index persistor",
                              persistor,
                              config.storeDataFlushDelaySeconds + new Random().nextInt(SystemTime.SecsPerMin),
                              config.storeDataFlushIntervalSeconds,
                              TimeUnit.SECONDS);
    }
    catch (Exception e) {
      logger.error("Error while creating index ", e);
      throw new StoreException("Error while creating index " + e.getMessage(), StoreErrorCodes.Index_Creation_Failure);
    }
  }

  /**
   * Recovers a segment given the end offset in the log and a recovery handler
   * @param segmentToRecover The segment to recover. If this is null, it creates a new segment
   * @param endOffset The end offset till which recovery needs to happen in the log
   * @param recovery The recovery handler that is used to perform the recovery
   * @throws StoreException
   * @throws IOException
   */
  private void recover(IndexSegmentInfo segmentToRecover, long endOffset, MessageStoreRecovery recovery)
          throws StoreException, IOException {
    // fix the start offset in the log for recovery.
    long startOffsetForRecovery = 0;
    if (segmentToRecover != null) {
      startOffsetForRecovery = segmentToRecover.getEndOffset() == -1 ?
                               segmentToRecover.getStartOffset() :
                               segmentToRecover.getEndOffset();
    }
    logger.info("Performing recovery on index with start offset {} and end offset {}", startOffsetForRecovery, endOffset);
    List<MessageInfo> messagesRecovered = recovery.recover(log, startOffsetForRecovery, endOffset, factory);
    if (messagesRecovered.size() > 0)
      metrics.nonzeroMessageRecovery.inc(1);
    long runningOffset = startOffsetForRecovery;
    // Iterate through the recovered messages and update the index
    for (MessageInfo info : messagesRecovered) {
      logger.trace("Index {} recovering key {} offset {} size {}",
                   dataDir, info.getStoreKey(), runningOffset, info.getSize());
      if (segmentToRecover == null) {
        // if there was no segment passed in, create a new one

        segmentToRecover = new IndexSegmentInfo(dataDir,
                                                startOffsetForRecovery,
                                                factory,
                                                info.getStoreKey().sizeInBytes(),
                                                BlobIndexValue.Index_Value_Size_In_Bytes,
                                                config,
                                                metrics);
        indexes.put(startOffsetForRecovery, segmentToRecover);
      }
      BlobIndexValue value = findKey(info.getStoreKey());
      if (value != null) {
        // if the key already exist in the index, update it if it is deleted or ttl updated
        logger.info("Msg already exist with key {}", info.getStoreKey());
        if (info.isDeleted()) {
          value.setFlag(BlobIndexValue.Flags.Delete_Index);
        }
        else if (info.getExpirationTimeInMs() == Utils.Infinite_Time) {
          value.setTimeToLive(Utils.Infinite_Time);
        }
        else
          throw new StoreException("Illegal message state during restore. ", StoreErrorCodes.Initialization_Error);
        verifyFileEndOffset(new FileSpan(runningOffset, runningOffset + info.getSize()));
        value.setNewOffset(runningOffset);
        segmentToRecover.addEntry(new BlobIndexEntry(info.getStoreKey(), value), runningOffset + info.getSize());
        journal.addEntry(runningOffset, info.getStoreKey());
        logger.info("Updated message with key {} size {} ttl {} deleted {}",
                info.getStoreKey(), value.getSize(), value.getTimeToLiveInMs(), info.isDeleted());
      }
      else {
        // create a new entry in the index
        BlobIndexValue newValue = new BlobIndexValue(info.getSize(), runningOffset, info.getExpirationTimeInMs());
        verifyFileEndOffset(new FileSpan(runningOffset, runningOffset + info.getSize()));
        segmentToRecover.addEntry(new BlobIndexEntry(info.getStoreKey(), newValue), runningOffset + info.getSize());
        journal.addEntry(runningOffset, info.getStoreKey());
        logger.info("Adding new message to index with key {} size {} ttl {} deleted {}",
                info.getStoreKey(), info.getSize(), info.getExpirationTimeInMs(), info.isDeleted());
      }
      runningOffset += info.getSize();
    }
  }

  /**
   * Adds a new entry to the index
   * @param entry The entry to be added to the index
   * @param fileSpan The file span that this entry represents in the log
   * @throws StoreException
   */
  public void addToIndex(BlobIndexEntry entry, FileSpan fileSpan) throws StoreException {
    verifyFileEndOffset(fileSpan);
    if (needToRollOverIndex(entry)) {
      IndexSegmentInfo info = new IndexSegmentInfo(dataDir,
                                                   entry.getValue().getOffset(),
                                                   factory,
                                                   entry.getKey().sizeInBytes(),
                                                   BlobIndexValue.Index_Value_Size_In_Bytes,
                                                   config,
                                                   metrics);
      info.addEntry(entry, fileSpan.getEndOffset());
      indexes.put(info.getStartOffset(), info);
    }
    else {
      indexes.lastEntry().getValue().addEntry(entry, fileSpan.getEndOffset());
    }
    journal.addEntry(entry.getValue().getOffset(), entry.getKey());
  }

  /**
   * Adds a set of entries to the index
   * @param entries The entries to be added to the index
   * @param fileSpan The file span that the entries represent in the log
   * @throws StoreException
   */
  public void addToIndex(ArrayList<BlobIndexEntry> entries, FileSpan fileSpan) throws StoreException {
    verifyFileEndOffset(fileSpan);
    if (needToRollOverIndex(entries.get(0))) {
      IndexSegmentInfo info = new IndexSegmentInfo(dataDir,
                                                   entries.get(0).getValue().getOffset(),
                                                   factory,
                                                   entries.get(0).getKey().sizeInBytes(),
                                                   BlobIndexValue.Index_Value_Size_In_Bytes,
                                                   config,
                                                   metrics);
      info.addEntries(entries, fileSpan.getEndOffset());
      indexes.put(info.getStartOffset(), info);
    }
    else {
      indexes.lastEntry().getValue().addEntries(entries, fileSpan.getEndOffset());
    }
    for (BlobIndexEntry entry : entries) {
      journal.addEntry(entry.getValue().getOffset(), entry.getKey());
    }
  }

  /**
   * Checks if the index segment needs to roll over to a new segment
   * @param entry The new entry that needs to be added to the existing active segment
   * @return True, if segment needs to roll over. False, otherwise
   */
  private boolean needToRollOverIndex(BlobIndexEntry entry) {
    return  indexes.size() == 0 ||
            indexes.lastEntry().getValue().getSizeWritten() >= maxInMemoryIndexSizeInBytes ||
            indexes.lastEntry().getValue().getNumberOfItems() >= maxInMemoryNumElements ||
            indexes.lastEntry().getValue().getKeySize() != entry.getKey().sizeInBytes() ||
            indexes.lastEntry().getValue().getValueSize() != BlobIndexValue.Index_Value_Size_In_Bytes;
  }

  /**
   * Indicates if a key is present in the index
   * @param key The key to do the exist check against
   * @return True, if the key exist in the index. False, otherwise.
   * @throws StoreException
   */
  public boolean exists(StoreKey key) throws StoreException {
    return findKey(key) != null;
  }

  /**
   * Finds a key in the index and returns the blob index value associated with it. If not found,
   * returns null
   * @param key  The key to find in the index
   * @return The blob index value associated with the key. Null if the key is not found.
   * @throws StoreException
   */
  protected BlobIndexValue findKey(StoreKey key) throws StoreException {
    final Timer.Context context = metrics.findTime.time();
    try {
      ConcurrentNavigableMap<Long, IndexSegmentInfo> descendMap = indexes.descendingMap();
      for (Map.Entry<Long, IndexSegmentInfo> entry : descendMap.entrySet()) {
        logger.trace("Searching index with start offset {}", entry.getKey());
        BlobIndexValue value = entry.getValue().find(key);
        if (value != null) {
          logger.trace("found value offset {} size {} ttl {}",
                  value.getOffset(), value.getSize(), value.getTimeToLiveInMs());
          return value;
        }
      }
    }
    finally {
      context.stop();
    }
    return null;
  }

  /**
   * Marks the index entry represented by the key for delete
   * @param id The id of the entry that needs to be deleted
   * @param fileSpan The file range represented by this entry in the log
   * @throws StoreException
   */
  public void markAsDeleted(StoreKey id, FileSpan fileSpan) throws StoreException {
    verifyFileEndOffset(fileSpan);
    BlobIndexValue value = findKey(id);
    if (value == null) {
      logger.error("id {} not present in index. marking id as deleted failed", id);
      throw new StoreException("id not present in index : " + id, StoreErrorCodes.ID_Not_Found);
    }
    value.setFlag(BlobIndexValue.Flags.Delete_Index);
    value.setNewOffset(fileSpan.getStartOffset());
    indexes.lastEntry().getValue().addEntry(new BlobIndexEntry(id, value), fileSpan.getEndOffset());
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
    BlobIndexValue value = findKey(id);
    if (value == null) {
      logger.error("id {} not present in index. updating ttl failed", id);
      throw new StoreException("id not present in index : " + id, StoreErrorCodes.ID_Not_Found);
    }
    value.setTimeToLive(ttl);
    value.setNewOffset(fileSpan.getStartOffset());
    indexes.lastEntry().getValue().addEntry(new BlobIndexEntry(id, value), fileSpan.getEndOffset());
    journal.addEntry(fileSpan.getStartOffset(), id);
  }

  /**
   * Returns the blob read info for a given key that is not deleted or expired ttl
   * @param id The id of the entry whose info is required
   * @return The blob read info that contains the information for the given key
   * @throws StoreException
   */
  public BlobReadOptions getBlobReadInfo(StoreKey id) throws StoreException {
    BlobIndexValue value = findKey(id);
    if (value == null) {
      logger.error("id {} not present in index. cannot find blob", id);
      throw new StoreException("id not present in index " + id, StoreErrorCodes.ID_Not_Found);
    }
    else if (value.isFlagSet(BlobIndexValue.Flags.Delete_Index)) {
      logger.error("id {} has been deleted", id);
      throw new StoreException("id has been deleted in index " + id, StoreErrorCodes.ID_Deleted);
    }
    else if (value.isExpired()) {
      logger.error("id {} has expired ttl {}", id, value.getTimeToLiveInMs());
      throw new StoreException("id has expired ttl in index " + id, StoreErrorCodes.TTL_Expired);
    }
    return new BlobReadOptions(value.getOffset(), value.getSize(), value.getTimeToLiveInMs(), id);
  }

  /**
   * Returns the list of keys that are not found in the index from the given input keys. This also checks
   * keys that are marked for deletion and those that have an expired ttl
   * @param keys The list of keys that needs to be tested against the index
   * @return The list of keys that are not found in the index
   * @throws StoreException
   */
  public Set<StoreKey> findMissingKeys(List<StoreKey> keys) throws StoreException {
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
   *                              return a list of entries whose total size is close to this value.
   * @return The FindInfo state that contains both the list of entries and the new findtoken to start the next iteration
   */
  public FindInfo findEntriesSince(FindToken token, long maxTotalSizeOfEntries) throws StoreException {
    try {
      StoreFindToken storeToken = (StoreFindToken)token;
      // validate token
      if (storeToken.getSessionId() == null || storeToken.getSessionId().compareTo(sessionId) != 0) {
        // the session has changed. check if we had an unclean shutdown on startup
        if (!cleanShutdown) {
          // if we had an unclean shutdown and the token offset is larger than the logEndOffsetOnStartup
          // we reset the token to logEndOffsetOnStartup
          if ((storeToken.getStoreKey() != null && storeToken.getIndexStartOffset() > logEndOffsetOnStartup) ||
              (storeToken.getOffset() > logEndOffsetOnStartup)) {
            logger.info("Index: " + dataDir + " resetting offset after not clean shutdown " + logEndOffsetOnStartup + " before offset " + storeToken.getOffset());
            storeToken = new StoreFindToken(logEndOffsetOnStartup, sessionId);
          }
        }
        else if ((storeToken.getStoreKey() != null && storeToken.getIndexStartOffset() > logEndOffsetOnStartup) ||
                 (storeToken.getOffset() > logEndOffsetOnStartup)){
          logger.error("Index: " + dataDir + " Invalid token. Provided offset is outside the log range after clean shutdown");
          // if the shutdown was clean, the offset should always be lesser or equal to the logEndOffsetOnStartup
          throw new IllegalArgumentException("Invalid token. Provided offset is outside the log range after clean shutdown");
        }
      }
      List<MessageInfo> messageEntries = new ArrayList<MessageInfo>();
      if (storeToken.getStoreKey() == null) {
        boolean inclusive = false;
        long offsetToStart = storeToken.getOffset();
        if (storeToken.getOffset() == -1) {
          inclusive = true;
          offsetToStart = 0;
        }
        logger.trace("Index: " + dataDir + " Getting entries since " + offsetToStart);
        // check journal
        List<JournalEntry> entries = journal.getEntriesSince(offsetToStart, inclusive);
        if (entries != null) {
          long offsetEnd = offsetToStart;
          long currentTotalSizeOfEntries = 0;
          for (JournalEntry entry : entries) {
            BlobIndexValue value = findKey(entry.getKey());
            messageEntries.add(new MessageInfo(entry.getKey(),
                                               value.getSize(),
                                               value.isFlagSet(BlobIndexValue.Flags.Delete_Index),
                                               value.getTimeToLiveInMs()));
            currentTotalSizeOfEntries += value.getSize();
            offsetEnd = entry.getOffset();
            if (currentTotalSizeOfEntries >= maxTotalSizeOfEntries)
              break;
          }
          logger.trace("Index: " + dataDir + " New offset from find info" + offsetEnd);
          eliminateDuplicates(messageEntries);
          return new FindInfo(messageEntries, new StoreFindToken(offsetEnd, sessionId));
        }
        else {
          // find index segment closest to the offset. get all entries after that
          Map.Entry<Long, IndexSegmentInfo> entry = indexes.floorEntry(offsetToStart);
          StoreFindToken newToken = null;
          if (entry != null)
            newToken = findEntriesFromOffset(entry.getKey(), null, messageEntries, maxTotalSizeOfEntries);
          else
            newToken = storeToken;
          eliminateDuplicates(messageEntries);
          logger.trace("Index: " + dataDir +
                       " New offset from find info" +
                       " offset : " + (newToken.getOffset() != -1 ? newToken.getOffset() :
                                       newToken.getIndexStartOffset() + ":" + newToken.getStoreKey()));
          return new FindInfo(messageEntries, newToken);
        }
      }
      else {
        // find index segment closest to the offset. get all entries after that
        long prevOffset = storeToken.getIndexStartOffset();
        StoreFindToken newToken = findEntriesFromOffset(prevOffset, storeToken.getStoreKey(), messageEntries, maxTotalSizeOfEntries);
        eliminateDuplicates(messageEntries);
        return new FindInfo(messageEntries, newToken);
      }
    }
    catch (IOException e) {
      logger.error("FindEntriesSince : IO error {}", e);
      throw new StoreException("IOError when finding entries", StoreErrorCodes.IOError);
    }
  }

  private StoreFindToken findEntriesFromOffset(long offset,
                                               StoreKey key,
                                               List<MessageInfo> messageEntries,
                                               long maxTotalSizeOfEntries) throws IOException, StoreException {
    IndexSegmentInfo segment = indexes.get(offset);
    // Use atomic long here to pass by reference
    AtomicLong currentTotalSizeOfEntries = new AtomicLong(0);
    segment.getEntriesSince(key, maxTotalSizeOfEntries, messageEntries, currentTotalSizeOfEntries);
    long lastSegmentIndex = offset;
    long offsetEnd = -1;
    while (currentTotalSizeOfEntries.get() < maxTotalSizeOfEntries && indexes.higherEntry(offset) != null) {
      segment = indexes.higherEntry(offset).getValue();
      offset = segment.getStartOffset();
      IndexSegmentInfo lastSegment = indexes.lastEntry().getValue();
      if (segment != lastSegment) {
        segment.getEntriesSince(null, maxTotalSizeOfEntries, messageEntries, currentTotalSizeOfEntries);
        lastSegmentIndex = segment.getStartOffset();
      }
      else {
        List<JournalEntry> entries = journal.getEntriesSince(lastSegment.getStartOffset(), true);
        if (entries != null) {
          for (JournalEntry entry : entries) {
            offsetEnd = entry.getOffset();
            BlobIndexValue value = findKey(entry.getKey());
            messageEntries.add(new MessageInfo(entry.getKey(),
                                               value.getSize(),
                                               value.isFlagSet(BlobIndexValue.Flags.Delete_Index),
                                               value.getTimeToLiveInMs()));
            currentTotalSizeOfEntries.addAndGet(value.getSize());
            if (currentTotalSizeOfEntries.get() >= maxTotalSizeOfEntries)
              break;
          }
        }
        break;
      }
    }
    if (offsetEnd != -1)
      return new StoreFindToken(offsetEnd, sessionId);
    else
      return new StoreFindToken(messageEntries.get(messageEntries.size() - 1).getStoreKey(),
                                lastSegmentIndex,
                                sessionId);
  }

  /**
   * We can have duplicate entries in the message entries since updates can happen to the same key. For example,
   * insert a key followed by a delete. This would create two entries in the journal or the index. A single findInfo
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
      }
      else {
        setToFindDuplicate.add(messageInfo.getStoreKey());
      }
    }
  }

  /**
   * Closes the index
   * @throws StoreException
   */
  public void close() throws StoreException {
    persistor.write();
    File cleanShutdownFile = new File(dataDir, Clean_Shutdown_Filename);
    try {
      cleanShutdownFile.createNewFile();
    }
    catch (IOException e) {
      logger.error("Index " + dataDir + " error while creating clean shutdown file ", e);
    }
  }

  /**
   * Returns the current end offset that the index represents in the log
   * @return The end offset in the log that this index currently represents
   */
  protected long getCurrentEndOffset() {
    return indexes.size() == 0 ? 0 : indexes.lastEntry().getValue().getEndOffset();
  }

  /**
   * Ensures that the provided fileendoffset satisfies constraints
   * @param fileSpan The filespan that needs to be verified
   */
  private void verifyFileEndOffset(FileSpan fileSpan) {
    if (getCurrentEndOffset() > fileSpan.getStartOffset() || fileSpan.getStartOffset() > fileSpan.getEndOffset()) {
      logger.error("File span offsets provided to the index does not meet constraints " +
                   "logEndOffset {} inputFileStartOffset {} inputFileEndOffset {}",
                   getCurrentEndOffset(), fileSpan.getStartOffset(), fileSpan.getEndOffset());
      throw new IllegalArgumentException("File span offsets provided to the index does not meet constraints " +
                                         "logEndOffset " + getCurrentEndOffset() +
                                         " inputFileStartOffset" + fileSpan.getStartOffset() +
                                         " inputFileEndOffset " + fileSpan.getEndOffset());
    }
  }

  class IndexPersistor implements Runnable {

    /**
     * Writes all the individual index segments to disk. It flushes the log before starting the
     * index flush. The penultimate index segment is flushed if it is not already flushed and mapped.
     * The last index segment is flushed whenever write is invoked.
     * @throws StoreException
     */
    public void write() throws StoreException {
      final Timer.Context context = metrics.indexFlushTime.time();
      try {
        if (indexes.size() > 0) {
          // before iterating the map, get the current file end pointer
          IndexSegmentInfo currentInfo = indexes.lastEntry().getValue();
          long fileEndPointer = log.getLogEndOffset();

          // flush the log to ensure everything till the fileEndPointer is flushed
          log.flush();

          long lastOffset = indexes.lastEntry().getKey();
          IndexSegmentInfo prevInfo = indexes.size() > 1 ? indexes.lowerEntry(lastOffset).getValue() : null;
          while (prevInfo != null && !prevInfo.isMapped()) {
            if (prevInfo.getEndOffset() > fileEndPointer) {
              String message = "The read only index cannot have a file end pointer " + prevInfo.getEndOffset() +
                               " greater than the log end offset " + fileEndPointer;
              logger.error(message);
              throw new StoreException(message, StoreErrorCodes.IOError);
            }
            prevInfo.writeIndexToFile(prevInfo.getEndOffset());
            prevInfo.map(true);
            Map.Entry<Long, IndexSegmentInfo> infoEntry = indexes.lowerEntry(prevInfo.getStartOffset());
            prevInfo = infoEntry != null ? infoEntry.getValue() : null;
          }
          currentInfo.writeIndexToFile(fileEndPointer);
        }
      }
      catch (IOException e) {
        throw new StoreException("IO error while writing index to file", e, StoreErrorCodes.IOError);
      }
      finally {
        context.stop();
      }
    }

    public void run() {
      try {
        write();
      }
      catch (Exception e) {
        logger.info("Error while persisting the index to disk {}", e);
      }
    }
  }
}

/**
 * The StoreFindToken is an implementation of FindToken.
 * It is used to provide a token to the client to resume
 * the find from where it was left previously. The StoreFindToken
 * maintains a offset to track entries within the journal. If the
 * offset gets outside the range of the journal, the storekey and
 * indexstartoffset that refers to the segment of the index is used
 * to perform the search. This is possible because the journal is
 * always equal or larger than the writable segment.
 */
class StoreFindToken implements FindToken {
  private long offset;
  private long indexStartOffset;
  private StoreKey storeKey;
  private UUID sessionId;

  private static final short version = 0;
  private static final int Version_Size = 2;
  private static final int SessionId_Size = 4;
  private static final int Offset_Size = 8;
  private static final int Start_Offset_Size = 8;

  public StoreFindToken() {
    this(-1, -1, null, null);
  }

  public StoreFindToken(StoreKey key, long indexStartOffset, UUID sessionId) {
    this(-1, indexStartOffset, key, sessionId);
  }

  public StoreFindToken(long offset, UUID sessionId) {
    this(offset, -1, null, sessionId);
  }

  private StoreFindToken(long offset, long indexStartOffset, StoreKey key, UUID sessionId) {
    this.offset = offset;
    this.indexStartOffset = indexStartOffset;
    this.storeKey = key;
    this.sessionId = sessionId;
  }

  public static StoreFindToken fromBytes(DataInputStream stream, StoreKeyFactory factory) throws IOException {
    // read version
    short version = stream.readShort();
    // read sessionId
    String sessionId = Utils.readIntString(stream);
    UUID sessionIdUUID = null;
    if (sessionId != null)
      sessionIdUUID = UUID.fromString(sessionId);
    // read offset
    long offset = stream.readLong();
    // read index start offset
    long indexStartOffset = stream.readLong();
    // read store key if needed
    if (indexStartOffset != -1)
      return new StoreFindToken(factory.getStoreKey(stream), indexStartOffset, sessionIdUUID);
    else
      return new StoreFindToken(offset, sessionIdUUID);
  }

  public long getOffset() {
    return offset;
  }

  public StoreKey getStoreKey() {
    return storeKey;
  }

  public long getIndexStartOffset() {
    return indexStartOffset;
  }

  public UUID getSessionId() {
    return sessionId;
  }

  public void setOffset(long offset) {
    this.offset = offset;
    this.storeKey = null;
    this.indexStartOffset = -1;
  }

  @Override
  public byte[] toBytes() {
    int size = Version_Size + SessionId_Size + (sessionId == null ? 0 : sessionId.toString().getBytes().length) +
               Offset_Size + Start_Offset_Size + (storeKey == null ? 0 : storeKey.sizeInBytes());
    byte[] buf = new byte[size];
    ByteBuffer bufWrap = ByteBuffer.wrap(buf);
    // add version
    bufWrap.putShort(version);
    // add sessionId
    bufWrap.putInt(sessionId == null ? 0 : sessionId.toString().length());
    if (sessionId != null)
      bufWrap.put(sessionId.toString().getBytes());
    // add offset
    bufWrap.putLong(offset);
    // add index start offset
    bufWrap.putLong(indexStartOffset);
    // add storekey
    if (storeKey != null)
      bufWrap.put(storeKey.toBytes());
    return buf;
  }

  @Override
  public String toString() {
    String tokenStringFormat = "version: " + version;
    if (sessionId != null) {
      tokenStringFormat += " sessionId " + sessionId;
    }
    if (storeKey != null) {
      tokenStringFormat += " indexStartOffset " + indexStartOffset + " storeKey " + storeKey;
    }
    else {
      tokenStringFormat += " offset " + offset;
    }
    return tokenStringFormat;
  }
}
