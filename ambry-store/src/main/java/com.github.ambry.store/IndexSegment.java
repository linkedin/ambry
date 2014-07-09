package com.github.ambry.store;

import com.github.ambry.config.StoreConfig;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.CrcInputStream;
import com.github.ambry.utils.CrcOutputStream;
import com.github.ambry.utils.FilterFactory;
import com.github.ambry.utils.IFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
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
class IndexSegment {
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
  protected ConcurrentSkipListMap<StoreKey, IndexValue> index = null;
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
  public IndexSegment(String dataDir, long startOffset, StoreKeyFactory factory, int keySize, int valueSize,
      StoreConfig config, StoreMetrics metrics) {
    // create a new file with the start offset
    indexFile = new File(dataDir, startOffset + "_" + PersistentIndex.Index_File_Name_Suffix);
    bloomFile = new File(dataDir, startOffset + "_" + PersistentIndex.Bloom_File_Name_Suffix);
    this.rwLock = new ReentrantReadWriteLock();
    this.startOffset = new AtomicLong(startOffset);
    this.endOffset = new AtomicLong(-1);
    index = new ConcurrentSkipListMap<StoreKey, IndexValue>();
    mapped = new AtomicBoolean(false);
    sizeWritten = new AtomicLong(0);
    this.factory = factory;
    this.keySize = keySize;
    this.valueSize = valueSize;
    bloomFilter = FilterFactory
        .getFilter(config.storeIndexMaxNumberOfInmemElements, config.storeIndexBloomMaxFalsePositiveProbability);
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
  public IndexSegment(File indexFile, boolean isMapped, StoreKeyFactory factory, StoreConfig config,
      StoreMetrics metrics, InMemoryJournal journal)
      throws StoreException {
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
        bloomFile = new File(indexFile.getParent(), startOffset + "_" + PersistentIndex.Bloom_File_Name_Suffix);
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
      } else {
        index = new ConcurrentSkipListMap<StoreKey, IndexValue>();
        bloomFilter = FilterFactory
            .getFilter(config.storeIndexMaxNumberOfInmemElements, config.storeIndexBloomMaxFalsePositiveProbability);
        bloomFile = new File(indexFile.getParent(), startOffset + "_" + PersistentIndex.Bloom_File_Name_Suffix);
        try {
          readFromFile(indexFile, journal);
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
    } catch (Exception e) {
      logger.error("Error while loading index from file", e);
      throw new StoreException("Error while loading index from file Exception: " + e,
          StoreErrorCodes.Index_Creation_Failure);
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
  public IndexValue find(StoreKey keyToFind)
      throws StoreException {
    try {
      rwLock.readLock().lock();
      if (!(mapped.get())) {
        return index.get(keyToFind);
      } else {
        boolean bloomSaysYes = false;
        // check bloom filter first
        if (bloomFilter == null || bloomFilter.isPresent(ByteBuffer.wrap(keyToFind.toBytes()))) {
          bloomSaysYes = true;
          metrics.bloomPositiveCount.inc(1);
          logger.trace(bloomFilter == null ? "Bloom filter empty. Searching file with start offset {} and for key {} "
              : "Found in bloom filter for index with start offset {} and for key {} ", startOffset.get(), keyToFind);
          // binary search on the mapped file
          ByteBuffer duplicate = mmap.duplicate();
          int low = 0;
          int high = numberOfEntries(duplicate) - 1;
          logger.trace("binary search low : {} high : {}", low, high);
          while (low <= high) {
            int mid = (int) (Math.ceil(high / 2.0 + low / 2.0));
            StoreKey found = getKeyAt(duplicate, mid);
            logger.trace("Binary search - key found on iteration {}", found);
            int result = found.compareTo(keyToFind);
            if (result == 0) {
              byte[] buf = new byte[valueSize];
              duplicate.get(buf);
              return new IndexValue(ByteBuffer.wrap(buf));
            } else if (result < 0) {
              low = mid + 1;
            } else {
              high = mid - 1;
            }
          }
        }
        if (bloomSaysYes) {
          metrics.bloomFalsePositiveCount.inc(1);
        }
        return null;
      }
    } catch (IOException e) {
      logger.error("IO error while searching the index {}", indexFile.getAbsoluteFile());
      throw new StoreException("IO error while searching the index " + indexFile.getAbsolutePath(), e,
          StoreErrorCodes.IOError);
    } finally {
      rwLock.readLock().unlock();
    }
  }

  private int numberOfEntries(ByteBuffer mmap) {
    return (mmap.capacity() - Index_Size_Excluding_Entries) / (keySize + valueSize);
  }

  private StoreKey getKeyAt(ByteBuffer mmap, int index)
      throws IOException {
    mmap.position(Version_Size + Key_Size + Value_Size + Log_End_Offset_Size + (index * (keySize + valueSize)));
    return factory.getStoreKey(new DataInputStream(new ByteBufferInputStream(mmap)));
  }

  private int findIndex(StoreKey keyToFind, ByteBuffer mmap)
      throws IOException {
    // binary search on the mapped file
    int low = 0;
    int high = numberOfEntries(mmap) - 1;
    logger.trace("binary search low : {} high : {}", low, high);
    while (low <= high) {
      int mid = (int) (Math.ceil(high / 2.0 + low / 2.0));
      StoreKey found = getKeyAt(mmap, mid);
      logger.trace("Binary search - key found on iteration {}", found);
      int result = found.compareTo(keyToFind);
      if (result == 0) {
        return mid;
      } else if (result < 0) {
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }
    return -1;
  }

  /**
   * Adds an entry into the segment. The operation works only if the segment is read/write
   * @param entry The entry that needs to be added to the segment.
   * @param fileEndOffset The file end offset that this entry represents.
   * @throws StoreException
   */
  public void addEntry(IndexEntry entry, long fileEndOffset)
      throws StoreException {
    try {
      rwLock.readLock().lock();
      if (mapped.get()) {
        throw new StoreException("Cannot add to a mapped index " + indexFile.getAbsolutePath(),
            StoreErrorCodes.Illegal_Index_Operation);
      }
      logger.trace("Inserting key {} value offset {} size {} ttl {} originalMessageOffset {} fileEndOffset {}",
          entry.getKey(), entry.getValue().getOffset(), entry.getValue().getSize(),
          entry.getValue().getTimeToLiveInMs(), entry.getValue().getOriginalMessageOffset(), fileEndOffset);
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
    } finally {
      rwLock.readLock().unlock();
    }
  }

  /**
   * Adds a list of entries into the segment. The operation works only if the segment is read/write
   * @param entries The entries that needs to be added to the segment.
   * @param fileEndOffset The file end offset of the last entry in the entries list
   * @throws StoreException
   */
  public void addEntries(ArrayList<IndexEntry> entries, long fileEndOffset)
      throws StoreException {
    try {
      rwLock.readLock().lock();
      if (mapped.get()) {
        throw new StoreException("Cannot add to a mapped index" + indexFile.getAbsolutePath(),
            StoreErrorCodes.Illegal_Index_Operation);
      }
      if (entries.size() == 0) {
        throw new IllegalArgumentException("No entries to add to the index. The entries provided is empty");
      }
      for (IndexEntry entry : entries) {
        logger.trace("Inserting key {} value offset {} size {} ttl {} originalMessageOffset {} fileEndOffset {}",
            entry.getKey(), entry.getValue().getOffset(), entry.getValue().getSize(),
            entry.getValue().getTimeToLiveInMs(), entry.getValue().getOriginalMessageOffset(), fileEndOffset);
        index.put(entry.getKey(), entry.getValue());
        sizeWritten.addAndGet(entry.getKey().sizeInBytes() + IndexValue.Index_Value_Size_In_Bytes);
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
    } finally {
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
      if (mapped.get()) {
        throw new UnsupportedOperationException("Operation supported only on umapped indexes");
      }
      return sizeWritten.get();
    } finally {
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
      if (mapped.get()) {
        throw new UnsupportedOperationException("Operation supported only on unmapped indexes");
      }
      return numberOfItems.get();
    } finally {
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
  public void writeIndexToFile(long fileEndPointer)
      throws IOException, StoreException {
    if (prevNumOfEntriesWritten != index.size()) {
      File temp = new File(getFile().getAbsolutePath() + ".tmp");
      FileOutputStream fileStream = new FileOutputStream(temp);
      CrcOutputStream crc = new CrcOutputStream(fileStream);
      DataOutputStream writer = new DataOutputStream(crc);
      try {
        rwLock.readLock().lock();

        // write the current version
        writer.writeShort(PersistentIndex.version);
        // write key, value size and file end pointer for this index
        writer.writeInt(this.keySize);
        writer.writeInt(this.valueSize);
        writer.writeLong(fileEndPointer);

        int numOfEntries = 0;
        // write the entries
        for (Map.Entry<StoreKey, IndexValue> entry : index.entrySet()) {
          writer.write(entry.getKey().toBytes());
          writer.write(entry.getValue().getBytes().array());
          logger.trace("Index {} writing key {} offset {} size {} fileEndOffset {}", getFile().getAbsolutePath(),
              entry.getKey(), entry.getValue().getOffset(), entry.getValue().getOffset(), fileEndPointer);
          numOfEntries++;
        }
        prevNumOfEntriesWritten = numOfEntries;
        long crcValue = crc.getValue();
        writer.writeLong(crcValue);

        // flush and overwrite old file
        fileStream.getChannel().force(true);
        // swap temp file with the original file
        temp.renameTo(getFile());
      } catch (IOException e) {
        logger.error("IO error while persisting index to disk " + indexFile.getAbsoluteFile());
        throw new StoreException("IO error while persisting index to disk " + indexFile.getAbsolutePath(), e,
            StoreErrorCodes.IOError);
      } finally {
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
  public void map(boolean persistBloom)
      throws IOException, StoreException {
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
  private void readFromFile(File fileToRead, InMemoryJournal journal)
      throws StoreException, IOException {
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
          long logEndOffset = stream.readLong();
          logger.trace("Index {} Reading log end offset from file {}", indexFile.getPath(), logEndOffset);
          long maxEndOffset = Long.MIN_VALUE;
          while (stream.available() > Crc_Size) {
            StoreKey key = factory.getStoreKey(stream);
            byte[] value = new byte[IndexValue.Index_Value_Size_In_Bytes];
            stream.read(value);
            IndexValue blobValue = new IndexValue(ByteBuffer.wrap(value));
            // ignore entries that have offsets outside the log end offset that this index represents
            if (blobValue.getOffset() + blobValue.getSize() <= logEndOffset) {
              index.put(key, blobValue);
              logger.trace("Index {} Putting key {} in index offset {} size {}", indexFile.getPath(), key,
                  blobValue.getOffset(), blobValue.getSize());
              // regenerate the bloom filter for in memory indexes
              bloomFilter.add(ByteBuffer.wrap(key.toBytes()));
              // add to the journal
              journal.addEntry(blobValue.getOffset(), key);
              sizeWritten.addAndGet(key.sizeInBytes() + IndexValue.Index_Value_Size_In_Bytes);
              numberOfItems.incrementAndGet();
              if (blobValue.getOffset() + blobValue.getSize() > maxEndOffset) {
                maxEndOffset = blobValue.getOffset() + blobValue.getSize();
              }
            } else {
              logger.info(
                  "Index {} Ignoring index entry outside the log end offset that was not synced logEndOffset {} "
                      + "key {} entryOffset {} entrySize {} entryDeleteState {}", indexFile.getPath(), logEndOffset,
                  key, blobValue.getOffset(), blobValue.getSize(), blobValue.isFlagSet(IndexValue.Flags.Delete_Index));
            }
          }
          if (maxEndOffset != logEndOffset) {
            logger.error("Index {} MaxEndOffset {} of index entries does not match the log end offset {} in index ",
                indexFile.getPath(), maxEndOffset, logEndOffset);
            throw new StoreException("Index inconsistency error", StoreErrorCodes.Initialization_Error);
          }
          this.endOffset.set(maxEndOffset);
          logger.trace("Index {} Setting end offset for index {}", indexFile.getPath(), maxEndOffset);
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
    } catch (IOException e) {
      throw new StoreException("IO error while reading from file " + indexFile.getAbsolutePath(), e,
          StoreErrorCodes.IOError);
    } finally {
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
  public void getEntriesSince(StoreKey key, long maxTotalSizeOfEntriesInBytes, List<MessageInfo> entries,
      AtomicLong currentTotalSizeOfEntriesInBytes)
      throws IOException {
    if (mapped.get()) {
      int index = 0;
      if (key != null) {
        index = findIndex(key, mmap.duplicate());
      }
      if (index != -1) {
        ByteBuffer readBuf = mmap.duplicate();
        int totalEntries = numberOfEntries(readBuf);
        while (currentTotalSizeOfEntriesInBytes.get() < maxTotalSizeOfEntriesInBytes && index < totalEntries) {
          StoreKey newKey = getKeyAt(readBuf, index);
          byte[] buf = new byte[valueSize];
          readBuf.get(buf);
          if (key == null || newKey.compareTo(key) != 0) {
            IndexValue newValue = new IndexValue(ByteBuffer.wrap(buf));
            MessageInfo info =
                new MessageInfo(newKey, newValue.getSize(), newValue.isFlagSet(IndexValue.Flags.Delete_Index),
                    newValue.getTimeToLiveInMs());
            entries.add(info);
            currentTotalSizeOfEntriesInBytes.addAndGet(newValue.getSize());
          }
          index++;
        }
      }
    } else {
      ConcurrentNavigableMap<StoreKey, IndexValue> tempMap = index;
      if (key != null) {
        tempMap = index.tailMap(key, true);
      }
      for (Map.Entry<StoreKey, IndexValue> entry : tempMap.entrySet()) {
        if (key == null || entry.getKey().compareTo(key) != 0) {
          MessageInfo info = new MessageInfo(entry.getKey(), entry.getValue().getSize(),
              entry.getValue().isFlagSet(IndexValue.Flags.Delete_Index), entry.getValue().getTimeToLiveInMs());
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

