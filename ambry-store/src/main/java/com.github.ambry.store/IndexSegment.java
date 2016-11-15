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

import com.github.ambry.config.StoreConfig;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.CrcInputStream;
import com.github.ambry.utils.CrcOutputStream;
import com.github.ambry.utils.FilterFactory;
import com.github.ambry.utils.IFilter;
import com.github.ambry.utils.SystemTime;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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

  private final static int Version_Field_Length = 2;
  private final static int Key_Size_Field_Length = 4;
  private final static int Value_Size_Field_Length = 4;
  private final static int Crc_Field_Length = 8;
  private final static int Log_End_Offset_Field_Length = 8;
  private final static int Index_Size_Excluding_Entries =
      Version_Field_Length + Key_Size_Field_Length + Value_Size_Field_Length + Log_End_Offset_Field_Length
          + Crc_Field_Length;

  private int keySize;
  private int valueSize;
  private File bloomFile;
  private long prevSegmentEndOffset = 0;
  private AtomicLong lastModifiedTimeSec; // an approximation of the last modified time.
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
    bloomFilter = FilterFactory.getFilter(config.storeIndexMaxNumberOfInmemElements,
        config.storeIndexBloomMaxFalsePositiveProbability);
    numberOfItems = new AtomicInteger(0);
    this.metrics = metrics;
    this.lastModifiedTimeSec = new AtomicLong(0);
  }

  /**
   * Initializes an existing segment. Memory maps the segment or reads the segment into memory. Also reads the
   * persisted bloom filter from disk.
   * @param indexFile The index file that the segment needs to be initialized from
   * @param isMapped Indicates if the segment needs to be memory mapped
   * @param factory The store key factory used to create new store keys
   * @param config The store config used to initialize the index segment
   * @param metrics The store metrics used to track metrics
   * @param journal The journal to use
   * @throws StoreException
   */
  public IndexSegment(File indexFile, boolean isMapped, StoreKeyFactory factory, StoreConfig config,
      StoreMetrics metrics, Journal journal) throws StoreException {
    try {
      int startIndex = indexFile.getName().indexOf("_", 0);
      String startOffsetValue = indexFile.getName().substring(0, startIndex);
      startOffset = new AtomicLong(Long.parseLong(startOffsetValue));
      endOffset = new AtomicLong(-1);
      this.indexFile = indexFile;
      this.lastModifiedTimeSec = new AtomicLong(indexFile.lastModified() / 1000);
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
          logger.error("IndexSegment : {} error validating crc for bloom filter for {}", indexFile.getAbsolutePath(),
              bloomFile.getAbsolutePath());
        }
        stream.close();
      } else {
        index = new ConcurrentSkipListMap<StoreKey, IndexValue>();
        bloomFilter = FilterFactory.getFilter(config.storeIndexMaxNumberOfInmemElements,
            config.storeIndexBloomMaxFalsePositiveProbability);
        bloomFile = new File(indexFile.getParent(), startOffset + "_" + PersistentIndex.Bloom_File_Name_Suffix);
        try {
          readFromFile(indexFile, journal);
        } catch (StoreException e) {
          if (e.getErrorCode() == StoreErrorCodes.Index_Creation_Failure
              || e.getErrorCode() == StoreErrorCodes.Index_Version_Error) {
            // we just log the error here and retain the index so far created.
            // subsequent recovery process will add the missed out entries
            logger.error("Index Segment : {} error while reading from index {}", indexFile.getAbsolutePath(),
                e.getMessage());
          } else {
            throw e;
          }
        }
      }
    } catch (Exception e) {
      throw new StoreException(
          "Index Segment : " + indexFile.getAbsolutePath() + " error while loading index from file Exception: "
              + e.getMessage(), StoreErrorCodes.Index_Creation_Failure);
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
   * The time of last modification of this segment
   * @return The time in seconds of the last modification of this segment.
   */
  public long getLastModifiedTime() {
    return lastModifiedTimeSec.get();
  }

  /**
   * Finds an entry given a key. It finds from the in memory map or
   * does a binary search on the mapped persistent segment
   * @param keyToFind The key to find
   * @return The blob index value that represents the key or null if not found
   * @throws StoreException
   */
  public IndexValue find(StoreKey keyToFind) throws StoreException {
    try {
      rwLock.readLock().lock();
      if (!(mapped.get())) {
        return index.get(keyToFind);
      } else {
        // check bloom filter first
        if (bloomFilter == null || bloomFilter.isPresent(ByteBuffer.wrap(keyToFind.toBytes()))) {
          metrics.bloomPositiveCount.inc(1);
          logger.trace(bloomFilter == null
                  ? "IndexSegment {} bloom filter empty. Searching file with start offset {} and for key {} "
                  : "IndexSegment {} found in bloom filter for index with start offset {} and for key {} ",
              indexFile.getAbsolutePath(), startOffset.get(), keyToFind);
          // binary search on the mapped file
          ByteBuffer duplicate = mmap.duplicate();
          int low = 0;
          int high = numberOfEntries(duplicate) - 1;
          logger.trace("binary search low : {} high : {}", low, high);
          while (low <= high) {
            int mid = (int) (Math.ceil(high / 2.0 + low / 2.0));
            StoreKey found = getKeyAt(duplicate, mid);
            logger.trace("Index Segment {} binary search - key found on iteration {}", indexFile.getAbsolutePath(),
                found);
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
          metrics.bloomFalsePositiveCount.inc(1);
        }
        return null;
      }
    } catch (IOException e) {
      throw new StoreException("IndexSegment : " + indexFile.getAbsolutePath() + " IO error while searching", e,
          StoreErrorCodes.IOError);
    } finally {
      rwLock.readLock().unlock();
    }
  }

  private int numberOfEntries(ByteBuffer mmap) {
    return (mmap.capacity() - Index_Size_Excluding_Entries) / (keySize + valueSize);
  }

  private StoreKey getKeyAt(ByteBuffer mmap, int index) throws IOException {
    mmap.position(
        Version_Field_Length + Key_Size_Field_Length + Value_Size_Field_Length + Log_End_Offset_Field_Length + (index
            * (keySize + valueSize)));
    return factory.getStoreKey(new DataInputStream(new ByteBufferInputStream(mmap)));
  }

  private int findIndex(StoreKey keyToFind, ByteBuffer mmap) throws IOException {
    // binary search on the mapped file
    int low = 0;
    int high = numberOfEntries(mmap) - 1;
    logger.trace("IndexSegment {} binary search low : {} high : {}", indexFile.getAbsolutePath(), low, high);
    while (low <= high) {
      int mid = (int) (Math.ceil(high / 2.0 + low / 2.0));
      StoreKey found = getKeyAt(mmap, mid);
      logger.trace("IndexSegment {} binary search - key found on iteration {}", indexFile.getAbsolutePath(), found);
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
  public void addEntry(IndexEntry entry, long fileEndOffset) throws StoreException {
    try {
      rwLock.readLock().lock();
      if (mapped.get()) {
        throw new StoreException("IndexSegment : " + indexFile.getAbsolutePath() + " cannot add to a mapped index ",
            StoreErrorCodes.Illegal_Index_Operation);
      }
      logger.trace("IndexSegment {} inserting key - {} value - offset {} size {} ttl {} "
              + "originalMessageOffset {} fileEndOffset {}", indexFile.getAbsolutePath(), entry.getKey(),
          entry.getValue().getOffset(), entry.getValue().getSize(), entry.getValue().getTimeToLiveInMs(),
          entry.getValue().getOriginalMessageOffset(), fileEndOffset);
      if (index.put(entry.getKey(), entry.getValue()) == null) {
        numberOfItems.incrementAndGet();
        sizeWritten.addAndGet(entry.getKey().sizeInBytes() + IndexValue.Index_Value_Size_In_Bytes);
        bloomFilter.add(ByteBuffer.wrap(entry.getKey().toBytes()));
      }
      endOffset.set(fileEndOffset);
      lastModifiedTimeSec.set(SystemTime.getInstance().milliseconds() / 1000);
      if (keySize == Key_Size_Invalid_Value) {
        StoreKey key = entry.getKey();
        keySize = key.sizeInBytes();
        logger.info("IndexSegment : {} setting key size to {} of key {} for index with start offset {}",
            indexFile.getAbsolutePath(), key.sizeInBytes(), key.getLongForm(), startOffset);
      }
      if (valueSize == Value_Size_Invalid_Value) {
        valueSize = entry.getValue().getBytes().capacity();
        logger.info("IndexSegment : {} setting value size to {} for index with start offset {}",
            indexFile.getAbsolutePath(), valueSize, startOffset);
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
  public void addEntries(ArrayList<IndexEntry> entries, long fileEndOffset) throws StoreException {
    try {
      rwLock.readLock().lock();
      if (mapped.get()) {
        throw new StoreException("IndexSegment : " + indexFile.getAbsolutePath() + " cannot add to a mapped index",
            StoreErrorCodes.Illegal_Index_Operation);
      }
      if (entries.size() == 0) {
        throw new IllegalArgumentException("IndexSegment : " + indexFile.getAbsolutePath()
            + " no entries to add to the index. The entries provided is empty");
      }
      for (IndexEntry entry : entries) {
        logger.trace("IndexSegment {} Inserting key - {} value - offset {} size {} ttl {} "
                + "originalMessageOffset {} fileEndOffset {}", indexFile.getAbsolutePath(), entry.getKey(),
            entry.getValue().getOffset(), entry.getValue().getSize(), entry.getValue().getTimeToLiveInMs(),
            entry.getValue().getOriginalMessageOffset(), fileEndOffset);
        if (index.put(entry.getKey(), entry.getValue()) == null) {
          numberOfItems.incrementAndGet();
          sizeWritten.addAndGet(entry.getKey().sizeInBytes() + IndexValue.Index_Value_Size_In_Bytes);
          bloomFilter.add(ByteBuffer.wrap(entry.getKey().toBytes()));
        }
      }
      endOffset.set(fileEndOffset);
      if (keySize == Key_Size_Invalid_Value) {
        StoreKey key = entries.get(0).getKey();
        keySize = key.sizeInBytes();
        logger.info("IndexSegment : {} setting key size to {} of key {} for index with start offset {}",
            indexFile.getAbsolutePath(), key.sizeInBytes(), key.getLongForm(), startOffset);
      }
      if (valueSize == Value_Size_Invalid_Value) {
        valueSize = entries.get(0).getValue().getBytes().capacity();
        logger.info("IndexSegment : {} setting value size to {} for index with start offset {}",
            indexFile.getAbsolutePath(), valueSize, startOffset);
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
   * @param safeEndPoint
   * @throws IOException
   * @throws StoreException
   */
  public void writeIndexToFile(long safeEndPoint) throws IOException, StoreException {
    if (prevSegmentEndOffset != safeEndPoint) {
      if (safeEndPoint > getEndOffset()) {
        throw new StoreException(
            "SafeEndOffSet " + safeEndPoint + " is greater than current end offset for current " + "index segment "
                + getEndOffset(), StoreErrorCodes.Illegal_Index_Operation);
      }
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
        writer.writeLong(safeEndPoint);

        // write the entries
        for (Map.Entry<StoreKey, IndexValue> entry : index.entrySet()) {
          if (entry.getValue().getOffset() + entry.getValue().getSize() <= safeEndPoint) {
            writer.write(entry.getKey().toBytes());
            writer.write(entry.getValue().getBytes().array());
            logger.trace("IndexSegment : {} writing key - {} value - offset {} size {} fileEndOffset {}",
                getFile().getAbsolutePath(), entry.getKey(), entry.getValue().getOffset(), entry.getValue().getSize(),
                safeEndPoint);
          }
        }
        prevSegmentEndOffset = safeEndPoint;
        long crcValue = crc.getValue();
        writer.writeLong(crcValue);

        // flush and overwrite old file
        fileStream.getChannel().force(true);
        // swap temp file with the original file
        temp.renameTo(getFile());
      } catch (IOException e) {
        throw new StoreException(
            "IndexSegment : " + indexFile.getAbsolutePath() + " IO error while persisting index to disk", e,
            StoreErrorCodes.IOError);
      } finally {
        writer.close();
        rwLock.readLock().unlock();
      }
      logger.trace("IndexSegment : {} completed writing index to file", indexFile.getAbsolutePath());
    }
  }

  /**
   * Memory maps the segment of index. Optionally, it also persist the bloom filter to disk
   * @param persistBloom True, if the bloom filter needs to be persisted. False otherwise.
   * @throws IOException
   * @throws StoreException
   */
  public void map(boolean persistBloom) throws IOException, StoreException {
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
          throw new StoreException("IndexSegment : " + indexFile.getAbsolutePath() + " unknown version in index file",
              StoreErrorCodes.Index_Version_Error);
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
   * @param journal The journal to use.
   * @throws StoreException
   * @throws IOException
   */
  private void readFromFile(File fileToRead, Journal journal) throws StoreException, IOException {
    logger.info("IndexSegment : {} reading index from file", indexFile.getAbsolutePath());
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
          logger.trace("IndexSegment : {} reading log end offset {} from file", indexFile.getAbsolutePath(),
              logEndOffset);
          long maxEndOffset = Long.MIN_VALUE;
          while (stream.available() > Crc_Field_Length) {
            StoreKey key = factory.getStoreKey(stream);
            byte[] value = new byte[IndexValue.Index_Value_Size_In_Bytes];
            stream.read(value);
            IndexValue blobValue = new IndexValue(ByteBuffer.wrap(value));
            // ignore entries that have offsets outside the log end offset that this index represents
            if (blobValue.getOffset() + blobValue.getSize() <= logEndOffset) {
              index.put(key, blobValue);
              logger.trace("IndexSegment : {} putting key {} in index offset {} size {}", indexFile.getAbsolutePath(),
                  key, blobValue.getOffset(), blobValue.getSize());
              // regenerate the bloom filter for in memory indexes
              bloomFilter.add(ByteBuffer.wrap(key.toBytes()));
              // add to the journal
              if (blobValue.getOffset() != blobValue.getOriginalMessageOffset()
                  && blobValue.getOriginalMessageOffset() >= startOffset.get()) {
                // we add an entry for the original message offset if it is within the same index segment
                journal.addEntry(blobValue.getOriginalMessageOffset(), key);
              }
              journal.addEntry(blobValue.getOffset(), key);
              sizeWritten.addAndGet(key.sizeInBytes() + IndexValue.Index_Value_Size_In_Bytes);
              numberOfItems.incrementAndGet();
              if (blobValue.getOffset() + blobValue.getSize() > maxEndOffset) {
                maxEndOffset = blobValue.getOffset() + blobValue.getSize();
              }
            } else {
              logger.info(
                  "IndexSegment : {} ignoring index entry outside the log end offset that was not synced logEndOffset {} "
                      + "key {} entryOffset {} entrySize {} entryDeleteState {}", indexFile.getAbsolutePath(),
                  logEndOffset, key, blobValue.getOffset(), blobValue.getSize(),
                  blobValue.isFlagSet(IndexValue.Flags.Delete_Index));
            }
          }
          this.endOffset.set(maxEndOffset);
          logger.trace("IndexSegment : {} setting end offset for index {}", indexFile.getAbsolutePath(), maxEndOffset);
          long crc = crcStream.getValue();
          if (crc != stream.readLong()) {
            // reset structures
            this.keySize = Key_Size_Invalid_Value;
            this.valueSize = Value_Size_Invalid_Value;
            this.endOffset.set(0);
            index.clear();
            bloomFilter.clear();
            throw new StoreException("IndexSegment : " + indexFile.getAbsolutePath() + " crc check does not match",
                StoreErrorCodes.Index_Creation_Failure);
          }
          break;
        default:
          throw new StoreException("IndexSegment : " + indexFile.getAbsolutePath() + " invalid version in index file",
              StoreErrorCodes.Index_Version_Error);
      }
    } catch (IOException e) {
      throw new StoreException("IndexSegment : " + indexFile.getAbsolutePath() + " IO error while reading from file ",
          e, StoreErrorCodes.IOError);
    } finally {
      stream.close();
    }
  }

  /**
   * Gets all the entries upto maxEntries from the start of a given key (exclusive) or all entries if key is null,
   * till maxTotalSizeOfEntriesInBytes
   * @param key The key from where to start retrieving entries.
   *            If the key is null, all entries are retrieved upto maxentries
   * @param findEntriesCondition The condition that determines when to stop fetching entries.
   * @param entries The input entries list that needs to be filled. The entries list can have existing entries
   * @param currentTotalSizeOfEntriesInBytes The current total size in bytes of the entries
   * @return true if any entries were added.
   * @throws IOException
   */
  public boolean getEntriesSince(StoreKey key, FindEntriesCondition findEntriesCondition, List<MessageInfo> entries,
      AtomicLong currentTotalSizeOfEntriesInBytes) throws IOException {
    int entriesSizeAtStart = entries.size();
    if (mapped.get()) {
      int index = 0;
      if (key != null) {
        index = findIndex(key, mmap.duplicate());
      }
      if (index != -1) {
        ByteBuffer readBuf = mmap.duplicate();
        int totalEntries = numberOfEntries(readBuf);
        while (findEntriesCondition.proceed(currentTotalSizeOfEntriesInBytes.get(), this.getLastModifiedTime())
            && index < totalEntries) {
          StoreKey newKey = getKeyAt(readBuf, index);
          byte[] buf = new byte[valueSize];
          readBuf.get(buf);
          // we include the key in the final list if it is not the initial key or if the initial key was null
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
      } else {
        logger.error("IndexSegment : " + indexFile.getAbsolutePath() + " index not found for key " + key);
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
          if (!findEntriesCondition.proceed(currentTotalSizeOfEntriesInBytes.get(), this.getLastModifiedTime())) {
            break;
          }
        }
      }
    }
    return entries.size() > entriesSizeAtStart;
  }
}

