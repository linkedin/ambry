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
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
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
  static final String INDEX_SEGMENT_FILE_NAME_SUFFIX = "index";
  static final String BLOOM_FILE_NAME_SUFFIX = "bloom";

  private final static int ENTRY_SIZE_INVALID_VALUE = -1;
  private final static int VALUE_SIZE_INVALID_VALUE = -1;

  private final int VERSION_FIELD_LENGTH = 2;
  private final int KEY_OR_ENTRY_SIZE_FIELD_LENGTH = 4;
  private final int VALUE_SIZE_FIELD_LENGTH = 4;
  private final int CRC_FIELD_LENGTH = 8;
  private final int LOG_END_OFFSET_FIELD_LENGTH = 8;
  private final int LAST_MODIFIED_TIME_FIELD_LENGTH = 8;
  private final int RESET_KEY_TYPE_FIELD_LENGTH = 2;

  private int indexSizeExcludingEntries;
  private int firstKeyRelativeOffset;
  private final StoreConfig config;
  private final String indexSegmentFilenamePrefix;
  private final Offset startOffset;
  private final AtomicReference<Offset> endOffset;
  private final File indexFile;
  private final ReadWriteLock rwLock;
  private final AtomicBoolean mapped;
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final AtomicLong sizeWritten;
  private final StoreKeyFactory factory;
  private final File bloomFile;
  private final StoreMetrics metrics;
  private final AtomicInteger numberOfItems;
  private final Time time;

  // an approximation of the last modified time.
  private final AtomicLong lastModifiedTimeSec;
  private MappedByteBuffer mmap = null;
  private IFilter bloomFilter;
  private int valueSize;
  private int persistedEntrySize;
  private short version;
  private Offset prevSafeEndPoint = null;
  // reset key refers to the first StoreKey that is added to the index segment
  private Pair<StoreKey, PersistentIndex.IndexEntryType> resetKey = null;
  protected ConcurrentSkipListMap<StoreKey, IndexValue> index = null;

  /**
   * Creates a new segment
   * @param dataDir The data directory to use for this segment
   * @param startOffset The start {@link Offset} in the {@link Log} that this segment represents.
   * @param factory The store key factory used to create new store keys
   * @param entrySize The size of entries that this segment needs to support. The actual supported entry size for the
   *                  constructed segment is the max of this value and {@link StoreConfig#storeIndexPersistedEntryMinBytes}.
   *                  The constructed index segment guarantees to support entries that are of this size or smaller.
   * @param valueSize The value size that this segment supports. All entries in a segment must have the same value sizes.
   * @param config The store config used to initialize the index segment
   * @param time the {@link Time} instance to use
   */
  IndexSegment(String dataDir, Offset startOffset, StoreKeyFactory factory, int entrySize, int valueSize,
      StoreConfig config, StoreMetrics metrics, Time time) {
    this.rwLock = new ReentrantReadWriteLock();
    this.config = config;
    this.startOffset = startOffset;
    this.endOffset = new AtomicReference<>(startOffset);
    index = new ConcurrentSkipListMap<>();
    mapped = new AtomicBoolean(false);
    sizeWritten = new AtomicLong(0);
    this.factory = factory;
    this.version = PersistentIndex.CURRENT_VERSION;
    this.valueSize = valueSize;
    this.persistedEntrySize = Math.max(config.storeIndexPersistedEntryMinBytes, entrySize);
    bloomFilter = FilterFactory.getFilter(config.storeIndexMaxNumberOfInmemElements,
        config.storeIndexBloomMaxFalsePositiveProbability);
    numberOfItems = new AtomicInteger(0);
    this.metrics = metrics;
    this.time = time;
    lastModifiedTimeSec = new AtomicLong(time.seconds());
    indexSegmentFilenamePrefix = generateIndexSegmentFilenamePrefix();
    indexFile = new File(dataDir, indexSegmentFilenamePrefix + INDEX_SEGMENT_FILE_NAME_SUFFIX);
    bloomFile = new File(dataDir, indexSegmentFilenamePrefix + BLOOM_FILE_NAME_SUFFIX);
  }

  /**
   * Initializes an existing segment. Memory maps the segment or reads the segment into memory. Also reads the
   * persisted bloom filter from disk.
   * @param indexFile The index file that the segment needs to be initialized from
   * @param shouldMap Indicates if the segment needs to be memory mapped
   * @param factory The store key factory used to create new store keys
   * @param config The store config used to initialize the index segment
   * @param metrics The store metrics used to track metrics
   * @param journal The journal to use
   * @param time the {@link Time} instance to use
   * @throws StoreException
   */
  IndexSegment(File indexFile, boolean shouldMap, StoreKeyFactory factory, StoreConfig config, StoreMetrics metrics,
      Journal journal, Time time) throws StoreException {
    try {
      this.config = config;
      startOffset = getIndexSegmentStartOffset(indexFile.getName());
      endOffset = new AtomicReference<>(startOffset);
      indexSegmentFilenamePrefix = generateIndexSegmentFilenamePrefix();
      this.indexFile = indexFile;
      this.rwLock = new ReentrantReadWriteLock();
      this.factory = factory;
      this.time = time;
      sizeWritten = new AtomicLong(0);
      numberOfItems = new AtomicInteger(0);
      mapped = new AtomicBoolean(false);
      lastModifiedTimeSec = new AtomicLong(0);
      if (shouldMap) {
        map(false);
        // Load the bloom filter for this index
        // We need to load the bloom filter only for mapped indexes
        bloomFile = new File(indexFile.getParent(), indexSegmentFilenamePrefix + BLOOM_FILE_NAME_SUFFIX);
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
        bloomFile = new File(indexFile.getParent(), indexSegmentFilenamePrefix + BLOOM_FILE_NAME_SUFFIX);
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
          "Index Segment : " + indexFile.getAbsolutePath() + " error while loading index from file", e,
          StoreErrorCodes.Index_Creation_Failure);
    }
    this.metrics = metrics;
  }

  /**
   * @return the name of the log segment that this index segment refers to;
   */
  String getLogSegmentName() {
    return startOffset.getName();
  }

  /**
   * The start offset that this segment represents
   * @return The start offset that this segment represents
   */
  Offset getStartOffset() {
    return startOffset;
  }

  /**
   * The end offset that this segment represents
   * @return The end offset that this segment represents
   */
  Offset getEndOffset() {
    return endOffset.get();
  }

  /**
   * Returns if this segment is mapped or not
   * @return True, if the segment is readonly and mapped. False, otherwise
   */
  boolean isMapped() {
    return mapped.get();
  }

  /**
   * The underlying file that this segment represents
   * @return The file that this segment represents
   */
  File getFile() {
    return indexFile;
  }

  /**
   * The persisted entry size of this segment
   * @return The persisted entry size of this segment
   */
  int getPersistedEntrySize() {
    return persistedEntrySize;
  }

  /**
   * The value size in this segment
   * @return The value size in this segment
   */
  int getValueSize() {
    return valueSize;
  }

  /**
   * The time of last modification of this segment in ms
   * @return The time in ms of the last modification of this segment.
   */
  long getLastModifiedTimeMs() {
    return TimeUnit.SECONDS.toMillis(lastModifiedTimeSec.get());
  }

  /**
   * The time of last modification of this segment in secs
   * @return The time in secs of the last modification of this segment.
   */
  long getLastModifiedTimeSecs() {
    return lastModifiedTimeSec.get();
  }

  /**
   * Sets the last modified time (secs) of this segment.
   * @param lastModifiedTimeSec the value to set to (secs).
   */
  void setLastModifiedTimeSecs(long lastModifiedTimeSec) {
    this.lastModifiedTimeSec.set(lastModifiedTimeSec);
  }

  /**
   * The version of the {@link PersistentIndex} that this {@link IndexSegment} is based on
   * @return the version of the {@link PersistentIndex} that this {@link IndexSegment} is based on
   */
  short getVersion() {
    return version;
  }

  /**
   * The resetKey for the index segment.
   * @return the reset key for the index segment which is a {@link Pair} of StoreKey and
   * {@link PersistentIndex.IndexEntryType}
   */
  Pair<StoreKey, PersistentIndex.IndexEntryType> getResetKey() {
    return resetKey;
  }

  /**
   * Finds an entry given a key. It finds from the in memory map or
   * does a binary search on the mapped persistent segment
   * @param keyToFind The key to find
   * @return The blob index value that represents the key or null if not found
   * @throws StoreException
   */
  IndexValue find(StoreKey keyToFind) throws StoreException {
    IndexValue toReturn = null;
    try {
      rwLock.readLock().lock();
      if (!mapped.get()) {
        IndexValue value = index.get(keyToFind);
        if (value != null) {
          metrics.blobFoundInActiveSegmentCount.inc();
        }
        toReturn = value;
      } else {
        if (bloomFilter != null) {
          metrics.bloomAccessedCount.inc();
        }
        if (bloomFilter == null || bloomFilter.isPresent(ByteBuffer.wrap(keyToFind.toBytes()))) {
          if (bloomFilter == null) {
            logger.trace("IndexSegment {} bloom filter empty. Searching file with start offset {} and for key {}",
                indexFile.getAbsolutePath(), startOffset, keyToFind);
          } else {
            metrics.bloomPositiveCount.inc();
            logger.trace("IndexSegment {} found in bloom filter for index with start offset {} and for key {} ",
                indexFile.getAbsolutePath(), startOffset, keyToFind);
          }
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
              toReturn = new IndexValue(startOffset.getName(), ByteBuffer.wrap(buf), getVersion());
              break;
            } else if (result < 0) {
              low = mid + 1;
            } else {
              high = mid - 1;
            }
          }
          if (bloomFilter != null && toReturn == null) {
            metrics.bloomFalsePositiveCount.inc();
          }
        }
      }
    } catch (IOException e) {
      throw new StoreException("IndexSegment : " + indexFile.getAbsolutePath() + " IO error while searching", e,
          StoreErrorCodes.IOError);
    } finally {
      rwLock.readLock().unlock();
    }
    return toReturn;
  }

  private int numberOfEntries(ByteBuffer mmap) {
    return (mmap.capacity() - indexSizeExcludingEntries) / persistedEntrySize;
  }

  private StoreKey getKeyAt(ByteBuffer mmap, int index) throws IOException {
    mmap.position(firstKeyRelativeOffset + index * persistedEntrySize);
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
  void addEntry(IndexEntry entry, Offset fileEndOffset) throws StoreException {
    try {
      rwLock.readLock().lock();
      if (mapped.get()) {
        throw new StoreException("IndexSegment : " + indexFile.getAbsolutePath() + " cannot add to a mapped index ",
            StoreErrorCodes.Illegal_Index_Operation);
      }
      logger.trace("IndexSegment {} inserting key - {} value - offset {} size {} ttl {} "
              + "originalMessageOffset {} fileEndOffset {}", indexFile.getAbsolutePath(), entry.getKey(),
          entry.getValue().getOffset(), entry.getValue().getSize(), entry.getValue().getExpiresAtMs(),
          entry.getValue().getOriginalMessageOffset(), fileEndOffset);
      if (index.put(entry.getKey(), entry.getValue()) == null) {
        numberOfItems.incrementAndGet();
        sizeWritten.addAndGet(entry.getKey().sizeInBytes() + entry.getValue().getBytes().capacity());
        bloomFilter.add(ByteBuffer.wrap(entry.getKey().toBytes()));
        if (resetKey == null) {
          resetKey = new Pair<>(entry.getKey(),
              entry.getValue().isFlagSet(IndexValue.Flags.Delete_Index) ? PersistentIndex.IndexEntryType.DELETE
                  : PersistentIndex.IndexEntryType.PUT);
        }
      }
      endOffset.set(fileEndOffset);
      long operationTimeInMs = entry.getValue().getOperationTimeInMs();
      if (operationTimeInMs == Utils.Infinite_Time) {
        lastModifiedTimeSec.set(time.seconds());
      } else if ((operationTimeInMs / Time.MsPerSec) > lastModifiedTimeSec.get()) {
        lastModifiedTimeSec.set(operationTimeInMs / Time.MsPerSec);
      }
      if (valueSize == VALUE_SIZE_INVALID_VALUE) {
        valueSize = entry.getValue().getBytes().capacity();
        logger.info("IndexSegment : {} setting value size to {} for index with start offset {}",
            indexFile.getAbsolutePath(), valueSize, startOffset);
      }
      if (persistedEntrySize == ENTRY_SIZE_INVALID_VALUE) {
        StoreKey key = entry.getKey();
        persistedEntrySize =
            getVersion() == PersistentIndex.VERSION_2 ? Math.max(config.storeIndexPersistedEntryMinBytes,
                key.sizeInBytes() + valueSize) : key.sizeInBytes() + valueSize;
        logger.info("IndexSegment : {} setting persisted entry size to {} of key {} for index with start offset {}",
            indexFile.getAbsolutePath(), persistedEntrySize, key.getLongForm(), startOffset);
      }
    } finally {
      rwLock.readLock().unlock();
    }
  }

  /**
   * The total size in bytes written to this segment so far
   * @return The total size in bytes written to this segment so far
   */
  long getSizeWritten() {
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
  int getNumberOfItems() {
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
   * Writes the index to a persistent file.
   *
   * Those that are written in version 2 have the following format:
   *
   *  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
   * | version | entrysize | valuesize | fileendpointer |  last modified time(in secs) | Reset key | Reset key type  ...
   * |(2 bytes)|(4 bytes)  | (4 bytes) |    (8 bytes)   |     (4 bytes)                | (m bytes) |   ( 2 bytes)    ...
   *  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
   *         - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
   *     ...   key 1    | value 1          | Padding    | ...  | key n     | value n           | Padding    | crc      |
   *     ...   (m bytes)| (valuesize bytes)| (var size) |      | (p bytes) | (valuesize bytes) | (var size) | (8 bytes)|
   *         - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
   *          |<- - - - - (entrysize bytes) - - - - - ->|      |<- - - - - - (entrysize bytes)- - - - - - ->|
   *
   *  version            - the index format version
   *  entrysize          - the total size of an entry (key + value + padding) in this index segment
   *  valuesize          - the size of the value in this index segment
   *  fileendpointer     - the log end pointer that pertains to the index being persisted
   *  last modified time - the last modified time of the index segment in secs
   *  reset key          - the reset key(StoreKey) of the index segment
   *  reset key type     - the reset key index entry type(PUT/DELETE)
   *  key n / value n    - the key and value entries contained in this index segment
   *  crc                - the crc of the index segment content
   *
   *
   * Those that were written in version 1 have the following format
   *  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
   * | version | keysize | valuesize | fileendpointer |  last modified time(in secs) | Reset key | Reset key type
   * |(2 bytes)|(4 bytes)| (4 bytes) |    (8 bytes)   |     (4 bytes)                | (m bytes) |   ( 2 bytes)
   *  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
   *                      - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
   *                  ...  key 1          | value 1          |  ...  |   key n         | value n           | crc       |
   *                  ...  (keysize bytes)| (valuesize bytes)|       | (keysize bytes) | (valuesize bytes) | (8 bytes) |
   *                      - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
   *  version            - the index format version
   *  keysize            - the size of the key in this index segment
   *  valuesize          - the size of the value in this index segment
   *  fileendpointer     - the log end pointer that pertains to the index being persisted
   *  last modified time - the last modified time of the index segment in secs
   *  reset key          - the reset key(StoreKey) of the index segment
   *  reset key type     - the reset key index entry type(PUT/DELETE)
   *  key n / value n    - the key and value entries contained in this index segment
   *  crc                - the crc of the index segment content
   *
   * Those that were written in version 0 have the following format
   *  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
   * | version | keysize | valuesize | fileendpointer |   key 1        | value 1          |  ...  |   key n          ...
   * |(2 bytes)|(4 bytes)| (4 bytes) |    (8 bytes)   | (keysize bytes)| (valuesize bytes)|       | (keysize bytes)  ...
   *  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
   *                                                          - - - - - - - - - - - - - - - -
   *                                                      ...  value n            | crc      |
   *                                                      ...  (valuesize  bytes) | (8 bytes)|
   *                                                          - - - - - - - - - - - - - - - -
   *
   *  version         - the index format version
   *  keysize         - the size of the key in this index segment
   *  valuesize       - the size of the value in this index segment
   *  fileendpointer  - the log end pointer that pertains to the index being persisted
   *  key n / value n - the key and value entries contained in this index segment
   *  crc             - the crc of the index segment content
   *
   * @param safeEndPoint the end point (that is relevant to this segment) until which the log has been flushed.
   * @throws IOException
   * @throws StoreException
   */
  void writeIndexSegmentToFile(Offset safeEndPoint) throws IOException, StoreException {
    if (safeEndPoint.compareTo(startOffset) <= 0) {
      return;
    }
    if (!safeEndPoint.equals(prevSafeEndPoint)) {
      if (safeEndPoint.compareTo(getEndOffset()) > 0) {
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

        writer.writeShort(getVersion());
        if (getVersion() == PersistentIndex.VERSION_2) {
          writer.writeInt(getPersistedEntrySize());
        } else {
          // write the key size
          writer.writeInt(getPersistedEntrySize() - getValueSize());
        }
        writer.writeInt(getValueSize());
        writer.writeLong(safeEndPoint.getOffset());
        if (getVersion() != PersistentIndex.VERSION_0) {
          // write last modified time and reset key in case of version != 0
          writer.writeLong(lastModifiedTimeSec.get());
          writer.write(resetKey.getFirst().toBytes());
          writer.writeShort(resetKey.getSecond().ordinal());
        }

        // NOTE: In the event of a crash, it is possible that there is a part of the log that is not covered by the
        // index. This happens due to the fact that a DELETE that occurs in the same segment as a PUT overwrites the
        // PUT entry. Consider the following case:-
        // (entries are of the form ID:TYPE:START_OFFSET-END_OFFSET)
        // This is the order of operations
        // A:PUT:0-100
        // B:PUT:101-200
        // A:DELETE:201-250
        // These are the entries in the index segment
        // A:DELETE:201-250
        // B:PUT:101-200
        // If safeEndPoint < 250, then B:PUT:101-200 will be written but not A:DELETE:201-250. If the process were to
        // crash at this point, the index end offset would be 200 and the span 0-100 would not be represented in the
        // index.
        // write the entries
        byte[] maxPaddingBytes = null;
        if (getVersion() == PersistentIndex.VERSION_2) {
          maxPaddingBytes = new byte[persistedEntrySize - valueSize];
        }
        for (Map.Entry<StoreKey, IndexValue> entry : index.entrySet()) {
          if (entry.getValue().getOffset().getOffset() + entry.getValue().getSize() <= safeEndPoint.getOffset()) {
            writer.write(entry.getKey().toBytes());
            writer.write(entry.getValue().getBytes().array());
            if (getVersion() == PersistentIndex.VERSION_2) {
              // Add padding if necessary
              writer.write(maxPaddingBytes, 0, persistedEntrySize - (entry.getKey().sizeInBytes() + valueSize));
            }
            logger.trace("IndexSegment : {} writing key - {} value - offset {} size {} fileEndOffset {}",
                getFile().getAbsolutePath(), entry.getKey(), entry.getValue().getOffset(), entry.getValue().getSize(),
                safeEndPoint);
          }
        }
        prevSafeEndPoint = safeEndPoint;
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
  void map(boolean persistBloom) throws IOException, StoreException {
    RandomAccessFile raf = new RandomAccessFile(indexFile, "r");
    rwLock.writeLock().lock();
    try {
      mmap = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, indexFile.length());
      mmap.position(0);
      version = mmap.getShort();
      StoreKey storeKey;
      int keySize;
      short resetKeyType;
      switch (version) {
        case PersistentIndex.VERSION_0:
          indexSizeExcludingEntries = VERSION_FIELD_LENGTH + KEY_OR_ENTRY_SIZE_FIELD_LENGTH + VALUE_SIZE_FIELD_LENGTH
              + LOG_END_OFFSET_FIELD_LENGTH + CRC_FIELD_LENGTH;
          keySize = mmap.getInt();
          valueSize = mmap.getInt();
          persistedEntrySize = keySize + valueSize;
          endOffset.set(new Offset(startOffset.getName(), mmap.getLong()));
          lastModifiedTimeSec.set(indexFile.lastModified() / 1000);
          firstKeyRelativeOffset = indexSizeExcludingEntries - CRC_FIELD_LENGTH;
          break;
        case PersistentIndex.VERSION_1:
          keySize = mmap.getInt();
          valueSize = mmap.getInt();
          persistedEntrySize = keySize + valueSize;
          endOffset.set(new Offset(startOffset.getName(), mmap.getLong()));
          lastModifiedTimeSec.set(mmap.getLong());
          storeKey = factory.getStoreKey(new DataInputStream(new ByteBufferInputStream(mmap)));
          resetKeyType = mmap.getShort();
          resetKey = new Pair<>(storeKey, PersistentIndex.IndexEntryType.values()[resetKeyType]);
          indexSizeExcludingEntries = VERSION_FIELD_LENGTH + KEY_OR_ENTRY_SIZE_FIELD_LENGTH + VALUE_SIZE_FIELD_LENGTH
              + LOG_END_OFFSET_FIELD_LENGTH + CRC_FIELD_LENGTH + LAST_MODIFIED_TIME_FIELD_LENGTH + resetKey.getFirst()
              .sizeInBytes() + RESET_KEY_TYPE_FIELD_LENGTH;
          firstKeyRelativeOffset = indexSizeExcludingEntries - CRC_FIELD_LENGTH;
          break;
        case PersistentIndex.VERSION_2:
          persistedEntrySize = mmap.getInt();
          valueSize = mmap.getInt();
          endOffset.set(new Offset(startOffset.getName(), mmap.getLong()));
          lastModifiedTimeSec.set(mmap.getLong());
          storeKey = factory.getStoreKey(new DataInputStream(new ByteBufferInputStream(mmap)));
          resetKeyType = mmap.getShort();
          resetKey = new Pair<>(storeKey, PersistentIndex.IndexEntryType.values()[resetKeyType]);
          indexSizeExcludingEntries = VERSION_FIELD_LENGTH + KEY_OR_ENTRY_SIZE_FIELD_LENGTH + VALUE_SIZE_FIELD_LENGTH
              + LOG_END_OFFSET_FIELD_LENGTH + CRC_FIELD_LENGTH + LAST_MODIFIED_TIME_FIELD_LENGTH + resetKey.getFirst()
              .sizeInBytes() + RESET_KEY_TYPE_FIELD_LENGTH;
          firstKeyRelativeOffset = indexSizeExcludingEntries - CRC_FIELD_LENGTH;
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
      version = stream.readShort();
      switch (version) {
        case PersistentIndex.VERSION_0:
        case PersistentIndex.VERSION_1:
          int keySize = stream.readInt();
          valueSize = stream.readInt();
          persistedEntrySize = keySize + valueSize;
          indexSizeExcludingEntries = VERSION_FIELD_LENGTH + KEY_OR_ENTRY_SIZE_FIELD_LENGTH + VALUE_SIZE_FIELD_LENGTH
              + LOG_END_OFFSET_FIELD_LENGTH + CRC_FIELD_LENGTH;
          break;
        case PersistentIndex.VERSION_2:
          persistedEntrySize = stream.readInt();
          valueSize = stream.readInt();
          indexSizeExcludingEntries = VERSION_FIELD_LENGTH + KEY_OR_ENTRY_SIZE_FIELD_LENGTH + VALUE_SIZE_FIELD_LENGTH
              + LOG_END_OFFSET_FIELD_LENGTH + CRC_FIELD_LENGTH;
          break;
        default:
          throw new StoreException("IndexSegment : " + indexFile.getAbsolutePath() + " invalid version in index file ",
              StoreErrorCodes.Index_Version_Error);
      }
      long logEndOffset = stream.readLong();
      if (version == PersistentIndex.VERSION_0) {
        lastModifiedTimeSec.set(indexFile.lastModified() / 1000);
      } else {
        lastModifiedTimeSec.set(stream.readLong());
        StoreKey storeKey = factory.getStoreKey(stream);
        short resetKeyType = stream.readShort();
        resetKey = new Pair<>(storeKey, PersistentIndex.IndexEntryType.values()[resetKeyType]);
        indexSizeExcludingEntries +=
            LAST_MODIFIED_TIME_FIELD_LENGTH + resetKey.getFirst().sizeInBytes() + RESET_KEY_TYPE_FIELD_LENGTH;
      }
      firstKeyRelativeOffset = indexSizeExcludingEntries - CRC_FIELD_LENGTH;
      logger.trace("IndexSegment : {} reading log end offset {} from file", indexFile.getAbsolutePath(), logEndOffset);
      long maxEndOffset = Long.MIN_VALUE;
      byte[] padding = new byte[persistedEntrySize - valueSize];
      while (stream.available() > CRC_FIELD_LENGTH) {
        StoreKey key = factory.getStoreKey(stream);
        byte[] value = new byte[valueSize];
        stream.readFully(value);
        if (version == PersistentIndex.VERSION_2) {
          stream.readFully(padding, 0, persistedEntrySize - (key.sizeInBytes() + valueSize));
        }
        IndexValue blobValue = new IndexValue(startOffset.getName(), ByteBuffer.wrap(value), version);
        long offsetInLogSegment = blobValue.getOffset().getOffset();
        // ignore entries that have offsets outside the log end offset that this index represents
        if (offsetInLogSegment + blobValue.getSize() <= logEndOffset) {
          index.put(key, blobValue);
          logger.trace("IndexSegment : {} putting key {} in index offset {} size {}", indexFile.getAbsolutePath(), key,
              blobValue.getOffset(), blobValue.getSize());
          // regenerate the bloom filter for in memory indexes
          bloomFilter.add(ByteBuffer.wrap(key.toBytes()));
          // add to the journal
          if (blobValue.getOriginalMessageOffset() != IndexValue.UNKNOWN_ORIGINAL_MESSAGE_OFFSET
              && offsetInLogSegment != blobValue.getOriginalMessageOffset()
              && blobValue.getOriginalMessageOffset() >= startOffset.getOffset()) {
            // we add an entry for the original message offset if it is within the same index segment
            journal.addEntry(new Offset(startOffset.getName(), blobValue.getOriginalMessageOffset()), key);
          }
          journal.addEntry(blobValue.getOffset(), key);
          // sizeWritten is only used for in-memory segments, and for those the padding does not come into picture.
          sizeWritten.addAndGet(key.sizeInBytes() + valueSize);
          numberOfItems.incrementAndGet();
          if (offsetInLogSegment + blobValue.getSize() > maxEndOffset) {
            maxEndOffset = offsetInLogSegment + blobValue.getSize();
          }
        } else {
          logger.info(
              "IndexSegment : {} ignoring index entry outside the log end offset that was not synced logEndOffset "
                  + "{} key {} entryOffset {} entrySize {} entryDeleteState {}", indexFile.getAbsolutePath(),
              logEndOffset, key, blobValue.getOffset(), blobValue.getSize(),
              blobValue.isFlagSet(IndexValue.Flags.Delete_Index));
        }
      }
      endOffset.set(new Offset(startOffset.getName(), maxEndOffset));
      logger.trace("IndexSegment : {} setting end offset for index {}", indexFile.getAbsolutePath(), maxEndOffset);
      long crc = crcStream.getValue();
      if (crc != stream.readLong()) {
        // reset structures
        persistedEntrySize = ENTRY_SIZE_INVALID_VALUE;
        valueSize = VALUE_SIZE_INVALID_VALUE;
        endOffset.set(startOffset);
        index.clear();
        bloomFilter.clear();
        throw new StoreException("IndexSegment : " + indexFile.getAbsolutePath() + " crc check does not match",
            StoreErrorCodes.Index_Creation_Failure);
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
  boolean getEntriesSince(StoreKey key, FindEntriesCondition findEntriesCondition, List<MessageInfo> entries,
      AtomicLong currentTotalSizeOfEntriesInBytes) throws IOException {
    List<IndexEntry> indexEntries = new ArrayList<>();
    boolean isNewEntriesAdded =
        getIndexEntriesSince(key, findEntriesCondition, indexEntries, currentTotalSizeOfEntriesInBytes);
    for (IndexEntry indexEntry : indexEntries) {
      IndexValue value = indexEntry.getValue();
      MessageInfo info =
          new MessageInfo(indexEntry.getKey(), value.getSize(), value.isFlagSet(IndexValue.Flags.Delete_Index),
              value.getExpiresAtMs(), value.getAccountId(), value.getContainerId(), value.getOperationTimeInMs());
      entries.add(info);
    }
    return isNewEntriesAdded;
  }

  /**
   * Gets all the index entries upto maxEntries from the start of a given key (exclusive) or all entries if key is null,
   * till maxTotalSizeOfEntriesInBytes
   * @param key The key from where to start retrieving entries.
   *            If the key is null, all entries are retrieved upto maxentries
   * @param findEntriesCondition The condition that determines when to stop fetching entries.
   * @param entries The input entries list that needs to be filled. The entries list can have existing entries
   * @param currentTotalSizeOfEntriesInBytes The current total size in bytes of the entries
   * @return true if any entries were added.
   * @throws IOException
   */
  boolean getIndexEntriesSince(StoreKey key, FindEntriesCondition findEntriesCondition, List<IndexEntry> entries,
      AtomicLong currentTotalSizeOfEntriesInBytes) throws IOException {
    if (!findEntriesCondition.proceed(currentTotalSizeOfEntriesInBytes.get(), getLastModifiedTimeSecs())) {
      return false;
    }
    int entriesSizeAtStart = entries.size();
    if (mapped.get()) {
      int index = 0;
      if (key != null) {
        index = findIndex(key, mmap.duplicate());
      }
      if (index != -1) {
        ByteBuffer readBuf = mmap.duplicate();
        int totalEntries = numberOfEntries(readBuf);
        while (findEntriesCondition.proceed(currentTotalSizeOfEntriesInBytes.get(), getLastModifiedTimeSecs())
            && index < totalEntries) {
          StoreKey newKey = getKeyAt(readBuf, index);
          byte[] buf = new byte[valueSize];
          readBuf.get(buf);
          // we include the key in the final list if it is not the initial key or if the initial key was null
          if (key == null || newKey.compareTo(key) != 0) {
            IndexValue newValue = new IndexValue(startOffset.getName(), ByteBuffer.wrap(buf), getVersion());
            entries.add(new IndexEntry(newKey, newValue));
            currentTotalSizeOfEntriesInBytes.addAndGet(newValue.getSize());
          }
          index++;
        }
      } else {
        logger.error("IndexSegment : " + indexFile.getAbsolutePath() + " index not found for key " + key);
      }
    } else if (key == null || index.containsKey(key)) {
      ConcurrentNavigableMap<StoreKey, IndexValue> tempMap = index;
      if (key != null) {
        tempMap = index.tailMap(key, true);
      }
      for (Map.Entry<StoreKey, IndexValue> entry : tempMap.entrySet()) {
        if (key == null || entry.getKey().compareTo(key) != 0) {
          IndexValue newValue = new IndexValue(startOffset.getName(), entry.getValue().getBytes(), getVersion());
          entries.add(new IndexEntry(entry.getKey(), newValue));
          currentTotalSizeOfEntriesInBytes.addAndGet(entry.getValue().getSize());
          if (!findEntriesCondition.proceed(currentTotalSizeOfEntriesInBytes.get(), getLastModifiedTimeSecs())) {
            break;
          }
        }
      }
    } else {
      logger.error("IndexSegment : " + indexFile.getAbsolutePath() + " key not found: " + key);
    }
    return entries.size() > entriesSizeAtStart;
  }

  /**
   * @return the prefix for the index segment file name (also used for bloom filter file name).
   */
  private String generateIndexSegmentFilenamePrefix() {
    String logSegmentName = startOffset.getName();
    StringBuilder filenamePrefix = new StringBuilder(logSegmentName);
    if (!logSegmentName.isEmpty()) {
      filenamePrefix.append(BlobStore.SEPARATOR);
    }
    return filenamePrefix.append(startOffset.getOffset()).append(BlobStore.SEPARATOR).toString();
  }

  /**
   * Gets the start {@link Offset} in the {@link Log} that the index with file name {@code filename} represents.
   * @param filename the name of the index file.
   * @return the start {@link Offset} in the {@link Log} that the index with file name {@code filename} represents.
   */
  static Offset getIndexSegmentStartOffset(String filename) {
    // file name pattern for index is {logSegmentName}_{offset}_index.
    // If the logSegment name is empty, then the file name pattern is {offset}_index.
    String logSegmentName;
    String startOffsetValue;
    int firstSepIdx = filename.indexOf(BlobStore.SEPARATOR);
    int lastSepIdx = filename.lastIndexOf(BlobStore.SEPARATOR);
    if (firstSepIdx == lastSepIdx) {
      // pattern is offset_index.
      logSegmentName = "";
      startOffsetValue = filename.substring(0, firstSepIdx);
    } else {
      // pattern is logSegmentName_offset_index.
      int lastButOneSepIdx = filename.substring(0, lastSepIdx).lastIndexOf(BlobStore.SEPARATOR);
      logSegmentName = filename.substring(0, lastButOneSepIdx);
      startOffsetValue = filename.substring(lastButOneSepIdx + 1, lastSepIdx);
    }
    return new Offset(logSegmentName, Long.parseLong(startOffsetValue));
  }
}

