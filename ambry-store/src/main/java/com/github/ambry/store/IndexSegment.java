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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
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
  private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
  private final AtomicBoolean sealed = new AtomicBoolean(false);
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final AtomicLong sizeWritten = new AtomicLong(0);
  private final StoreKeyFactory factory;
  private final File bloomFile;
  private final StoreMetrics metrics;
  private final AtomicInteger numberOfItems = new AtomicInteger(0);
  private final Time time;

  // an approximation of the last modified time.
  private final AtomicLong lastModifiedTimeSec = new AtomicLong(0);
  private ByteBuffer serEntries = null;
  private IFilter bloomFilter = null;
  private int valueSize;
  private int persistedEntrySize;
  private short version;
  private Offset prevSafeEndPoint = null;
  // reset key refers to the first StoreKey that is added to the index segment
  private Pair<StoreKey, PersistentIndex.IndexEntryType> resetKey = null;
  private NavigableMap<StoreKey, ConcurrentSkipListSet<IndexValue>> index = null;

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
    this.config = config;
    this.startOffset = startOffset;
    this.factory = factory;
    this.metrics = metrics;
    this.time = time;
    this.valueSize = valueSize;
    endOffset = new AtomicReference<>(startOffset);
    index = new ConcurrentSkipListMap<>();
    version = PersistentIndex.CURRENT_VERSION;
    persistedEntrySize = Math.max(config.storeIndexPersistedEntryMinBytes, entrySize);
    bloomFilter = FilterFactory.getFilter(config.storeIndexMaxNumberOfInmemElements,
        config.storeIndexBloomMaxFalsePositiveProbability);
    lastModifiedTimeSec.set(time.seconds());
    indexSegmentFilenamePrefix = generateIndexSegmentFilenamePrefix(startOffset);
    indexFile = new File(dataDir, indexSegmentFilenamePrefix + INDEX_SEGMENT_FILE_NAME_SUFFIX);
    bloomFile = new File(dataDir, indexSegmentFilenamePrefix + BLOOM_FILE_NAME_SUFFIX);
  }

  /**
   * Initializes an existing segment. Memory maps the segment or reads the segment into memory. Also reads the
   * persisted bloom filter from disk.
   * @param indexFile The index file that the segment needs to be initialized from
   * @param sealed Indicates that the segment is sealed
   * @param factory The store key factory used to create new store keys
   * @param config The store config used to initialize the index segment
   * @param metrics The store metrics used to track metrics
   * @param journal The journal to use
   * @param time the {@link Time} instance to use
   * @throws StoreException
   */
  IndexSegment(File indexFile, boolean sealed, StoreKeyFactory factory, StoreConfig config, StoreMetrics metrics,
      Journal journal, Time time) throws StoreException {
    try {
      this.config = config;
      this.indexFile = indexFile;
      this.factory = factory;
      this.metrics = metrics;
      this.time = time;
      this.sealed.set(sealed);
      startOffset = getIndexSegmentStartOffset(indexFile.getName());
      endOffset = new AtomicReference<>(startOffset);
      indexSegmentFilenamePrefix = generateIndexSegmentFilenamePrefix(startOffset);
      bloomFile = new File(indexFile.getParent(), indexSegmentFilenamePrefix + BLOOM_FILE_NAME_SUFFIX);
      if (bloomFile.exists() && config.storeIndexRebuildBloomFilterEnabled) {
        if (!bloomFile.delete()) {
          throw new StoreException("Could not delete bloom file named " + bloomFile, StoreErrorCodes.Unknown_Error);
        }
        logger.info(bloomFile + " is successfully deleted and will be rebuilt based on index segment");
      }
      if (sealed) {
        map();
        if (!bloomFile.exists()) {
          generateBloomFilterAndPersist();
        } else {
          // Load the bloom filter for this index
          // We need to load the bloom filter only for mapped indexes
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
        }
        if (config.storeSetFilePermissionEnabled) {
          Utils.setFilesPermission(Arrays.asList(this.indexFile, bloomFile), config.storeDataFilePermission);
        }
      } else {
        index = new ConcurrentSkipListMap<>();
        bloomFilter = FilterFactory.getFilter(config.storeIndexMaxNumberOfInmemElements,
            config.storeIndexBloomMaxFalsePositiveProbability);
        try {
          readFromFile(indexFile, journal);
        } catch (StoreException e) {
          if ((e.getErrorCode() == StoreErrorCodes.Index_Creation_Failure
              || e.getErrorCode() == StoreErrorCodes.Index_Version_Error)) {
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
   * @return The end offset that this segment represents
   */
  Offset getEndOffset() {
    return endOffset.get();
  }

  /**
   * @return {@code true}, if the segment is sealed
   */
  boolean isSealed() {
    return sealed.get();
  }

  /**
   * @return The file that this segment represents
   */
  File getFile() {
    return indexFile;
  }

  /**
   * @return The persisted entry size of this segment
   */
  int getPersistedEntrySize() {
    return persistedEntrySize;
  }

  /**
   * @return The value size in this segment
   */
  int getValueSize() {
    return valueSize;
  }

  /**
   * @return The time in ms of the last modification of this segment.
   */
  long getLastModifiedTimeMs() {
    return TimeUnit.SECONDS.toMillis(lastModifiedTimeSec.get());
  }

  /**
   * @return The time in secs of the last modification of this segment.
   */
  long getLastModifiedTimeSecs() {
    return lastModifiedTimeSec.get();
  }

  /**
   * @param lastModifiedTimeSec the value to set last modified time to (secs).
   */
  void setLastModifiedTimeSecs(long lastModifiedTimeSec) {
    this.lastModifiedTimeSec.set(lastModifiedTimeSec);
  }

  /**
   * @return the format version of the {@link PersistentIndex} that this {@link IndexSegment} is based on
   */
  short getVersion() {
    return version;
  }

  /**
   * set the format version of the {@link PersistentIndex} that this {@link IndexSegment} is based on
   */
  void setVersion(short version) {
    this.version = version;
  }

  /**
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
  NavigableSet<IndexValue> find(StoreKey keyToFind) throws StoreException {
    NavigableSet<IndexValue> toReturn = null;
    rwLock.readLock().lock();
    try {
      if (!sealed.get()) {
        ConcurrentSkipListSet<IndexValue> values = index.get(keyToFind);
        if (values != null) {
          metrics.blobFoundInMemSegmentCount.inc();
          toReturn = values.clone();
        }
      } else {
        if (bloomFilter != null) {
          metrics.bloomAccessedCount.inc();
        }
        if (bloomFilter == null || bloomFilter.isPresent(getStoreKeyBytes(keyToFind))) {
          if (bloomFilter == null) {
            logger.trace("IndexSegment {} bloom filter empty. Searching file with start offset {} and for key {}",
                indexFile.getAbsolutePath(), startOffset, keyToFind);
          } else {
            metrics.bloomPositiveCount.inc();
            logger.trace("IndexSegment {} found in bloom filter for index with start offset {} and for key {} ",
                indexFile.getAbsolutePath(), startOffset, keyToFind);
          }
          if (config.storeIndexMemState == IndexMemState.MMAP_WITH_FORCE_LOAD
              || config.storeIndexMemState == IndexMemState.MMAP_WITHOUT_FORCE_LOAD) {
            // isLoaded() will be true only if the entire buffer is in memory - so it being false does not necessarily
            // mean that the pages in the scope of the search need to be loaded from disk.
            // Secondly, even if it returned true (or false), by the time the actual lookup is done,
            // the situation may be different.
            if (((MappedByteBuffer) serEntries).isLoaded()) {
              metrics.mappedSegmentIsLoadedDuringFindCount.inc();
            } else {
              metrics.mappedSegmentIsNotLoadedDuringFindCount.inc();
            }
          }
          // binary search on the mapped file
          ByteBuffer duplicate = serEntries.duplicate();
          int low = 0;
          int totalEntries = numberOfEntries(duplicate);
          int high = totalEntries - 1;
          logger.trace("binary search low : {} high : {}", low, high);
          while (low <= high) {
            int mid = (int) (Math.ceil(high / 2.0 + low / 2.0));
            StoreKey found = getKeyAt(duplicate, mid);
            logger.trace("Index Segment {} binary search - key found on iteration {}", indexFile.getAbsolutePath(),
                found);
            int result = found.compareTo(keyToFind);
            if (result == 0) {
              toReturn = new TreeSet<>();
              getAllValuesFromMmap(duplicate, keyToFind, mid, totalEntries, toReturn);
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
    } catch (StoreException e) {
      throw new StoreException(String.format("IndexSegment %s : %s", indexFile.getAbsolutePath(), e.getMessage()), e,
          e.getErrorCode());
    } finally {
      rwLock.readLock().unlock();
    }
    return toReturn != null ? Collections.unmodifiableNavigableSet(toReturn) : null;
  }

  /**
   * According to config, get the {@link ByteBuffer} of {@link StoreKey} for bloom filter. The store config specifies
   * whether to populate bloom filter with key's UUID only.
   * @param key the store key to use in bloom filter.
   * @return required {@link ByteBuffer} associated with the key.
   */
  private ByteBuffer getStoreKeyBytes(StoreKey key) {
    return config.storeUuidBasedBloomFilterEnabled ? ByteBuffer.wrap(key.getUuidBytesArray())
        : ByteBuffer.wrap(key.toBytes());
  }

  /**
   * Generate bloom filter by walking through all index entries in this segment and persist it.
   * @throws StoreException
   */
  private void generateBloomFilterAndPersist() throws StoreException {
    int numOfIndexEntries = numberOfEntries(serEntries);
    // This is a workaround since we found higher than intended false positive rates with small bloom filter sizes. Note
    // that the number of entries in each index segment varies (from hundreds to thousands), the workaround ensures bloom
    // filter uses at least storeIndexMaxNumberOfInmemElements for creation to achieve decent performance.
    bloomFilter = FilterFactory.getFilter(Math.max(numOfIndexEntries, config.storeIndexMaxNumberOfInmemElements),
        config.storeIndexBloomMaxFalsePositiveProbability);
    for (int i = 0; i < numOfIndexEntries; i++) {
      StoreKey key = getKeyAt(serEntries, i);
      bloomFilter.add(getStoreKeyBytes(key));
    }
    persistBloomFilter();
  }

  /**
   * Persist the bloom filter.
   * @throws StoreException
   */
  private void persistBloomFilter() throws StoreException {
    try {
      CrcOutputStream crcStream = new CrcOutputStream(new FileOutputStream(bloomFile));
      DataOutputStream stream = new DataOutputStream(crcStream);
      FilterFactory.serialize(bloomFilter, stream);
      long crcValue = crcStream.getValue();
      stream.writeLong(crcValue);
      stream.close();
      if (config.storeSetFilePermissionEnabled) {
        Files.setPosixFilePermissions(bloomFile.toPath(), config.storeDataFilePermission);
      }
    } catch (IOException e) {
      StoreErrorCodes errorCode = StoreException.resolveErrorCode(e);
      throw new StoreException(errorCode.toString() + " while trying to persist bloom filter", e, errorCode);
    }
  }

  /**
   * Gets values for all matches for {@code keyToFind} at and in the vicinity of {@code positiveMatchInd}.
   * @param mmap the serEntries to read values off of
   * @param keyToFind the needle {@link StoreKey}
   * @param positiveMatchInd the index of a confirmed positive match
   * @param totalEntries the total number of entries in the index
   * @param values the set to which the {@link IndexValue}s will be added
   * @return a pair consisting of the lowest index that matched the key and the highest
   * @throws StoreException if there are problems reading from the mmap
   */
  private Pair<Integer, Integer> getAllValuesFromMmap(ByteBuffer mmap, StoreKey keyToFind, int positiveMatchInd,
      int totalEntries, NavigableSet<IndexValue> values) throws StoreException {
    byte[] buf = new byte[valueSize];
    // add the value at the positive match and anything after that matches
    int end = positiveMatchInd;
    for (; end < totalEntries && getKeyAt(mmap, end).equals(keyToFind); end++) {
      logger.trace("Index Segment {}: found {} at {}", indexFile.getAbsolutePath(), keyToFind, end);
      mmap.get(buf);
      values.add(new IndexValue(startOffset.getName(), ByteBuffer.wrap(buf), getVersion()));
    }
    end--;

    // add any values before the match
    int start = positiveMatchInd - 1;
    for (; start >= 0 && getKeyAt(mmap, start).equals(keyToFind); start--) {
      logger.trace("Index Segment {}: found {} at {}", indexFile.getAbsolutePath(), keyToFind, start);
      mmap.get(buf);
      values.add(new IndexValue(startOffset.getName(), ByteBuffer.wrap(buf), getVersion()));
    }
    return new Pair<>(start, end);
  }

  private int numberOfEntries(ByteBuffer mmap) {
    return (mmap.capacity() - indexSizeExcludingEntries) / persistedEntrySize;
  }

  private StoreKey getKeyAt(ByteBuffer mmap, int index) throws StoreException {
    StoreKey storeKey = null;
    try {
      mmap.position(firstKeyRelativeOffset + index * persistedEntrySize);
      storeKey = factory.getStoreKey(new DataInputStream(new ByteBufferInputStream(mmap)));
    } catch (InternalError e) {
      StoreErrorCodes errorCode = StoreException.resolveErrorCode(e);
      throw new StoreException("Internal " + errorCode.toString() + " while trying to get store key", e, errorCode);
    } catch (IOException e) {
      StoreErrorCodes errorCode = StoreException.resolveErrorCode(e);
      throw new StoreException(errorCode.toString() + " while trying to get store key", e, errorCode);
    } catch (Throwable t) {
      throw new StoreException("Unknown error while trying to get store key ", t, StoreErrorCodes.Unknown_Error);
    }
    return storeKey;
  }

  private int findIndex(StoreKey keyToFind, ByteBuffer mmap) throws StoreException {
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
    rwLock.writeLock().lock();
    try {
      if (sealed.get()) {
        throw new StoreException("IndexSegment : " + indexFile.getAbsolutePath() + " cannot add to a sealed index ",
            StoreErrorCodes.Illegal_Index_Operation);
      }
      logger.trace("IndexSegment {} inserting key - {} value - offset {} size {} ttl {} "
              + "originalMessageOffset {} fileEndOffset {}", indexFile.getAbsolutePath(), entry.getKey(),
          entry.getValue().getOffset(), entry.getValue().getSize(), entry.getValue().getExpiresAtMs(),
          entry.getValue().getOriginalMessageOffset(), fileEndOffset);
      boolean isPresent = index.containsKey(entry.getKey());
      index.computeIfAbsent(entry.getKey(), key -> new ConcurrentSkipListSet<>()).add(entry.getValue());
      if (!isPresent) {
        bloomFilter.add(getStoreKeyBytes(entry.getKey()));
      }
      if (resetKey == null) {
        PersistentIndex.IndexEntryType type = PersistentIndex.IndexEntryType.PUT;
        if (entry.getValue().isDelete()) {
          type = PersistentIndex.IndexEntryType.DELETE;
        } else if (entry.getValue().isUndelete()) {
          type = PersistentIndex.IndexEntryType.UNDELETE;
        } else if (entry.getValue().isTtlUpdate()) {
          type = PersistentIndex.IndexEntryType.TTL_UPDATE;
        }
        resetKey = new Pair<>(entry.getKey(), type);
      }
      numberOfItems.incrementAndGet();
      sizeWritten.addAndGet(entry.getKey().sizeInBytes() + entry.getValue().getBytes().capacity());
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
            getVersion() >= PersistentIndex.VERSION_2 ? Math.max(config.storeIndexPersistedEntryMinBytes,
                key.sizeInBytes() + valueSize) : key.sizeInBytes() + valueSize;
        logger.info("IndexSegment : {} setting persisted entry size to {} of key {} for index with start offset {}",
            indexFile.getAbsolutePath(), persistedEntrySize, key.getLongForm(), startOffset);
      }
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  /**
   * The total size in bytes written to this segment so far
   * @return The total size in bytes written to this segment so far
   */
  long getSizeWritten() {
    rwLock.readLock().lock();
    try {
      if (sealed.get()) {
        throw new UnsupportedOperationException("Operation supported only on index segments that are not sealed");
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
    rwLock.readLock().lock();
    try {
      if (sealed.get()) {
        throw new UnsupportedOperationException(
            "Operation supported only on index segments that are not sealed: " + getStartOffset());
      }
      return numberOfItems.get();
    } finally {
      rwLock.readLock().unlock();
    }
  }

  /**
   * Writes the index to a persistent file.
   *
   * Those that are written in version 2 and 3 have the following format:
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
   * @throws FileNotFoundException
   * @throws StoreException
   */
  void writeIndexSegmentToFile(Offset safeEndPoint) throws FileNotFoundException, StoreException {
    if (sealed.get()) {
      throw new StoreException("Cannot persist sealed index segment", StoreErrorCodes.Illegal_Index_Operation);
    }
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
      rwLock.readLock().lock();
      try (DataOutputStream writer = new DataOutputStream(crc)) {
        writer.writeShort(getVersion());
        if (getVersion() >= PersistentIndex.VERSION_2) {
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

        byte[] maxPaddingBytes = null;
        if (getVersion() >= PersistentIndex.VERSION_2) {
          maxPaddingBytes = new byte[persistedEntrySize - valueSize];
        }
        for (Map.Entry<StoreKey, ConcurrentSkipListSet<IndexValue>> entry : index.entrySet()) {
          for (IndexValue value : entry.getValue()) {
            if (value.getOffset().getOffset() + value.getSize() <= safeEndPoint.getOffset()) {
              writer.write(entry.getKey().toBytes());
              writer.write(value.getBytes().array());
              if (getVersion() >= PersistentIndex.VERSION_2) {
                // Add padding if necessary
                writer.write(maxPaddingBytes, 0, persistedEntrySize - (entry.getKey().sizeInBytes() + valueSize));
              }
              logger.trace("IndexSegment : {} writing key - {} value - offset {} size {} fileEndOffset {}",
                  getFile().getAbsolutePath(), entry.getKey(), value.getOffset(), value.getSize(), safeEndPoint);
            }
          }
        }
        prevSafeEndPoint = safeEndPoint;
        long crcValue = crc.getValue();
        writer.writeLong(crcValue);

        // flush and overwrite old file
        fileStream.getChannel().force(true);
        // swap temp file with the original file
        temp.renameTo(getFile());
        if (config.storeSetFilePermissionEnabled) {
          Files.setPosixFilePermissions(getFile().toPath(), config.storeDataFilePermission);
        }
      } catch (IOException e) {
        StoreErrorCodes errorCode = StoreException.resolveErrorCode(e);
        throw new StoreException(
            "IndexSegment : " + indexFile.getAbsolutePath() + " encountered " + errorCode.toString()
                + " while persisting index to disk", e, errorCode);
      } finally {
        rwLock.readLock().unlock();
      }
      logger.trace("IndexSegment : {} completed writing index to file", indexFile.getAbsolutePath());
    }
  }

  /**
   * Marks the segment as sealed. Also persists the bloom filter to disk and conditionally mmaps the index segment.
   * @throws StoreException if there are problems with the index
   */
  void seal() throws StoreException {
    sealed.set(true);
    map();
    // we should be fine reading bloom filter here without synchronization as the index is read only
    persistBloomFilter();
  }

  /**
   * @return index value of last PUT record in this index segment. Return {@code null} if no PUT is found
   */
  IndexValue getIndexValueOfLastPut() throws StoreException {
    IndexValue indexValueOfLastPut = null;
    if (sealed.get()) {
      ByteBuffer readBuf = serEntries.duplicate();
      int numOfIndexEntries = numberOfEntries(readBuf);
      NavigableSet<IndexValue> values = new TreeSet<>();
      for (int i = 0; i < numOfIndexEntries; i++) {
        StoreKey key = getKeyAt(readBuf, i);
        values.clear();
        getAllValuesFromMmap(readBuf, key, i, numOfIndexEntries, values);
        for (IndexValue indexValue : values) {
          // FLAGS_DEFAULT_VALUE means PUT record
          if (indexValue.getFlags() == IndexValue.FLAGS_DEFAULT_VALUE && (indexValueOfLastPut == null
              || indexValue.compareTo(indexValueOfLastPut) > 0)) {
            indexValueOfLastPut = indexValue;
            // note that values set contains all entries associated with specific key, so there are at most 3 entries in
            // this set (one PUT, one TTL Update and one DELETE). Due to nature of log, PUT always comes first. And if we
            // already find PUT, we can jump out of the inner loop.
            break;
          }
        }
      }
    } else {
      for (Map.Entry<StoreKey, ConcurrentSkipListSet<IndexValue>> entry : index.entrySet()) {
        for (IndexValue indexValue : entry.getValue()) {
          // FLAGS_DEFAULT_VALUE means PUT record
          if (indexValue.getFlags() == IndexValue.FLAGS_DEFAULT_VALUE && (indexValueOfLastPut == null
              || indexValue.compareTo(indexValueOfLastPut) > 0)) {
            indexValueOfLastPut = indexValue;
            break;
          }
        }
      }
    }
    return indexValueOfLastPut;
  }

  /**
   * Maps the segment of index either as a memory map or a in memory buffer depending on config.
   * @throws StoreException if there are problems with the index
   */
  private void map() throws StoreException {
    rwLock.writeLock().lock();
    try (RandomAccessFile raf = new RandomAccessFile(indexFile, "r")) {
      switch (config.storeIndexMemState) {
        case IN_DIRECT_MEM:
          serEntries = readFileIntoBuffer(raf, true);
          break;
        case IN_HEAP_MEM:
          serEntries = readFileIntoBuffer(raf, false);
          break;
        default:
          MappedByteBuffer buf = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, indexFile.length());
          if (config.storeIndexMemState == IndexMemState.MMAP_WITH_FORCE_LOAD) {
            buf.load();
          }
          serEntries = buf;
          break;
      }
      serEntries.position(0);
      setVersion(serEntries.getShort());
      StoreKey storeKey;
      int keySize;
      short resetKeyType;
      switch (getVersion()) {
        case PersistentIndex.VERSION_0:
          indexSizeExcludingEntries = VERSION_FIELD_LENGTH + KEY_OR_ENTRY_SIZE_FIELD_LENGTH + VALUE_SIZE_FIELD_LENGTH
              + LOG_END_OFFSET_FIELD_LENGTH + CRC_FIELD_LENGTH;
          keySize = serEntries.getInt();
          valueSize = serEntries.getInt();
          persistedEntrySize = keySize + valueSize;
          endOffset.set(new Offset(startOffset.getName(), serEntries.getLong()));
          lastModifiedTimeSec.set(indexFile.lastModified() / 1000);
          firstKeyRelativeOffset = indexSizeExcludingEntries - CRC_FIELD_LENGTH;
          break;
        case PersistentIndex.VERSION_1:
          keySize = serEntries.getInt();
          valueSize = serEntries.getInt();
          persistedEntrySize = keySize + valueSize;
          endOffset.set(new Offset(startOffset.getName(), serEntries.getLong()));
          lastModifiedTimeSec.set(serEntries.getLong());
          storeKey = factory.getStoreKey(new DataInputStream(new ByteBufferInputStream(serEntries)));
          resetKeyType = serEntries.getShort();
          resetKey = new Pair<>(storeKey, PersistentIndex.IndexEntryType.values()[resetKeyType]);
          indexSizeExcludingEntries = VERSION_FIELD_LENGTH + KEY_OR_ENTRY_SIZE_FIELD_LENGTH + VALUE_SIZE_FIELD_LENGTH
              + LOG_END_OFFSET_FIELD_LENGTH + CRC_FIELD_LENGTH + LAST_MODIFIED_TIME_FIELD_LENGTH + resetKey.getFirst()
              .sizeInBytes() + RESET_KEY_TYPE_FIELD_LENGTH;
          firstKeyRelativeOffset = indexSizeExcludingEntries - CRC_FIELD_LENGTH;
          break;
        case PersistentIndex.VERSION_2:
        case PersistentIndex.VERSION_3:
          persistedEntrySize = serEntries.getInt();
          valueSize = serEntries.getInt();
          endOffset.set(new Offset(startOffset.getName(), serEntries.getLong()));
          lastModifiedTimeSec.set(serEntries.getLong());
          storeKey = factory.getStoreKey(new DataInputStream(new ByteBufferInputStream(serEntries)));
          resetKeyType = serEntries.getShort();
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
      index = null;
    } catch (FileNotFoundException e) {
      throw new StoreException("File not found while mapping the segment of index", e, StoreErrorCodes.File_Not_Found);
    } catch (IOException e) {
      StoreErrorCodes errorCode = StoreException.resolveErrorCode(e);
      throw new StoreException(errorCode.toString() + " while mapping the segment of index", e, errorCode);
    } finally {
      rwLock.writeLock().unlock();
    }
  }

  /**
   * Reads the index segment from file into an in memory representation
   * @param fileToRead The file to read the index segment from
   * @param journal The journal to use.
   * @throws StoreException
   * @throws FileNotFoundException
   */
  private void readFromFile(File fileToRead, Journal journal) throws StoreException, FileNotFoundException {
    logger.info("IndexSegment : {} reading index from file", indexFile.getAbsolutePath());
    index.clear();
    CrcInputStream crcStream = new CrcInputStream(new FileInputStream(fileToRead));
    try (DataInputStream stream = new DataInputStream(crcStream)) {
      setVersion(stream.readShort());
      switch (getVersion()) {
        case PersistentIndex.VERSION_0:
        case PersistentIndex.VERSION_1:
          int keySize = stream.readInt();
          valueSize = stream.readInt();
          persistedEntrySize = keySize + valueSize;
          indexSizeExcludingEntries = VERSION_FIELD_LENGTH + KEY_OR_ENTRY_SIZE_FIELD_LENGTH + VALUE_SIZE_FIELD_LENGTH
              + LOG_END_OFFSET_FIELD_LENGTH + CRC_FIELD_LENGTH;
          break;
        case PersistentIndex.VERSION_2:
        case PersistentIndex.VERSION_3:
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
      if (getVersion() == PersistentIndex.VERSION_0) {
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
        if (getVersion() >= PersistentIndex.VERSION_2) {
          stream.readFully(padding, 0, persistedEntrySize - (key.sizeInBytes() + valueSize));
        }
        IndexValue blobValue = new IndexValue(startOffset.getName(), ByteBuffer.wrap(value), getVersion());
        long offsetInLogSegment = blobValue.getOffset().getOffset();
        // ignore entries that have offsets outside the log end offset that this index represents
        if (offsetInLogSegment + blobValue.getSize() <= logEndOffset) {
          boolean isPresent = index.containsKey(key);
          index.computeIfAbsent(key, k -> new ConcurrentSkipListSet<>()).add(blobValue);
          logger.trace("IndexSegment : {} putting key {} in index offset {} size {}", indexFile.getAbsolutePath(), key,
              blobValue.getOffset(), blobValue.getSize());
          // regenerate the bloom filter for index segments that are not sealed
          if (!isPresent) {
            bloomFilter.add(getStoreKeyBytes(key));
          }
          // add to the journal
          long oMsgOff = blobValue.getOriginalMessageOffset();
          if (oMsgOff != IndexValue.UNKNOWN_ORIGINAL_MESSAGE_OFFSET && offsetInLogSegment != oMsgOff
              && oMsgOff >= startOffset.getOffset()
              && journal.getKeyAtOffset(new Offset(startOffset.getName(), oMsgOff)) == null) {
            // we add an entry for the original message offset if it is within the same index segment and
            // an entry is not already in the journal
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
              logEndOffset, key, blobValue.getOffset(), blobValue.getSize(), blobValue.isDelete());
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
      StoreErrorCodes errorCode = StoreException.resolveErrorCode(e);
      throw new StoreException("IndexSegment : " + indexFile.getAbsolutePath() + " encountered " + errorCode.toString()
          + " while reading from file ", e, errorCode);
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
   * @throws StoreException
   */
  boolean getEntriesSince(StoreKey key, FindEntriesCondition findEntriesCondition, List<MessageInfo> entries,
      AtomicLong currentTotalSizeOfEntriesInBytes) throws StoreException {
    List<IndexEntry> indexEntries = new ArrayList<>();
    boolean areNewEntriesAdded =
        getIndexEntriesSince(key, findEntriesCondition, indexEntries, currentTotalSizeOfEntriesInBytes, true);
    for (IndexEntry indexEntry : indexEntries) {
      IndexValue value = indexEntry.getValue();
      MessageInfo info = new MessageInfo(indexEntry.getKey(), value.getSize(), value.isDelete(), value.isTtlUpdate(),
          value.isUndelete(), value.getExpiresAtMs(), null, value.getAccountId(), value.getContainerId(),
          value.getOperationTimeInMs(), value.getLifeVersion());
      entries.add(info);
    }
    return areNewEntriesAdded;
  }

  /**
   * Return an {@link Iterator<IndexEntry>} from a sealed IndexSegment.
   * @return an {@link Iterator<IndexEntry>}.
   */
  Iterator<IndexEntry> getIterator() {
    if (!sealed.get()) {
      throw new IllegalStateException(
          "IndexSegment at " + indexFile.getAbsolutePath() + " is not sealed to get iterator");
    }
    return new SealedIndexSegmentEntryIterator();
  }

  /**
   * Gets all the index entries upto maxEntries from the start of a given key (exclusive) or all entries if key is null,
   * till maxTotalSizeOfEntriesInBytes
   * @param key The key from where to start retrieving entries.
   *            If the key is null, all entries are retrieved upto maxentries
   * @param findEntriesCondition The condition that determines when to stop fetching entries.
   * @param entries The input entries list that needs to be filled. The entries list can have existing entries
   * @param currentTotalSizeOfEntriesInBytes The current total size in bytes of the entries
   * @param oneEntryPerKey returns only one index entry per key even if the segment has multiple values for the key.
   *                       Favors DELETE records over all other records. Favors PUT over a TTL update record.
   * @return true if any entries were added.
   * @throws StoreException
   */
  boolean getIndexEntriesSince(StoreKey key, FindEntriesCondition findEntriesCondition, List<IndexEntry> entries,
      AtomicLong currentTotalSizeOfEntriesInBytes, boolean oneEntryPerKey) throws StoreException {
    if (!findEntriesCondition.proceed(currentTotalSizeOfEntriesInBytes.get(), getLastModifiedTimeSecs())) {
      return false;
    }
    NavigableSet<IndexValue> values = new TreeSet<>();
    List<IndexEntry> entriesLocal = new ArrayList<>();
    if (sealed.get()) {
      int index = 0;
      if (key != null) {
        index = findIndex(key, serEntries.duplicate());
      }
      if (index != -1) {
        ByteBuffer readBuf = serEntries.duplicate();
        int totalEntries = numberOfEntries(readBuf);
        while (findEntriesCondition.proceed(currentTotalSizeOfEntriesInBytes.get(), getLastModifiedTimeSecs())
            && index < totalEntries) {
          StoreKey newKey = getKeyAt(readBuf, index);
          // we include the key in the final list if it is not the initial key or if the initial key was null
          if (key == null || newKey.compareTo(key) != 0) {
            values.clear();
            index = getAllValuesFromMmap(readBuf, newKey, index, totalEntries, values).getSecond();
            for (IndexValue value : values) {
              entriesLocal.add(new IndexEntry(newKey, value));
              currentTotalSizeOfEntriesInBytes.addAndGet(value.getSize());
            }
          }
          index++;
        }
      } else {
        logger.error("IndexSegment : " + indexFile.getAbsolutePath() + " index not found for key " + key);
        metrics.keyInFindEntriesAbsent.inc();
      }
    } else if (key == null || index.containsKey(key)) {
      NavigableMap<StoreKey, ConcurrentSkipListSet<IndexValue>> tempMap = index;
      if (key != null) {
        tempMap = index.tailMap(key, true);
      }
      for (Map.Entry<StoreKey, ConcurrentSkipListSet<IndexValue>> entry : tempMap.entrySet()) {
        if (key == null || entry.getKey().compareTo(key) != 0) {
          for (IndexValue value : entry.getValue()) {
            IndexValue newValue = new IndexValue(startOffset.getName(), value.getBytes(), getVersion());
            entriesLocal.add(new IndexEntry(entry.getKey(), newValue));
            currentTotalSizeOfEntriesInBytes.addAndGet(value.getSize());
          }
          // will break if size exceeded only after processing ALL entries for a key
          if (!findEntriesCondition.proceed(currentTotalSizeOfEntriesInBytes.get(), getLastModifiedTimeSecs())) {
            break;
          }
        }
      }
    } else {
      logger.error("IndexSegment : " + indexFile.getAbsolutePath() + " key not found: " + key);
      metrics.keyInFindEntriesAbsent.inc();
    }
    if (oneEntryPerKey) {
      eliminateDuplicates(entriesLocal);
    }
    entries.addAll(entriesLocal);
    return entriesLocal.size() > 0;
  }

  /**
   * Eliminates duplicates in {@code entries}
   * @param entries the entries to eliminate duplicates from.
   */
  private void eliminateDuplicates(List<IndexEntry> entries) {
    Set<StoreKey> setToFindDuplicate = new HashSet<>();
    // first choose PUTs over update entries (omitting DELETEs and UNDELETEs)
    entries.removeIf(entry -> !entry.getValue().isDelete() && !entry.getValue().isUndelete() && !setToFindDuplicate.add(
        entry.getKey()));
    // then choose DELETEs/UNDELETEs over all other entries
    setToFindDuplicate.clear();
    ListIterator<IndexEntry> iterator = entries.listIterator(entries.size());
    while (iterator.hasPrevious()) {
      IndexEntry entry = iterator.previous();
      if (!setToFindDuplicate.add(entry.getKey())) {
        iterator.remove();
      }
    }
  }

  /**
   * Read a full {@link RandomAccessFile} into a buffer.
   * @param file the file to read.
   * @param useDirect {@code true} to allocate a direct buffer instead of a heap buffer.
   * @return the buffer containing the contents of {@code file}
   * @throws IOException on read failure.
   */
  private static ByteBuffer readFileIntoBuffer(RandomAccessFile file, boolean useDirect) throws IOException {
    if (file.length() > Integer.MAX_VALUE) {
      throw new IllegalStateException("Configured to keep indexes in memory but index file length > IntegerMax");
    }
    ByteBuffer buf =
        useDirect ? ByteBuffer.allocateDirect((int) file.length()) : ByteBuffer.allocate((int) file.length());
    file.getChannel().read(buf);
    return buf;
  }

  /**
   * Creates the prefix for the index segment file name.
   * @param startOffset The start {@link Offset} in the {@link Log} that this segment represents.
   * @return the prefix for the index segment file name (also used for bloom filter file name).
   */
  static String generateIndexSegmentFilenamePrefix(Offset startOffset) {
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

  /**
   * An {@link IndexEntry} {@link Iterator} for a sealed {@link IndexSegment}. This {@link Iterator} should
   * only be used in the compaction so that the {@link IndexSegment} should be sealed already.
   */
  class SealedIndexSegmentEntryIterator implements Iterator<IndexEntry> {
    private int currentIdx = 0;
    private ByteBuffer mmap = serEntries.duplicate();
    private int numberOfEntries = numberOfEntries(mmap);
    private byte[] valueBuf = new byte[valueSize];

    @Override
    public boolean hasNext() {
      return currentIdx < numberOfEntries;
    }

    @Override
    public IndexEntry next() {
      try {
        StoreKey key = getKeyAt(mmap, currentIdx);
        mmap.get(valueBuf);
        return new IndexEntry(key, new IndexValue(startOffset.getName(), ByteBuffer.wrap(valueBuf), getVersion()));
      } catch (Exception e) {
        String message = "Failed to read index entry at " + currentIdx;
        logger.error(message, e);
        throw new IllegalStateException(message, e);
      } finally {
        currentIdx++;
      }
    }
  }
}

