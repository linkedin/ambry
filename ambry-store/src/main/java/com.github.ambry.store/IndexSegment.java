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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
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
  // an approximation of the last modified time.
  private final AtomicLong lastModifiedTimeSec;
  private final AtomicInteger numberOfItems;

  private MappedByteBuffer mmap = null;
  private IFilter bloomFilter;
  private int keySize;
  private int valueSize;
  private Offset prevSafeEndPoint = null;
  protected ConcurrentSkipListMap<StoreKey, IndexValue> index = null;

  /**
   * Creates a new segment
   * @param dataDir The data directory to use for this segment
   * @param startOffset The start {@link Offset} in the {@link Log} that this segment represents.
   * @param factory The store key factory used to create new store keys
   * @param keySize The key size that this segment supports
   * @param valueSize The value size that this segment supports
   * @param config The store config used to initialize the index segment
   */
  IndexSegment(String dataDir, Offset startOffset, StoreKeyFactory factory, int keySize, int valueSize,
      StoreConfig config, StoreMetrics metrics) {
    this.rwLock = new ReentrantReadWriteLock();
    this.startOffset = startOffset;
    this.endOffset = new AtomicReference<>(startOffset);
    index = new ConcurrentSkipListMap<>();
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
    indexSegmentFilenamePrefix = generateIndexSegmentFilenamePrefix();
    indexFile = new File(dataDir, indexSegmentFilenamePrefix + PersistentIndex.INDEX_SEGMENT_FILE_NAME_SUFFIX);
    bloomFile = new File(dataDir, indexSegmentFilenamePrefix + PersistentIndex.BLOOM_FILE_NAME_SUFFIX);
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
   * @throws StoreException
   */
  IndexSegment(File indexFile, boolean shouldMap, StoreKeyFactory factory, StoreConfig config, StoreMetrics metrics,
      Journal journal) throws StoreException {
    try {
      startOffset = getIndexSegmentStartOffset(indexFile.getName());
      endOffset = new AtomicReference<>(startOffset);
      indexSegmentFilenamePrefix = generateIndexSegmentFilenamePrefix();
      this.indexFile = indexFile;
      this.lastModifiedTimeSec = new AtomicLong(indexFile.lastModified() / 1000);
      this.rwLock = new ReentrantReadWriteLock();
      this.factory = factory;
      sizeWritten = new AtomicLong(0);
      numberOfItems = new AtomicInteger(0);
      mapped = new AtomicBoolean(false);
      if (shouldMap) {
        map(false);
        // Load the bloom filter for this index
        // We need to load the bloom filter only for mapped indexes
        bloomFile =
            new File(indexFile.getParent(), indexSegmentFilenamePrefix + PersistentIndex.BLOOM_FILE_NAME_SUFFIX);
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
        bloomFile =
            new File(indexFile.getParent(), indexSegmentFilenamePrefix + PersistentIndex.BLOOM_FILE_NAME_SUFFIX);
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
   * The key size in this segment
   * @return The key size in this segment
   */
  int getKeySize() {
    return keySize;
  }

  /**
   * The value size in this segment
   * @return The value size in this segment
   */
  int getValueSize() {
    return valueSize;
  }

  /**
   * The time of last modification of this segment
   * @return The time in seconds of the last modification of this segment.
   */
  long getLastModifiedTime() {
    return lastModifiedTimeSec.get();
  }

  /**
   * Finds an entry given a key. It finds from the in memory map or
   * does a binary search on the mapped persistent segment
   * @param keyToFind The key to find
   * @return The blob index value that represents the key or null if not found
   * @throws StoreException
   */
  IndexValue find(StoreKey keyToFind) throws StoreException {
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
              indexFile.getAbsolutePath(), startOffset, keyToFind);
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
              return new IndexValue(startOffset.getName(), ByteBuffer.wrap(buf));
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
   * @param safeEndPoint the end point (that is relevant to this segment) until which the log has been flushed.
   * @throws IOException
   * @throws StoreException
   */
  void writeIndexSegmentToFile(Offset safeEndPoint) throws IOException, StoreException {
    if (safeEndPoint.compareTo(startOffset) < 0) {
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

        // write the current version
        writer.writeShort(PersistentIndex.VERSION);
        // write key, value size and file end pointer for this index
        writer.writeInt(this.keySize);
        writer.writeInt(this.valueSize);
        writer.writeLong(safeEndPoint.getOffset());

        // write the entries
        for (Map.Entry<StoreKey, IndexValue> entry : index.entrySet()) {
          if (entry.getValue().getOffset().getOffset() + entry.getValue().getSize() <= safeEndPoint.getOffset()) {
            writer.write(entry.getKey().toBytes());
            writer.write(entry.getValue().getBytes().array());
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
      short version = mmap.getShort();
      switch (version) {
        case 0:
          this.keySize = mmap.getInt();
          this.valueSize = mmap.getInt();
          this.endOffset.set(new Offset(startOffset.getName(), mmap.getLong()));
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
          keySize = stream.readInt();
          valueSize = stream.readInt();
          long logEndOffset = stream.readLong();
          logger.trace("IndexSegment : {} reading log end offset {} from file", indexFile.getAbsolutePath(),
              logEndOffset);
          long maxEndOffset = Long.MIN_VALUE;
          while (stream.available() > Crc_Field_Length) {
            StoreKey key = factory.getStoreKey(stream);
            byte[] value = new byte[valueSize];
            stream.read(value);
            IndexValue blobValue = new IndexValue(startOffset.getName(), ByteBuffer.wrap(value));
            long offsetInLogSegment = blobValue.getOffset().getOffset();
            // ignore entries that have offsets outside the log end offset that this index represents
            if (offsetInLogSegment + blobValue.getSize() <= logEndOffset) {
              index.put(key, blobValue);
              logger.trace("IndexSegment : {} putting key {} in index offset {} size {}", indexFile.getAbsolutePath(),
                  key, blobValue.getOffset(), blobValue.getSize());
              // regenerate the bloom filter for in memory indexes
              bloomFilter.add(ByteBuffer.wrap(key.toBytes()));
              // add to the journal
              if (offsetInLogSegment != -1 && offsetInLogSegment != blobValue.getOriginalMessageOffset()
                  && blobValue.getOriginalMessageOffset() >= startOffset.getOffset()) {
                // we add an entry for the original message offset if it is within the same index segment
                journal.addEntry(new Offset(startOffset.getName(), blobValue.getOriginalMessageOffset()), key);
              }
              journal.addEntry(blobValue.getOffset(), key);
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
            this.keySize = Key_Size_Invalid_Value;
            this.valueSize = Value_Size_Invalid_Value;
            this.endOffset.set(startOffset);
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
  boolean getEntriesSince(StoreKey key, FindEntriesCondition findEntriesCondition, List<MessageInfo> entries,
      AtomicLong currentTotalSizeOfEntriesInBytes) throws IOException {
    if (!findEntriesCondition.proceed(currentTotalSizeOfEntriesInBytes.get(), this.getLastModifiedTime())) {
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
        while (findEntriesCondition.proceed(currentTotalSizeOfEntriesInBytes.get(), this.getLastModifiedTime())
            && index < totalEntries) {
          StoreKey newKey = getKeyAt(readBuf, index);
          byte[] buf = new byte[valueSize];
          readBuf.get(buf);
          // we include the key in the final list if it is not the initial key or if the initial key was null
          if (key == null || newKey.compareTo(key) != 0) {
            IndexValue newValue = new IndexValue(startOffset.getName(), ByteBuffer.wrap(buf));
            MessageInfo info =
                new MessageInfo(newKey, newValue.getSize(), newValue.isFlagSet(IndexValue.Flags.Delete_Index),
                    newValue.getExpiresAtMs());
            entries.add(info);
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
          MessageInfo info = new MessageInfo(entry.getKey(), entry.getValue().getSize(),
              entry.getValue().isFlagSet(IndexValue.Flags.Delete_Index), entry.getValue().getExpiresAtMs());
          entries.add(info);
          currentTotalSizeOfEntriesInBytes.addAndGet(entry.getValue().getSize());
          if (!findEntriesCondition.proceed(currentTotalSizeOfEntriesInBytes.get(), this.getLastModifiedTime())) {
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

