package com.github.ambry.store;

import com.github.ambry.config.StoreConfig;
import com.github.ambry.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
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
  private static int Version_Size = 2;
  private static int Key_Size = 4;
  private static int Value_Size = 4;
  private static int Crc_Size = 8;
  private static int Log_End_Offset_Size = 8;
  private static int Index_Size_Excluding_Entries =
          Version_Size + Key_Size + Value_Size + Log_End_Offset_Size + Crc_Size;
  private int keySize;
  private int valueSize;
  private File bloomFile;
  private int prevNumOfEntriesWritten = 0;
  private AtomicInteger numberOfItems;

  protected ConcurrentSkipListMap<StoreKey, BlobIndexValue> index = null;

  public IndexSegmentInfo(String dataDir,
                          long startOffset,
                          StoreKeyFactory factory,
                          int keySize,
                          int valueSize,
                          StoreConfig config) {
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
  }

  public IndexSegmentInfo(File indexFile, boolean isMapped, StoreKeyFactory factory, StoreConfig config)
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
        bloomFile = new File(indexFile.getParent(), startOffset + "_" + BlobPersistentIndex.Bloom_File_Name_Suffix);
        CrcInputStream crcBloom = new CrcInputStream(new FileInputStream(bloomFile));
        DataInputStream stream = new DataInputStream(crcBloom);
        bloomFilter = FilterFactory.deserialize(stream);
        long crcValue = crcBloom.getValue();
        if (crcValue != stream.readLong()) {
          // TODO metrics
          // TODO recreate the filter part of recovery
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
          readFromFile(indexFile);
        }
        catch (StoreException e) {
          if (e.getErrorCode() == StoreErrorCodes.Index_Creation_Failure) {
            logger.error("Error while reading from index {}", e);
            // do recovery
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
  }

  public long getStartOffset() {
    return startOffset.get();
  }

  public long getEndOffset() {
    return endOffset.get();
  }

  public boolean isMapped() {
    return mapped.get();
  }

  public File getFile() {
    return indexFile;
  }

  public int getKeySize() {
    return keySize;
  }

  public int getValueSize() {
    return valueSize;
  }

  public BlobIndexValue find(StoreKey keyToFind) throws StoreException {
    try {
      rwLock.readLock().lock();
      if (!(mapped.get())) {
        return index.get(keyToFind);
      }
      else {
        // check bloom filter first
        if (bloomFilter == null || bloomFilter.isPresent(ByteBuffer.wrap(keyToFind.toBytes()))) {
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
              byte[] buf = new byte[BlobIndexValue.Index_Value_Size_In_Bytes];
              duplicate.get(buf);
              return new BlobIndexValue(ByteBuffer.wrap(buf));
            }
            else if(result < 0)
              low = mid + 1;
            else
              high = mid - 1;
          }
        }
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
    mmap.position(Version_Size + Key_Size + Value_Size + (index * (keySize + valueSize)));
    return factory.getStoreKey(new DataInputStream(new ByteBufferInputStream(mmap)));
  }

  public void addEntry(BlobIndexEntry entry, long fileEndOffset) throws StoreException {
    try {
      rwLock.readLock().lock();
      if (mapped.get())
        throw new StoreException("Cannot add to a mapped index " +
                indexFile.getAbsolutePath(), StoreErrorCodes.Illegal_Index_Operation);
      logger.trace("Inserting key {} value offset {} size {} ttl {} fileEndOffset {}",
              entry.getKey(),
              entry.getValue().getOffset(),
              entry.getValue().getSize(),
              entry.getValue().getTimeToLiveInMs(),
              fileEndOffset);
      index.put(entry.getKey(), entry.getValue());
      sizeWritten.addAndGet(entry.getKey().sizeInBytes() + BlobIndexValue.Index_Value_Size_In_Bytes);
      numberOfItems.incrementAndGet();
      bloomFilter.add(ByteBuffer.wrap(entry.getKey().toBytes()));
      endOffset.set(fileEndOffset);
    }
    finally {
      rwLock.readLock().unlock();
    }
  }

  public void addEntries(ArrayList<BlobIndexEntry> entries, long fileEndOffset) throws StoreException {
    try {
      rwLock.readLock().lock();
      if (mapped.get())
        throw new StoreException("Cannot add to a mapped index" +
                indexFile.getAbsolutePath(), StoreErrorCodes.Illegal_Index_Operation);
      for (BlobIndexEntry entry : entries) {
        logger.trace("Inserting key {} value offset {} size {} ttl {} fileEndOffset {}",
                entry.getKey(),
                entry.getValue().getOffset(),
                entry.getValue().getSize(),
                entry.getValue().getTimeToLiveInMs(),
                fileEndOffset);
        index.put(entry.getKey(), entry.getValue());
        sizeWritten.addAndGet(entry.getKey().sizeInBytes() + BlobIndexValue.Index_Value_Size_In_Bytes);
        numberOfItems.incrementAndGet();
        bloomFilter.add(ByteBuffer.wrap(entry.getKey().toBytes()));
      }
      endOffset.set(fileEndOffset);
    }
    finally {
      rwLock.readLock().unlock();
    }
  }

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
        // write key and value size for this index
        writer.writeInt(this.keySize);
        writer.writeInt(this.valueSize);

        int numOfEntries = 0;
        // write the entries
        for (Map.Entry<StoreKey, BlobIndexValue> entry : index.entrySet()) {
          writer.write(entry.getKey().toBytes());
          writer.write(entry.getValue().getBytes().array());
          numOfEntries++;
        }
        prevNumOfEntriesWritten = numOfEntries;
        writer.writeLong(fileEndPointer);
        long crcValue = crc.getValue();
        writer.writeLong(crcValue);

        // flush and overwrite old file
        fileStream.getChannel().force(true);
        // swap temp file with the original file
        temp.renameTo(getFile());
      }
      catch (IOException e) {
        logger.error("IO error while persisting index to disk {}", indexFile.getAbsoluteFile());
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

  private void readFromFile(File fileToRead) throws StoreException, IOException {
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
          while (stream.available() > (Crc_Size + Log_End_Offset_Size)) {
            StoreKey key = factory.getStoreKey(stream);
            byte[] value = new byte[BlobIndexValue.Index_Value_Size_In_Bytes];
            stream.read(value);
            BlobIndexValue blobValue = new BlobIndexValue(ByteBuffer.wrap(value));
            index.put(key, blobValue);
            // regenerate the bloom filter for in memory indexes
            bloomFilter.add(ByteBuffer.wrap(key.toBytes()));
          }
          long endOffset = stream.readLong();
          this.endOffset.set(endOffset);
          long crc = crcStream.getValue();
          if (crc != stream.readLong())
            throw new StoreException("Crc check does not match", StoreErrorCodes.Index_Creation_Failure);
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

/**
 * A persistent index implementation that is responsible for adding and modifying index entries,
 * recovering an index from the log and commit and recover index to disk . This class
 * is not thread safe and expects the caller to do appropriate synchronization.
 **/
public class BlobPersistentIndex {

  private BlobJournal indexJournal;
  private long maxInMemoryIndexSizeInBytes;
  private int maxInMemoryNumElements;
  private Log log;
  private String dataDir;
  private Logger logger = LoggerFactory.getLogger(getClass());
  private IndexPersistor persistor;
  private StoreKeyFactory factory;
  private StoreConfig config;
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

  public BlobPersistentIndex(String datadir,
                             Scheduler scheduler,
                             Log log,
                             StoreConfig config,
                             StoreKeyFactory factory) throws StoreException {
    try {
      this.scheduler = scheduler;
      this.log = log;
      File indexDir = new File(datadir);
      File[] indexFiles = indexDir.listFiles(new IndexFilter());
      this.factory = factory;
      this.config = config;
      persistor = new IndexPersistor();
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
        IndexSegmentInfo info = new IndexSegmentInfo(indexFiles[i], map, factory, config);
        logger.info("Loaded index {}", indexFiles[i]);
        indexes.put(info.getStartOffset(), info);
      }
      this.dataDir = datadir;
      // get last index info and recover index by reading log here
      // start scheduler thread to persist index in the background
      this.scheduler = scheduler;
      this.scheduler.schedule("index persistor",
                              persistor,
                              config.storeDataFlushDelaySeconds + new Random().nextInt(SystemTime.SecsPerMin),
                              config.storeDataFlushIntervalSeconds,
                              TimeUnit.SECONDS);
      this.maxInMemoryIndexSizeInBytes = config.storeIndexMaxMemorySizeBytes;
      this.maxInMemoryNumElements = config.storeIndexMaxNumberOfInmemElements;
    }
    catch (Exception e) {
      logger.error("Error while creating index {}", e);
      throw new StoreException("Error while creating index " + e.getMessage(), StoreErrorCodes.Index_Creation_Failure);
    }
  }

  public void addToIndex(BlobIndexEntry entry, long fileEndOffset) throws StoreException {
    verifyFileEndOffset(fileEndOffset);
    if (needToRollOverIndex(entry)) {
      IndexSegmentInfo info = new IndexSegmentInfo(dataDir,
                                                   entry.getValue().getOffset(),
                                                   factory,
                                                   entry.getKey().sizeInBytes(),
                                                   BlobIndexValue.Index_Value_Size_In_Bytes,
                                                   config);
      info.addEntry(entry, fileEndOffset);
      indexes.put(info.getStartOffset(), info);
    }
    else {
      indexes.lastEntry().getValue().addEntry(entry, fileEndOffset);
    }
  }

  public void addToIndex(ArrayList<BlobIndexEntry> entries, long fileEndOffset) throws StoreException {
    verifyFileEndOffset(fileEndOffset);
    if (needToRollOverIndex(entries.get(0))) {
      IndexSegmentInfo info = new IndexSegmentInfo(dataDir,
                                                   entries.get(0).getValue().getOffset(),
                                                   factory,
                                                   entries.get(0).getKey().sizeInBytes(),
                                                   BlobIndexValue.Index_Value_Size_In_Bytes,
                                                   config);
      info.addEntries(entries, fileEndOffset);
      indexes.put(info.getStartOffset(), info);
    }
    else {
      indexes.lastEntry().getValue().addEntries(entries, fileEndOffset);
    }
  }

  private boolean needToRollOverIndex(BlobIndexEntry entry) {
    return  indexes.size() == 0 ||
            indexes.lastEntry().getValue().getSizeWritten() >= maxInMemoryIndexSizeInBytes ||
            indexes.lastEntry().getValue().getNumberOfItems() >= maxInMemoryNumElements ||
            indexes.lastEntry().getValue().getKeySize() != entry.getKey().sizeInBytes() ||
            indexes.lastEntry().getValue().getValueSize() != BlobIndexValue.Index_Value_Size_In_Bytes;
  }

  public boolean exists(StoreKey key) throws StoreException {
    return findKey(key) != null;
  }

  protected BlobIndexValue findKey(StoreKey key) throws StoreException {
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
    return null;
  }

  public void markAsDeleted(StoreKey id, long fileEndOffset) throws StoreException {
    verifyFileEndOffset(fileEndOffset);
    BlobIndexValue value = findKey(id);
    if (value == null) {
      logger.error("id {} not present in index. marking id as deleted failed", id);
      throw new StoreException("id not present in index : " + id, StoreErrorCodes.ID_Not_Found);
    }
    value.setFlag(BlobIndexValue.Flags.Delete_Index);
    indexes.lastEntry().getValue().addEntry(new BlobIndexEntry(id, value), fileEndOffset);
  }

  public void updateTTL(StoreKey id, long ttl, long fileEndOffset) throws StoreException {
    verifyFileEndOffset(fileEndOffset);
    BlobIndexValue value = findKey(id);
    if (value == null) {
      logger.error("id {} not present in index. updating ttl failed", id);
      throw new StoreException("id not present in index : " + id, StoreErrorCodes.ID_Not_Found);
    }
    value.setTimeToLive(ttl);
    indexes.lastEntry().getValue().addEntry(new BlobIndexEntry(id, value), fileEndOffset);
  }

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
    else if (value.getTimeToLiveInMs() != BlobIndexValue.TTL_Infinite &&
            SystemTime.getInstance().milliseconds() > value.getTimeToLiveInMs()) {
      logger.error("id {} has expired ttl {}", id, value.getTimeToLiveInMs());
      throw new StoreException("id not present in index " + id, StoreErrorCodes.TTL_Expired);
    }
    return new BlobReadOptions(value.getOffset(), value.getSize(), value.getTimeToLiveInMs());
  }

  public List<StoreKey> findMissingEntries(List<StoreKey> keys) throws StoreException {
    List<StoreKey> missingEntries = new ArrayList<StoreKey>();
    for (StoreKey key : keys) {
      if (!exists(key))
        missingEntries.add(key);
    }
    return missingEntries;
  }

  // TODO need to implement this
  public ArrayList<BlobIndexValue> getIndexEntrySince(long offset) {
    // check journal
    // return complete index
    return null;
  }

  public void close() throws StoreException {
    persistor.write();
  }

  public long getCurrentEndOffset() {
    return indexes.size() == 0 ? 0 : indexes.lastEntry().getValue().getEndOffset();
  }

  private void verifyFileEndOffset(long fileEndOffset) {
    if (getCurrentEndOffset() > fileEndOffset) {
      logger.error("File end offset provided to the index is less than the current end offset. " +
              "logEndOffset {} inputFileEndOffset {}", getCurrentEndOffset(), fileEndOffset);
      throw new IllegalArgumentException("File end offset provided to the index is less than the current end offset. " +
              "logEndOffset " + getCurrentEndOffset() + " inputFileEndOffset " + fileEndOffset);
    }
  }

  class IndexPersistor implements Runnable {

    public void write() throws StoreException {
      try {
        if (indexes.size() > 0) {
          // before iterating the map, get the current file end pointer
          IndexSegmentInfo currentInfo = indexes.lastEntry().getValue();
          long fileEndPointer = currentInfo.getEndOffset();

          // flush the log to ensure everything till the fileEndPointer is flushed
          log.flush();

          long lastOffset = indexes.lastEntry().getKey();
          IndexSegmentInfo prevInfo = indexes.size() > 1 ? indexes.lowerEntry(lastOffset).getValue() : null;
          while (prevInfo != null && !prevInfo.isMapped()) {
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
