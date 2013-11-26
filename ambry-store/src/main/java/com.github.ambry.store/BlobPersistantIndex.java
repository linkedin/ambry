package com.github.ambry.store;

import com.github.ambry.config.StoreConfig;
import com.github.ambry.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class IndexInfo {
  private AtomicLong startOffset;
  private AtomicLong endOffset;
  private File indexFile;
  private ReadWriteLock rwLock;
  private MappedByteBuffer mmap = null;
  private boolean mapped;
  private Logger logger = LoggerFactory.getLogger(getClass());
  private long sizeWritten;
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

  protected ConcurrentSkipListMap<StoreKey, BlobIndexValue> index = null;

  public IndexInfo(String dataDir, long startOffset, StoreKeyFactory factory, int keySize, int valueSize) {
    // create a new file with the start offset
    indexFile = new File(dataDir, startOffset + "_" + BlobPersistantIndex.Index_File_Name_Suffix);
    bloomFile = new File(dataDir, startOffset + "_" + BlobPersistantIndex.Bloom_File_Name_Suffix);
    this.rwLock = new ReentrantReadWriteLock();
    this.startOffset = new AtomicLong(startOffset);
    this.endOffset = new AtomicLong(-1);
    index = new ConcurrentSkipListMap<StoreKey, BlobIndexValue>();
    mapped = false;
    sizeWritten = 0;
    this.factory = factory;
    this.keySize = keySize;
    this.valueSize = valueSize;
    // TODO make it config
    bloomFilter = FilterFactory.getFilter(1000000, 0.1);
  }

  public IndexInfo(File indexFile, boolean map, StoreKeyFactory factory)
          throws StoreException {
    try {
      int startIndex = indexFile.getName().indexOf("_", 0);
      String startOffsetValue = indexFile.getName().substring(0, startIndex);
      startOffset = new AtomicLong(Long.parseLong(startOffsetValue));
      endOffset = new AtomicLong(-1);
      this.indexFile = indexFile;
      this.rwLock = new ReentrantReadWriteLock();
      this.factory = factory;
      sizeWritten = 0;
      if (map) {
        map(false);
        // Load the bloom filter for this index
        // We need to load the bloom filter only for mapped indexes
        bloomFile = new File(indexFile.getParent(), startIndex + "_" + BlobPersistantIndex.Bloom_File_Name_Suffix);
        CrcInputStream crcBloom = new CrcInputStream(new FileInputStream(bloomFile));
        DataInputStream stream = new DataInputStream(crcBloom);
        bloomFilter = FilterFactory.deserialize(stream);
        long crcValue = crcBloom.getValue();
        if (crcValue != stream.readLong()) {
          // TODO metrics
          // TODO recreate the filter
          bloomFilter = null;
          logger.error("Error validating crc for bloom filter for {}", bloomFile.getAbsolutePath());
        }
      }
      else {
        index = new ConcurrentSkipListMap<StoreKey, BlobIndexValue>();
        bloomFilter = FilterFactory.getFilter(1000000, 0.1);
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
      logger.error("Error while creating index {}", e);
      throw new StoreException("Error while creating index", e, StoreErrorCodes.Index_Creation_Failure);
    }
  }

  public long getStartOffset() {
    return startOffset.get();
  }

  public long getEndOffset() {
    return endOffset.get();
  }

  public boolean isMapped() {
    return mapped;
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
      if (!mapped) {
        return index.get(keyToFind);
      }
      else {
        // check bloom filter first
        if (bloomFilter == null || bloomFilter.isPresent(ByteBuffer.wrap(keyToFind.toBytes()))) {
          // binary search on the mapped file
          ByteBuffer duplicate = mmap.duplicate();
          int low = 0;
          int high  = numberOfEntries(duplicate) - 1;
          logger.trace("binary search low : {} high : {}", low, high);
          while(low <= high) {
            int mid = (int)(Math.ceil(high/2.0 + low/2.0));
            StoreKey found = getKeyAt(duplicate, mid);
            int result = found.compareTo(keyToFind);
            if(result == 0) {
              byte[] buf = new byte[BlobIndexValue.Index_Value_Size_In_Bytes];
              duplicate.get(buf);
              return new BlobIndexValue(ByteBuffer.wrap(buf));
            }
            else if(result < 0)
              low = mid;
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

  public void AddEntry(BlobIndexEntry entry, long fileEndOffset) throws StoreException {
    try {
      rwLock.readLock().lock();
      if (mapped)
        throw new StoreException("Cannot add to a mapped index " +
                indexFile.getAbsolutePath(), StoreErrorCodes.Illegal_Index_Operation);
      index.put(entry.getKey(), entry.getValue());
      sizeWritten += entry.getKey().sizeInBytes();
      sizeWritten += BlobIndexValue.Index_Value_Size_In_Bytes;
      bloomFilter.add(ByteBuffer.wrap(entry.getKey().toBytes()));
      endOffset.set(fileEndOffset);
    }
    finally {
      rwLock.readLock().unlock();
    }
  }

  public void AddEntries(ArrayList<BlobIndexEntry> entries, long fileEndOffset) throws StoreException {
    try {
      rwLock.readLock().lock();
      if (mapped)
        throw new StoreException("Cannot add to a mapped index" +
                indexFile.getAbsolutePath(), StoreErrorCodes.Illegal_Index_Operation);
      for (BlobIndexEntry entry : entries) {
        index.put(entry.getKey(), entry.getValue());
        sizeWritten += entry.getKey().sizeInBytes();
        sizeWritten += BlobIndexValue.Index_Value_Size_In_Bytes;
        bloomFilter.add(ByteBuffer.wrap(entry.getKey().toBytes()));
      }
      endOffset.set(fileEndOffset);
    }
    finally {
      rwLock.readLock().unlock();
    }
  }

  public long getSizeWritten() {
    return sizeWritten;
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
        writer.writeShort(BlobPersistantIndex.version);
        // write key and value size for this index
        writer.writeInt(this.keySize);
        writer.writeInt(this.valueSize);

        int numOfEntries = 0;
        // write the entries
        for (Map.Entry<StoreKey, BlobIndexValue> entry : index.entrySet()) {
          byte[] idBytes = entry.getKey().toBytes();
          writer.write(idBytes);
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
      mapped = true;
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
    boolean firstEntry = true;
    BlobIndexValue prevValue = null;
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
            if (firstEntry)
              if (blobValue.getOffset() != startOffset.get())
                throw new StoreException("First offset in index does not match the index name",
                        StoreErrorCodes.Index_Creation_Failure);
              else
                firstEntry = false;
            index.put(key, blobValue);
            // regenerate the bloom filter for in memory indexes
            bloomFilter.add(ByteBuffer.wrap(key.toBytes()));
            prevValue = blobValue;
          }
          long endOffset = stream.readLong();
          if (prevValue.getOffset() + prevValue.getSize() != endOffset)
            throw new StoreException("Last offset in index does not match the last entry",
                    StoreErrorCodes.Index_Creation_Failure);
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
 * A persistant version of the index
 */
public class BlobPersistantIndex {

  private BlobJournal indexJournal;
  private long maxInMemoryIndexSizeInBytes;
  private Log log;
  private String dataDir;
  private Logger logger = LoggerFactory.getLogger(getClass());
  private IndexPersistor persistor;
  private StoreKeyFactory factory;
  private static final int TTL_Infinite = -1;
  protected Scheduler scheduler;
  protected ConcurrentSkipListMap<Long, IndexInfo> indexes = new ConcurrentSkipListMap<Long, IndexInfo>();
  public static final String Index_File_Name_Suffix = "index";
  public static final String Bloom_File_Name_Suffix = "bloom";
  public static final Short version = 0;

  private class IndexFilter implements FilenameFilter {
    @Override
    public boolean accept(File dir, String name) {
      return name.endsWith(Index_File_Name_Suffix);
    }
  }

  public BlobPersistantIndex(String datadir, Scheduler scheduler, Log log, StoreConfig config) throws StoreException {
    try {
      this.scheduler = scheduler;
      this.log = log;
      File indexDir = new File(datadir);
      File[] indexFiles = indexDir.listFiles(new IndexFilter());
      this.factory = Utils.getObj(config.storeKeyFactory);
      persistor = new IndexPersistor();

      for (int i = 0; i < indexFiles.length; i++) {
        boolean map = false;
        if (i < indexFiles.length - 2)
          map = true;
        IndexInfo info = new IndexInfo(indexFiles[i], map, factory);
        indexes.put(info.getStartOffset(), info);
      }
      this.dataDir = datadir;
      // get last index info and recover index by reading log here
      // start scheduler thread to persist index in the background
      this.scheduler = scheduler;
      this.scheduler.schedule("index persistor", persistor, 5, config.storeDataFlushIntervalSeconds, TimeUnit.SECONDS);
      this.maxInMemoryIndexSizeInBytes = config.storeIndexMemorySizeMB * 1024 * 1024;
    }
    catch (Exception e) {
      logger.error("Error while creating index {}", e);
      throw new StoreException("Error while creating index " + e, StoreErrorCodes.Index_Creation_Failure);
    }
  }

  public void AddToIndex(BlobIndexEntry entry, long fileEndOffset) throws StoreException {
    verifyFileEndOffset(fileEndOffset);
    if (needToRollIndex(entry)) {
      IndexInfo info = new IndexInfo(dataDir, entry.getValue().getOffset(),
                                     factory, entry.getKey().sizeInBytes(),
                                     BlobIndexValue.Index_Value_Size_In_Bytes);
      info.AddEntry(entry, fileEndOffset);
      indexes.put(info.getStartOffset(), info);
    }
    else {
      indexes.lastEntry().getValue().AddEntry(entry, fileEndOffset);
    }
  }

  public void AddToIndex(ArrayList<BlobIndexEntry> entries, long fileEndOffset) throws StoreException {
    verifyFileEndOffset(fileEndOffset);
    if (needToRollIndex(entries.get(0))) {
      IndexInfo info = new IndexInfo(dataDir, entries.get(0).getValue().getOffset(),
                                     factory, entries.get(0).getKey().sizeInBytes(),
                                     BlobIndexValue.Index_Value_Size_In_Bytes);
      info.AddEntries(entries, fileEndOffset);
      indexes.put(info.getStartOffset(), info);
    }
    else {
      indexes.lastEntry().getValue().AddEntries(entries, fileEndOffset);
    }
  }

  private boolean needToRollIndex(BlobIndexEntry entry) {
    return  indexes.size() == 0 ||
            indexes.lastEntry().getValue().getSizeWritten() >= maxInMemoryIndexSizeInBytes ||
            indexes.lastEntry().getValue().getKeySize() != entry.getKey().sizeInBytes() ||
            indexes.lastEntry().getValue().getValueSize() != BlobIndexValue.Index_Value_Size_In_Bytes;
  }

  public boolean exist(StoreKey key) throws StoreException {
    return findKey(key) != null;
  }

  protected BlobIndexValue findKey(StoreKey key) throws StoreException {
    ConcurrentNavigableMap<Long, IndexInfo> descendMap = indexes.descendingMap();
    for (Map.Entry<Long, IndexInfo> entry : descendMap.entrySet()) {
      BlobIndexValue value = entry.getValue().find(key);
      if (value != null)
        return value;
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
    indexes.lastEntry().getValue().AddEntry(new BlobIndexEntry(id, value), fileEndOffset);
  }

  public void updateTTL(StoreKey id, long ttl, long fileEndOffset) throws StoreException {
    verifyFileEndOffset(fileEndOffset);
    BlobIndexValue value = findKey(id);
    if (value == null) {
      logger.error("id {} not present in index. updating ttl failed", id);
      throw new StoreException("id not present in index : " + id, StoreErrorCodes.ID_Not_Found);
    }
    value.setTimeToLive(ttl);
    indexes.lastEntry().getValue().AddEntry(new BlobIndexEntry(id, value), fileEndOffset);
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
    else if (value.getTimeToLiveInMs() != TTL_Infinite &&
            SystemTime.getInstance().milliseconds() > value.getTimeToLiveInMs()) {
      logger.error("id {} has expired ttl {}", id, value.getTimeToLiveInMs());
      throw new StoreException("id not present in index " + id, StoreErrorCodes.TTL_Expired);
    }
    return new BlobReadOptions(value.getOffset(), value.getSize(), value.getTimeToLiveInMs());
  }

  public List<StoreKey> findMissingEntries(List<StoreKey> keys) throws StoreException {
    List<StoreKey> missingEntries = new ArrayList<StoreKey>();
    for (StoreKey key : keys) {
      if (!exist(key))
        missingEntries.add(key);
    }
    return missingEntries;
  }

  // TODO need to fix this
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
          IndexInfo currentInfo = indexes.lastEntry().getValue();
          long fileEndPointer = currentInfo.getEndOffset();

          // flush the log to ensure everything till the fileEndPointer is flushed
          log.flush();

          // write to temp file and then swap with the existing file
          long lastOffset = indexes.lastEntry().getKey();
          IndexInfo prevInfo = indexes.size() > 1 ? indexes.higherEntry(lastOffset).getValue() : null;
          if (prevInfo != null && !prevInfo.isMapped()) {
            prevInfo.writeIndexToFile(prevInfo.getEndOffset());
            prevInfo.map(true);
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
