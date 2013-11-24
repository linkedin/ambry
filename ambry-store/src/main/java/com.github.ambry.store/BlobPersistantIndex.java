package com.github.ambry.store;

import com.github.ambry.utils.CrcInputStream;
import com.github.ambry.utils.CrcOutputStream;
import com.github.ambry.utils.Scheduler;
import com.github.ambry.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A persistant version of the index
 */
public class BlobPersistantIndex {

  protected Scheduler scheduler;
  private BlobJournal indexJournal;
  private static final String indexFileName = "index";
  private ConcurrentSkipListMap<Long, IndexInfo> indexes = new ConcurrentSkipListMap<Long, IndexInfo>();
  private Log log;
  private Logger logger = LoggerFactory.getLogger(getClass());
  public static final Short version = 0;

  private static class IndexInfo {
    private AtomicLong startOffset;
    private AtomicLong endOffset;
    private File indexFile;
    private ReadWriteLock rwLock;
    private MappedByteBuffer mmap = null;
    private boolean mapped;
    private Logger logger = LoggerFactory.getLogger(getClass());
    private int sizeWritten;
    private StoreKeyFactory factory;
    protected ConcurrentSkipListMap<StoreKey, ByteBuffer> index = null;
    public static final String Sorted_Index_Suffix = "sorted";
    private static int Blob_Size_In_Bytes = 8;
    private static int Offset_Size_In_Bytes = 8;
    private static int Flag_Size_In_Bytes = 1;
    private static int Time_To_Live_Size_In_Bytes = 8;
    public static int Index_Value_Size_In_Bytes = Blob_Size_In_Bytes + Offset_Size_In_Bytes +
                                                  Flag_Size_In_Bytes + Time_To_Live_Size_In_Bytes;

    public IndexInfo(String name, File indexFile)
            throws IOException, IllegalAccessException, ClassNotFoundException, InstantiationException {

      int startIndex = name.indexOf("_", 0);
      String startOffsetValue = name.substring(0, startIndex);
      int endIndex = name.indexOf("_", startIndex + 1);
      String endOffsetValue = name.substring(startIndex + 1, endIndex);
      startOffset = new AtomicLong(Long.parseLong(startOffsetValue));
      endOffset = new AtomicLong(Long.parseLong(endOffsetValue));
      this.indexFile = indexFile;
      this.rwLock = new ReentrantReadWriteLock();
      factory = Utils.getObj("com.github.ambry.shared.BlobIdFactory");
      sizeWritten = 0;
      if (name.substring(endIndex + 1, endIndex + 1 + Sorted_Index_Suffix.length()).equals(Sorted_Index_Suffix)) {
        map();
      }
      else {
        // we have a non sorted file
        index = new ConcurrentSkipListMap<StoreKey, ByteBuffer>();
      }
    }

    public IndexInfo(String name) {

    }

    public long getStartOffset() {
      return startOffset.get();
    }

    public long getEndOffset() {
      return endOffset.get();
    }

    public MappedByteBuffer getMappedIndex() {
      return mmap;
    }

    public boolean isMapped() {
      return mapped;
    }

    public File getFile() {
      return indexFile;
    }

    public void writeIndexToFile(File fileToWrite, long fileEndPointer) throws IOException {
      File temp = new File(fileToWrite.getAbsolutePath() + ".tmp");
      FileOutputStream fileStream = new FileOutputStream(temp);
      CrcOutputStream crc = new CrcOutputStream(fileStream);
      DataOutputStream writer = new DataOutputStream(crc);
      try {
        rwLock.readLock().lock();

        // write the current version
        writer.writeShort(BlobPersistantIndex.version);

        // write the entries
        for (Map.Entry<StoreKey, ByteBuffer> entry : index.entrySet()) {
          writer.write(entry.getKey().toBytes().array());
          entry.getValue().position(0);
          writer.writeInt(IndexInfo.Index_Value_Size_In_Bytes);
          writer.write(entry.getValue().array());
        }
        writer.writeLong(fileEndPointer);
        long crcValue = crc.getValue();
        writer.writeLong(crcValue);

        // flush and overwrite old file
        fileStream.getChannel().force(true);
        // swap temp file with the original file
        temp.renameTo(fileToWrite);
      } finally {
        writer.close();
        rwLock.readLock().lock();
      }
      logger.info("Completed writing index to file");
    }

    public void map() throws IOException {
      RandomAccessFile raf = new RandomAccessFile(indexFile, "rw");
      rwLock.writeLock().lock();
      try {
        mmap = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, indexFile.length());

        // all the sorted index are read only. set the index to the end
        mmap.position(0);
        mapped = true;
        index = null;
      } finally {
        raf.close();
        rwLock.writeLock().unlock();
      }
    }

    private void readFromFile(File fileToRead) throws FileNotFoundException {
      logger.info("Reading index from file {}", indexFile.getPath());
      index.clear();
      CrcInputStream crcStream = new CrcInputStream(new FileInputStream(fileToRead));
      DataInputStream stream = new DataInputStream(crcStream);
      while (stream.available() > 0)
      try {
        String line = reader.readLine();
        if(line == null)
          throw new IndexCreationException("file does not have any lines");
        Short version = (short)Integer.parseInt(line);
        switch (version) {
          case 0:
            line = reader.readLine();
            if(line == null)
              throw new IndexCreationException("file does not have any lines");
            String[] fields = null;
            while(line != null) {
              fields = line.split("\\s+");
              if (fields.length == 2 && fields[0].compareTo("fileendpointer") == 0) {
                break;
              }
              else if(fields.length == 5) {

                // get key
                StoreKey key = factory.getStoreKey(fields[0]);
                long offset = Long.parseLong(fields[1]);
                long size = Long.parseLong(fields[2]);
                byte flags = Byte.parseByte(fields[3]);
                long timeToLive = Long.parseLong(fields[4]);
                index.put(key, new BlobIndexValue(size, offset, flags, timeToLive));
                logger.trace("Index entry key : {} -> size: {} offset: {} flags: {} timeToLive: {}",
                        key, size, offset, flags, timeToLive);
              }
              else
                throw new IndexCreationException("Malformed line in index file: '%s'.".format(line));
              line = reader.readLine();
            }
            logEndOffset.set(Long.parseLong(fields[1]));
            logger.trace("logEndOffset " + logEndOffset.get());
            break;
          default:
            throw new IndexCreationException("version of index file not supported");
        }
      } finally {
        reader.close();
      }
    }
  }

  private class IndexFilter implements FilenameFilter {

    @Override
    public boolean accept(File dir, String name) {
      return name.endsWith(indexFileName);
    }
  }

  public BlobPersistantIndex(String datadir, Scheduler scheduler, Log log) {
    this.scheduler = scheduler;
    this.log = log;
    File indexDir = new File(datadir);
    File[] indexes = indexDir.listFiles(new IndexFilter());
    // create a map of these with their names
    ConcurrentHashMap<String, File> indexfiles = new ConcurrentHashMap<String, File>();
    for (File index : indexes)
      indexfiles.put(index.getName(), index);

  }

  class IndexPersistor implements Runnable {

    public void write() throws IOException {

      // before iterating the map, get the current file end pointer
      IndexInfo currentInfo = indexes.lastEntry().getValue();
      long fileEndPointer = currentInfo.getEndOffset();

      // flush the log to ensure everything till the fileEndPointer is flushed
      log.flush();

      // write to temp file and then swap with the existing file
      long lastOffset = indexes.lastEntry().getKey();
      IndexInfo prevInfo = indexes.higherEntry(lastOffset).getValue();
      if (!prevInfo.isMapped()) {
        File file = prevInfo.getFile();
        prevInfo.writeIndexToFile(file, prevInfo.getEndOffset());
        prevInfo.map();
      }
      currentInfo.writeIndexToFile(currentInfo.getFile(), fileEndPointer);
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
