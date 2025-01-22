package com.github.ambry.store;

import com.github.ambry.clustermap.FileStoreException;
import com.github.ambry.config.FileCopyConfig;
import com.github.ambry.replication.FindToken;
import com.github.ambry.utils.CrcInputStream;
import com.github.ambry.utils.CrcOutputStream;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import com.github.ambry.clustermap.FileStoreException.FileStoreErrorCode;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileStore {
  private static final Logger logger = LoggerFactory.getLogger(FileStore.class);
  private static boolean isRunning = false;
  private final String dataDir;
  private final FileMetadataSerde fileMetadataSerde;
  private final FileCopyConfig fileCopyConfig;

  public FileStore(String dataDir, FileCopyConfig fileCopyConfig){
    this.dataDir = dataDir;
    this.fileMetadataSerde = new FileMetadataSerde();
    this.fileCopyConfig = fileCopyConfig;
  }

  public void start() throws StoreException {
    isRunning = true;
  }
  public boolean isRunning() {
    return isRunning;
  }
  public void stop() {
    isRunning = false;
  }


  public FileInputStream getStreamForFileRead(String mountPath, String fileName)
      throws IOException {
    if(!isRunning){
      throw new FileStoreException("FileStore is not running", FileStoreErrorCode.FileStoreRunningFailure);
    }
    // TODO: Handle edge cases and validations
    String filePath = mountPath + "/" + fileName;
    File file = new File(filePath);
    // Check if file exists and is readable
    if (!file.exists() || !file.canRead()) {
      throw new IOException("File doesn't exist or cannot be read: " + filePath);
    }
    return new FileInputStream(file);
  }

  public void putChunkToFile(String outputFilePath, FileInputStream fileInputStream)
      throws IOException {
    if(!isRunning){
      throw new FileStoreException("FileStore is not running", FileStoreErrorCode.FileStoreRunningFailure);
    }
    if(fileInputStream == null){
      throw new IllegalArgumentException("fileInputStream is null");
    }
    // TODO: Handle edge cases and validations

    // Determine the size of the file
    long fileSize = fileInputStream.available();

    // Read all bytes from the source file and append them to the output file

    byte[] content = new byte[(int) fileSize]; // Read the content of the source file into a byte array
    fileInputStream.read(content); // Read bytes into the array
    Files.write(Paths.get(outputFilePath), content, StandardOpenOption.CREATE, StandardOpenOption.APPEND);

    System.out.println("Write successful for chunk to file: " + outputFilePath + " with contents: " + new String(content) );
  }

  // New class in input: List<FileMetaData>
  public void persistMetaDataToFile(String mountPath, List<LogInfo> logInfoList) throws IOException {
    if(!isRunning){
      throw new FileStoreException("FileStore is not running", FileStoreErrorCode.FileStoreRunningFailure);
    }
    if(logInfoList == null){
      throw new IllegalArgumentException("logInfoList is null");
    }

    File temp = new File(mountPath, fileCopyConfig.fileCopyMetaDataFileName + ".tmp");
    File actual = new File(mountPath, fileCopyConfig.fileCopyMetaDataFileName);
    try {
      FileOutputStream fileStream = new FileOutputStream(temp);
      fileMetadataSerde.persist(logInfoList, fileStream);
      System.out.println("FileCopyMetadata file serialized and written to file: " + actual.getAbsolutePath());
      // swap temp file with the original file
      temp.renameTo(actual);
      logger.debug("Completed writing remote tokens to file {}", actual.getAbsolutePath());
    } catch (IOException e) {
      logger.error("IO error while persisting tokens to disk {}", temp.getAbsoluteFile());
      throw e;
    }
  }


  public List<LogInfo> readMetaDataFromFile(String mountPath) throws IOException {
    List<LogInfo> logInfoList = new ArrayList<>();
    if(!isRunning){
      throw new FileStoreException("FileStore is not running", FileStoreErrorCode.FileStoreRunningFailure);
    }

    File fileCopyMetaDataFile = new File(mountPath, fileCopyConfig.fileCopyMetaDataFileName);
    if (!fileCopyMetaDataFile.exists()) {
      logger.info("fileCopyMetaDataFile {} not found", fileCopyMetaDataFile.getAbsolutePath());
      return logInfoList;
    }
    try {
      FileInputStream fileStream = new FileInputStream(fileCopyMetaDataFile);
      System.out.println("Attempting reading from file: " + fileCopyMetaDataFile.getAbsolutePath());
      logInfoList = fileMetadataSerde.retrieve(fileStream);
      return logInfoList;
    } catch (IOException e) {
      logger.error("IO error while reading filecopy metadata from disk {}", fileCopyMetaDataFile.getAbsoluteFile());
      throw e;
    }
  }

  public void shutdown(){
    return;
  }

  /**
   * Class to serialize and deserialize replica tokens
   */
  private static class FileMetadataSerde {
    private static final short Crc_Size = 8;
    private static final short VERSION_0 = 0;
    private static final short CURRENT_VERSION = VERSION_0;

    public FileMetadataSerde() {
    }

    /**
     * Serialize the remote tokens to the file
     * @param logInfoList the mapping from the replicas to the remote tokens
     * @param outputStream the file output stream to write to
     */
    public void persist(List<LogInfo> logInfoList, OutputStream outputStream)
        throws IOException {
      CrcOutputStream crcOutputStream = new CrcOutputStream(outputStream);
      DataOutputStream writer = new DataOutputStream(crcOutputStream);
      try {

        writer.writeInt(logInfoList.size());
        for (LogInfo logInfo : logInfoList) {
          // write log segment size and name
          writer.writeLong(logInfo.getSealedSegment().getFileSize());
          writer.writeLong(logInfo.getSealedSegment().getFileName().getBytes().length);
          writer.write(logInfo.getSealedSegment().getFileName().getBytes());
          writer.writeInt(logInfo.getIndexSegments().size());
          for(FileInfo fileInfo : logInfo.getIndexSegments()){
            writer.writeLong(fileInfo.getFileSize());
            writer.writeLong(fileInfo.getFileName().getBytes().length);
            writer.write(fileInfo.getFileName().getBytes());
          }
          writer.writeInt(logInfo.getBloomFilters().size());
          for(FileInfo fileInfo: logInfo.getBloomFilters()){
            writer.writeLong(fileInfo.getFileSize());
            writer.writeLong(fileInfo.getFileName().getBytes().length);
            writer.write(fileInfo.getFileName().getBytes());
          }
        }

        long crcValue = crcOutputStream.getValue();
        writer.writeLong(crcValue);
      } catch (IOException e) {
        logger.error("IO error while serializing remote peer tokens", e);
        throw e;
      } finally {
        if (outputStream instanceof FileOutputStream) {
          // flush and overwrite file
          ((FileOutputStream) outputStream).getChannel().force(true);
        }
        writer.close();
      }
    }

    /**
     * Deserialize the remote tokens
     * @param inputStream the input stream from the persistent file
     * @return the mapping from replicas to remote tokens
     */
    public List<LogInfo> retrieve(InputStream inputStream) throws IOException {
      List<LogInfo> logInfoList = new ArrayList<>();
      CrcInputStream crcStream = new CrcInputStream(inputStream);
      DataInputStream stream = new DataInputStream(crcStream);
      ConcurrentMap<String, Pair<Long, FindToken>> peerTokens = new ConcurrentHashMap<>();
      try {
        while (stream.available() > Crc_Size) {
          int logInfoListSize = stream.readInt();
          for(int i = 0; i < logInfoListSize; i++){
            // read log segment name
            Long logSegmentSize = stream.readLong();
            byte[] logSegmentNameBytes = new byte[(int) stream.readLong()];
            stream.readFully(logSegmentNameBytes);
            String logSegmentName = new String(logSegmentNameBytes);
            FileInfo logSegment = new FileInfo(logSegmentName, logSegmentSize);
            // read index segments
            int indexSegmentsSize = stream.readInt();
            List<FileInfo> indexSegments = new ArrayList<>();
            for(int j = 0; j < indexSegmentsSize; j++){
              Long fileSize = stream.readLong();
              byte[] indexSegmentNameBytes = new byte[(int) stream.readLong()];
              stream.readFully(indexSegmentNameBytes);
              String indexSegmentName = new String(indexSegmentNameBytes);
              indexSegments.add(new FileInfo(indexSegmentName, fileSize));
            }
            // read bloom filters
            int bloomFiltersSize = stream.readInt();
            List<FileInfo> bloomFilters = new ArrayList<>();
            for(int j = 0; j < bloomFiltersSize; j++){
              Long fileSize = stream.readLong();
              byte[] bloomFilterNameBytes = new byte[(int) stream.readLong()];
              stream.readFully(bloomFilterNameBytes);
              String bloomFilterName = new String(bloomFilterNameBytes);
              bloomFilters.add(new FileInfo(bloomFilterName, fileSize));
            }
            logInfoList.add(new LogInfo(logSegment, indexSegments, bloomFilters));
          }
        }

        long computedCrc = crcStream.getValue();
        long readCrc = stream.readLong();
        if (computedCrc != readCrc) {
          logger.error("Crc mismatch during peer token deserialization, computed " + computedCrc + ", read " + readCrc);
          return new ArrayList<>();
        }
        return logInfoList;
      } catch (IOException e) {
        logger.error("IO error deserializing remote peer tokens", e);
        return new ArrayList<>();
      } finally {
        stream.close();
      }
    }
  }
}
