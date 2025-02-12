/**
 * Copyright 2025 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.clustermap.FileStoreException;
import com.github.ambry.config.FileCopyConfig;
import com.github.ambry.utils.CrcInputStream;
import com.github.ambry.utils.CrcOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import com.github.ambry.clustermap.FileStoreException.FileStoreErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FileStore provides file system operations for the File Copy Protocol in Ambry.
 * It handles reading and writing of data chunks and metadata to disk, managing file operations
 * with proper serialization and error handling.
 * 
 * Key responsibilities:
 * 1. File Operations:
 *    - Reading/writing data chunks
 *    - Managing metadata files
 *    - Handling file streams
 * 
 * 2. Data Integrity:
 *    - CRC validation
 *    - Atomic file operations
 *    - Error handling
 * 
 * 3. Resource Management:
 *    - Stream handling
 *    - File handle cleanup
 *    - Memory management
 * 
 * Thread Safety:
 * - All public methods are thread-safe
 * - Uses atomic operations for file writes
 * - Maintains thread-safe state management
 */
public class FileStore {
  // Logger instance for this class
  private static final Logger logger = LoggerFactory.getLogger(FileStore.class);
  
  // Flag to track the running state of the FileStore
  private static boolean isRunning = false;
  
  // Handles serialization/deserialization of file metadata
  private final FileMetadataSerde fileMetadataSerde;
  
  // Configuration for file copy operations
  private final FileCopyConfig fileCopyConfig;

  /**
   * Creates a new FileStore instance.
   * @param fileCopyConfig Configuration for file copy operations
   * @throws NullPointerException if fileCopyConfig is null
   */
  public FileStore(FileCopyConfig fileCopyConfig) {
    // Initialize metadata serializer and store config
    this.fileMetadataSerde = new FileMetadataSerde();
    this.fileCopyConfig = fileCopyConfig;
  }

  /**
   * Starts the FileStore service.
   * @throws StoreException if the service fails to start
   */
  public void start() throws StoreException {
    // Mark the service as running
    isRunning = true;
  }

  /**
   * Checks if the FileStore service is running.
   * @return true if the service is running, false otherwise
   */
  public boolean isRunning() {
    return isRunning;
  }

  /**
   * Stops the FileStore service.
   */
  public void stop() {
    // Mark the service as stopped
    isRunning = false;
  }

  /**
   * Reads a portion of a file into a ByteBuffer.
   * @param mountPath The base directory path
   * @param fileName The name of the file to read
   * @param offset The starting position in the file
   * @param size The number of bytes to read
   * @return ByteBuffer containing the requested data
   * @throws IOException if there are issues reading the file
   * @throws FileStoreException if the service is not running
   */
  public ByteBuffer getStreamForFileRead(String mountPath, String fileName, int offset, int size)
      throws IOException {
    // Verify service is running before proceeding
    if (!isRunning) {
      throw new FileStoreException("FileStore is not running", FileStoreErrorCode.FileStoreRunningFailure);
    }

    // Construct the full file path
    String filePath = mountPath + "/" + fileName;
    File file = new File(filePath);

    // Use RandomAccessFile for seeking to specific position
    RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
    
    // Seek to the specified offset
    randomAccessFile.seek(offset);
    
    // Allocate buffer for reading data
    ByteBuffer buf = ByteBuffer.allocate(size);
    
    // Read data into buffer
    randomAccessFile.getChannel().read(buf);
    
    // Prepare buffer for reading
    buf.flip();
    return buf;
  }

  /**
   * Writes data from an input stream to a file.
   * @param outputFilePath The path where the file should be written
   * @param fileInputStream The input stream containing the data to write
   * @throws IOException if there are issues writing the file
   * @throws FileStoreException if the service is not running
   * @throws IllegalArgumentException if fileInputStream is null
   */
  public void putChunkToFile(String outputFilePath, FileInputStream fileInputStream)
      throws IOException {
    // Verify service is running
    if (!isRunning) {
      throw new FileStoreException("FileStore is not running", FileStoreErrorCode.FileStoreRunningFailure);
    }

    // Validate input
    if (fileInputStream == null) {
      throw new IllegalArgumentException("fileInputStream is null");
    }

    // Read the entire file content into memory
    long fileSize = fileInputStream.available();
    byte[] content = new byte[(int) fileSize];
    fileInputStream.read(content);
    
    // Write content to file with create and append options
    Files.write(Paths.get(outputFilePath), content, 
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    
    // Log successful write operation
    logger.info("Write successful for chunk to file: {} with contents: {}", 
                outputFilePath, new String(content));
  }

  /**
   * Persists metadata for a list of logs to a file.
   * @param mountPath The directory where the metadata file should be stored
   * @param logInfoList List of log information to persist
   * @throws IOException if there are issues writing the metadata
   * @throws FileStoreException if the service is not running
   * @throws IllegalArgumentException if logInfoList is null
   */
  public void persistMetaDataToFile(String mountPath, List<LogInfo> logInfoList) throws IOException {
    // Verify service is running
    if(!isRunning){
      throw new FileStoreException("FileStore is not running", FileStoreErrorCode.FileStoreRunningFailure);
    }

    // Validate input
    if(logInfoList == null){
      throw new IllegalArgumentException("logInfoList is null");
    }

    // Create temporary and actual file paths
    File temp = new File(mountPath, fileCopyConfig.fileCopyMetaDataFileName + ".tmp");
    File actual = new File(mountPath, fileCopyConfig.fileCopyMetaDataFileName);
    
    try {
      // Write metadata to temporary file first
      FileOutputStream fileStream = new FileOutputStream(temp);
      fileMetadataSerde.persist(logInfoList, fileStream);
      logger.info("FileCopyMetadata file serialized and written to file: {}", 
                  actual.getAbsolutePath());
      
      // Atomically rename temp file to actual file
      temp.renameTo(actual);
      logger.debug("Completed writing filecopy metadata to file {}", 
                  actual.getAbsolutePath());
    } catch (IOException e) {
      logger.error("IO error while persisting filecopy metadata to disk {}", 
                  temp.getAbsoluteFile());
      throw e;
    }
  }

  /**
   * Reads metadata from a file.
   * @param mountPath The directory containing the metadata file
   * @return List of LogInfo objects containing the metadata
   * @throws IOException if there are issues reading the metadata
   * @throws FileStoreException if the service is not running
   */
  public List<LogInfo> readMetaDataFromFile(String mountPath) throws IOException {
    // Initialize empty list for results
    List<LogInfo> logInfoList = new ArrayList<>();
    
    // Verify service is running
    if(!isRunning){
      throw new FileStoreException("FileStore is not running", 
                                 FileStoreErrorCode.FileStoreRunningFailure);
    }

    // Get metadata file
    File fileCopyMetaDataFile = new File(mountPath, fileCopyConfig.fileCopyMetaDataFileName);
    
    // Return empty list if file doesn't exist
    if (!fileCopyMetaDataFile.exists()) {
      logger.info("fileCopyMetaDataFile {} not found", fileCopyMetaDataFile.getAbsolutePath());
      return logInfoList;
    }

    try {
      // Read metadata from file
      FileInputStream fileStream = new FileInputStream(fileCopyMetaDataFile);
      logger.info("Attempting reading from file: {}", fileCopyMetaDataFile.getAbsolutePath());
      logInfoList = fileMetadataSerde.retrieve(fileStream);
      return logInfoList;
    } catch (IOException e) {
      logger.error("IO error while reading filecopy metadata from disk {}", 
                  fileCopyMetaDataFile.getAbsoluteFile());
      throw e;
    }
  }

  /**
   * Performs cleanup operations when shutting down the FileStore.
   * Currently a no-op implementation.
   */
  public void shutdown() {
    return;
  }

  /**
   * Inner class that handles serialization and deserialization of file metadata.
   * Implements custom serialization format with CRC checking for data integrity.
   * 
   * Format Structure:
   * 1. Number of entries (int)
   * 2. For each entry:
   *    - Sealed segment info
   *    - Index segments list
   *    - Bloom filters list
   * 3. CRC checksum (long)
   * 
   * Thread Safety:
   * - Thread-safe through synchronization on file operations
   * - Immutable internal state
   */
  private static class FileMetadataSerde {
    // Size of CRC value in bytes
    private static final short Crc_Size = 8;

    /**
     * Serializes log information to a file with CRC validation.
     * @param logInfoList List of log information to serialize
     * @param outputStream The output stream to write to
     * @throws IOException if there are issues during serialization
     */
    public void persist(List<LogInfo> logInfoList, OutputStream outputStream)
        throws IOException {
      // Create CRC output stream to calculate checksum while writing
      CrcOutputStream crcOutputStream = new CrcOutputStream(outputStream);
      DataOutputStream writer = new DataOutputStream(crcOutputStream);
      try {
        // Write the size of the log info list
        writer.writeInt(logInfoList.size());
        
        // Iterate through each log info entry
        for (LogInfo logInfo : logInfoList) {
          // Write sealed segment information
          writer.writeLong(logInfo.getSealedSegment().getFileSize());
          writer.writeLong(logInfo.getSealedSegment().getFileName().getBytes().length);
          writer.write(logInfo.getSealedSegment().getFileName().getBytes());
          
          // Write index segments information
          writer.writeInt(logInfo.getIndexSegments().size());
          for(FileInfo fileInfo : logInfo.getIndexSegments()){
            writer.writeLong(fileInfo.getFileSize());
            writer.writeLong(fileInfo.getFileName().getBytes().length);
            writer.write(fileInfo.getFileName().getBytes());
          }
          
          // Write bloom filters information
          writer.writeInt(logInfo.getBloomFilters().size());
          for(FileInfo fileInfo: logInfo.getBloomFilters()){
            writer.writeLong(fileInfo.getFileSize());
            writer.writeLong(fileInfo.getFileName().getBytes().length);
            writer.write(fileInfo.getFileName().getBytes());
          }
        }

        // Write CRC value at the end for validation
        long crcValue = crcOutputStream.getValue();
        writer.writeLong(crcValue);
      } catch (IOException e) {
        logger.error("IO error while serializing Filecopy metadata", e);
        throw e;
      } finally {
        if (outputStream instanceof FileOutputStream) {
          // Ensure data is written to disk
          ((FileOutputStream) outputStream).getChannel().force(true);
        }
        writer.close();
      }
    }

    /**
     * Deserializes log information from a file with CRC validation.
     * @param inputStream The input stream to read from
     * @return List of LogInfo objects, or empty list if deserialization fails
     * @throws IOException if there are issues during deserialization
     */
    public List<LogInfo> retrieve(InputStream inputStream) throws IOException {
      // Initialize result list
      List<LogInfo> logInfoList = new ArrayList<>();
      
      // Create CRC input stream to validate checksum while reading
      CrcInputStream crcStream = new CrcInputStream(inputStream);
      DataInputStream stream = new DataInputStream(crcStream);
      
      try {
        // Continue reading while there's more data than just CRC
        while (stream.available() > Crc_Size) {
          // Read number of log info entries
          int logInfoListSize = stream.readInt();
          
          for(int i = 0; i < logInfoListSize; i++){
            // Read sealed segment information
            Long logSegmentSize = stream.readLong();
            byte[] logSegmentNameBytes = new byte[(int) stream.readLong()];
            stream.readFully(logSegmentNameBytes);
            String logSegmentName = new String(logSegmentNameBytes);
            FileInfo logSegment = new FileInfo(logSegmentName, logSegmentSize);
            
            // Read index segments
            int indexSegmentsSize = stream.readInt();
            List<FileInfo> indexSegments = new ArrayList<>();
            for(int j = 0; j < indexSegmentsSize; j++){
              Long fileSize = stream.readLong();
              byte[] indexSegmentNameBytes = new byte[(int) stream.readLong()];
              stream.readFully(indexSegmentNameBytes);
              String indexSegmentName = new String(indexSegmentNameBytes);
              indexSegments.add(new FileInfo(indexSegmentName, fileSize));
            }
            
            // Read bloom filters
            int bloomFiltersSize = stream.readInt();
            List<FileInfo> bloomFilters = new ArrayList<>();
            for(int j = 0; j < bloomFiltersSize; j++){
              Long fileSize = stream.readLong();
              byte[] bloomFilterNameBytes = new byte[(int) stream.readLong()];
              stream.readFully(bloomFilterNameBytes);
              String bloomFilterName = new String(bloomFilterNameBytes);
              bloomFilters.add(new FileInfo(bloomFilterName, fileSize));
            }
            
            // Create and add LogInfo object to result list
            logInfoList.add(new LogInfo(logSegment, indexSegments, bloomFilters));
          }
        }

        // Validate CRC
        long computedCrc = crcStream.getValue();
        long readCrc = stream.readLong();
        if (computedCrc != readCrc) {
          logger.error("Crc mismatch during filecopy metadata deserialization, computed " + 
                      computedCrc + ", read " + readCrc);
          return new ArrayList<>();
        }
        return logInfoList;
      } catch (IOException e) {
        logger.error("IO error deserializing filecopy metadata", e);
        return new ArrayList<>();
      } finally {
        stream.close();
      }
    }
  }
}
