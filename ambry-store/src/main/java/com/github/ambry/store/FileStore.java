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

import com.github.ambry.config.FileCopyBasedReplicationConfig;
import com.github.ambry.store.FileStoreException.FileStoreErrorCode;
import com.github.ambry.utils.CrcInputStream;
import com.github.ambry.utils.CrcOutputStream;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
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
public class FileStore implements PartitionFileStore {
  // Logger instance for this class
  private static final Logger logger = LoggerFactory.getLogger(FileStore.class);

  // Flag to track the running state of the FileStore
  private static boolean isRunning = false;

  // Handles serialization/deserialization of file metadata
  // Initialize metadata serializer and store config
  private final FileMetadataSerde fileMetadataSerde = new FileMetadataSerde();;

  // Configuration for file copy operations
  private final String partitionToMountPath;

  // File object for temporary metadata file writing
  private final File tempMetadataFile;

  // File object for actual metadata file writing
  private final File actualMetadataFile;
  // Lock for write operations in Filestore
  private final Object storeWriteLock = new Object();


  /**
   * Creates a new FileStore instance.
   * @param fileCopyBasedReplicationConfig Configuration for file copy operations
   * @param partitionToMountPath partition path for Filestore to access
   * @throws NullPointerException if fileCopyConfig is null
   */
  public FileStore(FileCopyBasedReplicationConfig fileCopyBasedReplicationConfig, String partitionToMountPath) {
    this.partitionToMountPath = partitionToMountPath;

    // Create temporary and actual file paths for metadata persistence
    tempMetadataFile = new File(partitionToMountPath, fileCopyBasedReplicationConfig.fileCopyMetaDataFileName + ".tmp");
    actualMetadataFile = new File(partitionToMountPath, fileCopyBasedReplicationConfig.fileCopyMetaDataFileName);
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
   * validate if a filestore is running
   */
  private void validateIfFileStoreIsRunning() {
    if (!isRunning) {
      throw new FileStoreException("FileStore is not running", FileStoreErrorCode.FileStoreRunningFailure);
    }
  }

  /**
   * Reads a portion of a file into a StoreFileChunk object
   * @param fileName The name of the file to read
   * @param offset   The starting position in the file
   * @param size     The number of bytes to read
   * @return StoreFileChunk containing the requested data
   * @throws StoreException        if there are issues reading the file
   */
  public StoreFileChunk readStoreFileChunkFromDisk(String fileName, long offset, long size, boolean isChunked)
      throws StoreException, IOException {
    // Verify service is running before proceeding
    validateIfFileStoreIsRunning();

    File file = validateAndGetFile(fileName);
    if (!isChunked) {
      return new StoreFileChunk(new DataInputStream(Files.newInputStream(file.toPath())), file.length());
    }
    try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
      // Verify if offset + size is lesser than the filesize to avoid EOF exceptions
      if (offset + size > randomAccessFile.length()){
        throw new IndexOutOfBoundsException("Read offset and size exceeds the filesize for file: " + fileName);
      }
      // Seek to the specified offset
      randomAccessFile.seek(offset);

      // Allocate buffer for reading data
      ByteBuffer buf = ByteBuffer.allocate((int) size);

      // Read data into buffer. Looping since read() doesn't guarantee reading all bytes in one go.
      while (buf.hasRemaining()) {
        int bytesRead = randomAccessFile.getChannel().read(buf);
        if (bytesRead == -1) {
          break; // EOF reached
        }
      }
      // Prepare buffer for reading
      buf.flip();
      // return file chunk buffer read
      return StoreFileChunk.from(buf);
    } catch (FileNotFoundException e) {
      throw new StoreException("File not found while reading chunk for FileCopy", e, StoreErrorCodes.FileNotFound);
    } catch (IOException e) {
      StoreErrorCodes errorCode = StoreException.resolveErrorCode(e);
      throw new StoreException(errorCode.toString() + "while reading chunk for FileCopy", e, errorCode);
    } catch (Exception e){
      logger.error("Error while reading chunk from file: {}", fileName, e);
      throw new FileStoreException("Error while reading chunk from file: " + fileName, e, FileStoreErrorCode.FileStoreReadError);
    }
  }

  /**
   * Writes data from an input stream to a file.
   * @param outputFilePath  The path where the file should be written
   * @param storeFileChunk The StoreFileChunk object representing input stream containing the data to write
   * @throws IOException              if there are issues writing the file
   * @throws FileStoreException       if the service is not running
   * @throws IllegalArgumentException if fileInputStream is null
   */
  public void writeStoreFileChunkToDisk(String outputFilePath, StoreFileChunk storeFileChunk)
      throws IOException {
      // Verify service is running
      validateIfFileStoreIsRunning();

      // Validate input
      Objects.requireNonNull(storeFileChunk, "storeFileChunk must not be null");
      Objects.requireNonNull(storeFileChunk.getStream(), "dataInputStream in storeFileChunk must not be null");

      // Can add buffered streaming to avoid memory overusage if multiple threads calling FileStore.
      // Read the entire file content into memory
      int fileSize = storeFileChunk.getStream().available();
      byte[] content = Utils.readBytesFromStream(storeFileChunk.getStream(), fileSize);

      try {
        synchronized (storeWriteLock) {
          Path outputPath = Paths.get(outputFilePath);
          Path parentDir = outputPath.getParent();

          // Validate if parent directory exists
          if (parentDir != null && !Files.exists(parentDir)) {
            // Throwing IOException if the parent directory does not exist
            throw new IOException("Parent directory does not exist: " + parentDir);
          }
          // Write content to file with create and append options, which will create a new file if file doesn't exist
          // and append to the existing file if file exists
          Files.write(outputPath, content, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
      } catch (Exception e){
        logger.error("Error while writing chunk to file: {}", outputFilePath, e);
        throw new FileStoreException("Error while writing chunk to file: " + outputFilePath, FileStoreErrorCode.FileStoreWriteError);
      }
      logger.info("Write successful for chunk to file: {} with size: {}", outputFilePath, content.length);
  }

  /**
   * Persists metadata for a list of logs to a file.
   * @param logInfoList List of log information to persist
   * @throws IOException              if there are issues writing the metadata
   * @throws FileStoreException       if the service is not running
   * @throws IllegalArgumentException if logInfoList is null
   */
  public void writeMetaDataFileToDisk(List<LogInfo> logInfoList) throws IOException {
    // Verify service is running
    validateIfFileStoreIsRunning();

    // Validate input
    Objects.requireNonNull(logInfoList, "logInfoList must not be null");
    try {
      synchronized (storeWriteLock){
        // Write metadata to temporary file first
        try (FileOutputStream fileStream = new FileOutputStream(tempMetadataFile)) {
          fileMetadataSerde.persist(logInfoList, fileStream);
        }
        logger.info("FileCopyMetadata file serialized and written to temp file: {}", tempMetadataFile.getAbsolutePath());

        // Atomically rename temp file to actual file
        tempMetadataFile.renameTo(actualMetadataFile);
        logger.info("Completed writing filecopy metadata to file {}", actualMetadataFile.getAbsolutePath());
      }
    } catch (IOException e) {
      logger.error("IO error while persisting filecopy metadata to disk {}", tempMetadataFile.getAbsoluteFile());
      throw new FileStoreException("Error while writing metadata to disk", e, FileStoreErrorCode.FileStoreWriteError);
    } catch (Exception e) {
      logger.error("Error while writing metadata to disk", e);
      throw new FileStoreException("Error while writing metadata to disk", e, FileStoreErrorCode.UnknownError);
    }
  }

  /**
   * Reads metadata from a file.
   * @return List of LogInfo objects containing the metadata
   * @throws IOException        if there are issues reading the metadata
   * @throws FileStoreException if the service is not running
   */
  public List<LogInfo> readMetaDataFileFromDisk() throws IOException, StoreException {
    // Verify service is running
    validateIfFileStoreIsRunning();
    validateAndGetFile(actualMetadataFile.getName());
    try {
      // Read metadata from file
      try (FileInputStream fileStream = new FileInputStream(actualMetadataFile)) {
        logger.info("Attempting reading from file: {}", actualMetadataFile.getAbsolutePath());
        return fileMetadataSerde.retrieve(fileStream);
      }
    } catch (IOException e) {
      logger.error("IO error while reading filecopy metadata from disk {}", actualMetadataFile.getAbsoluteFile());
      throw new FileStoreException("Error while reading metadata from disk", e, FileStoreErrorCode.FileStoreReadError);
    } catch (Exception e) {
      logger.error("Error while reading metadata from disk", e);
      throw new FileStoreException("Error while writing metadata to disk", e, FileStoreErrorCode.UnknownError);
    }
  }

  /**
   * Performs cleanup operations when shutting down the FileStore.
   */
  public void shutdown() {
    isRunning = false;
  }

  /**
   * Validates and retrieves a file from the file system.
   * @param fileName The name of the file to retrieve
   * @return File object representing the file
   * @throws StoreException if the file doesn't exist or is not readable
   */
  private File validateAndGetFile(String fileName) throws StoreException {
    String filePath = partitionToMountPath + File.separator + fileName;
    File file = new File(filePath);
    if (!file.exists()) {
      logger.error("File doesn't exist: {}", filePath);
      throw new StoreException("File doesn't exist: " + filePath, StoreErrorCodes.FileNotFound);
    }
    if (!file.canRead()) {
      logger.error("File is not readable: {}", filePath);
      throw new StoreException("File is not readable: " + filePath, StoreErrorCodes.AuthorizationFailure);
    }
    return file;
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
          writer.writeLong(logInfo.getLogSegment().getFileSize());
          writer.writeLong(logInfo.getLogSegment().getFileName().getBytes().length);
          writer.write(logInfo.getLogSegment().getFileName().getBytes());

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
            FileInfo logSegment = new StoreFileInfo(logSegmentName, logSegmentSize);

            // Read index segments
            int indexSegmentsSize = stream.readInt();
            List<FileInfo> indexSegments = new ArrayList<>();
            for(int j = 0; j < indexSegmentsSize; j++){
              Long fileSize = stream.readLong();
              byte[] indexSegmentNameBytes = new byte[(int) stream.readLong()];
              stream.readFully(indexSegmentNameBytes);
              String indexSegmentName = new String(indexSegmentNameBytes);
              indexSegments.add(new StoreFileInfo(indexSegmentName, fileSize));
            }

            // Read bloom filters
            int bloomFiltersSize = stream.readInt();
            List<FileInfo> bloomFilters = new ArrayList<>();
            for(int j = 0; j < bloomFiltersSize; j++){
              Long fileSize = stream.readLong();
              byte[] bloomFilterNameBytes = new byte[(int) stream.readLong()];
              stream.readFully(bloomFilterNameBytes);
              String bloomFilterName = new String(bloomFilterNameBytes);
              bloomFilters.add(new StoreFileInfo(bloomFilterName, fileSize));
            }

            // Create and add LogInfo object to result list
            logInfoList.add(new StoreLogInfo(logSegment, indexSegments, bloomFilters));
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
