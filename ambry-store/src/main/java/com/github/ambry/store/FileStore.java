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

import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.store.FileStoreException.FileStoreErrorCode;
import com.github.ambry.utils.CrcInputStream;
import com.github.ambry.utils.CrcOutputStream;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.store.FileStoreException.FileStoreErrorCode.*;


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
  private boolean isRunning = false;

  // Handles serialization/deserialization of file metadata
  // Initialize metadata serializer and store config
  private final FileMetadataSerde fileMetadataSerde = new FileMetadataSerde();

  private final String partitionToMountPath;

  // Lock for write operations in Filestore
  private final Object storeWriteLock = new Object();
  // Pre-allocates files on disk on startup and allocates/free on demand
  private final DiskSpaceAllocator diskSpaceAllocator;
  // Size of each log segment

  // Executor service for handling asynchronous writes to disk
  ExecutorService executor = Executors.newSingleThreadExecutor();

  // Last executed future for async writes to disk. This is used to synchronise the writes to disk in a sequential manner.
  Future<?> lastExecutedFuture = null;
  /**
   * Size of each segment in bytes.
   * This is used to determine how much space to allocate for each segment.
   */
  private long segmentSize;

  /**
   * Suffix for log files.
   */
  private final String LOG_SUFFIX = "_log";

  /**
   * Suffix for index files.
   */
  private final String INDEX_SUFFIX = "_index";


  /**
   * Creates a new FileStore instance.
   * @param partitionToMountPath partition path for Filestore to access
   *
   * @throws NullPointerException if fileCopyConfig is null
   */
  public FileStore(String partitionToMountPath, DiskSpaceAllocator diskSpaceAllocator) {
    this.partitionToMountPath = partitionToMountPath;
    this.diskSpaceAllocator = diskSpaceAllocator;
  }

  /**
   * Starts the FileStore service.
   * @throws StoreException if the service fails to start
   */
  public void start(long segmentSize) throws StoreException {
    // Mark the service as running
    isRunning = true;
    this.segmentSize = segmentSize;
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
      if (offset + size > randomAccessFile.length()) {
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
    } catch (Exception e) {
      logger.error("Error while reading chunk from file: {}", fileName, e);
      throw new FileStoreException("Error while reading chunk from file: " + fileName, e,
          FileStoreErrorCode.FileStoreReadError);
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
  public void writeStoreFileChunkToDisk(String outputFilePath, StoreFileChunk storeFileChunk) throws IOException {
    // Verify service is running
    validateIfFileStoreIsRunning();

    // Validate input
    Objects.requireNonNull(storeFileChunk, "storeFileChunk must not be null");
    Objects.requireNonNull(storeFileChunk.getStream(), "dataInputStream in storeFileChunk must not be null");

    // Can add buffered streaming to avoid memory overusage if multiple threads calling FileStore.
    // Read the entire file content into memory
    int fileSize = storeFileChunk.getStream().available();
    byte[] content = Utils.readBytesFromStream(storeFileChunk.getStream(), fileSize);

    Path outputPath = Paths.get(outputFilePath);
    Path parentDir = outputPath.getParent();
    String targetPathString = "";
    try {
      synchronized (storeWriteLock) {
        String targetOutputString = updatePathWithResetCompactionIndex(outputFilePath);
        Path targetOutputPath = Paths.get(targetOutputString);
        // Validate if parent directory exists
        if (parentDir != null && !Files.exists(parentDir)) {
          // Throwing IOException if the parent directory does not exist
          throw new IOException("Parent directory does not exist: " + parentDir);
        }
        if(lastExecutedFuture != null){
          lastExecutedFuture.get();
        }

        // Write content to file with create and append options, which will create a new file if file doesn't exist
        // and append to the existing file if file exists
        Future<?> currentAsynWriteTaskFuture = executor.submit(() -> {
          try (FileOutputStream fos = new FileOutputStream(targetOutputPath.toFile(), true)) {
            fos.write(content);
            fos.flush();
          } catch (IOException e) {
            logger.error("Error while writing chunk to file: {}", outputFilePath, e);
          }
        });
        lastExecutedFuture = currentAsynWriteTaskFuture;
      }
    } catch (Exception e) {
      logger.error("Error while writing chunk to file: {}", targetPathString, e);
      throw new FileStoreException("Error while writing chunk to file: " + targetPathString,
          FileStoreWriteError);
    }
    logger.info("Write successful for chunk to file: {} with size: {}", targetPathString, content.length);
  }

  /**
   * Moves all regular files from the given source directory to the destination directory. This throws an exception in
   * case any file already exists with the same name in the destination.
   * Note: Both 'srcDirPath' and 'destDirPath' must be subpaths of 'partitionToMountPath'.
   *
   * @param srcDirPath   the path to the source directory
   * @param destDirPath  the path to the destination directory
   * @throws IOException if an I/O error occurs during the move operation
   */
  public void moveAllRegularFiles(String srcDirPath, String destDirPath) throws IOException {
    // Verify service is running.
    if(lastExecutedFuture != null && !lastExecutedFuture.isDone()) {
      logger.warn("Last executed future is not done yet, waiting for it to complete before proceeding.");
      try {
        lastExecutedFuture.get();
      } catch (InterruptedException | ExecutionException e) {
        logger.error("Thread was Interrupted while waiting for last executed future to complete", e);
        throw new RuntimeException(e);
      }
    }

    validateIfFileStoreIsRunning();

    // Validate inputs.
    Objects.requireNonNull(srcDirPath, "srcDirPath must not be null");
    Objects.requireNonNull(destDirPath, "destDirPath must not be null");

    try {
      synchronized (storeWriteLock) {
        // Ensure both source and destination are under the 'partitionToMountPath'.
        if (!srcDirPath.startsWith(partitionToMountPath +  File.separator)) {
          throw new IOException("Source directory is not under mount path: " + partitionToMountPath);
        }
        if (!destDirPath.startsWith(partitionToMountPath)) {
          throw new IOException("Destination directory is not under mount path: " + partitionToMountPath);
        }

        Path source = Paths.get(srcDirPath);
        Path destination = Paths.get(destDirPath);

        // Validate if source directory exists.
        if (!Files.exists(source)) {
          throw new IOException("Source directory does not exist: " + srcDirPath);
        }
        // Validate if destination directory exists.
        if (!Files.exists(destination)) {
          throw new IOException("Destination directory does not exist: " + destDirPath);
        }

        // 1. Check there are no regular files exist with the same name in the destination.
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(source)) {
          for (Path file : stream) {
            if (Files.isRegularFile(file)) {
              Path destFile = destination.resolve(file.getFileName());
              if (Files.exists(destFile)) {
                throw new IOException("File with the same name already exists: " + destFile);
              }
            }
          }
        }

        // 2. Move all regular files from the source directory.
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(source)) {
          for (Path file : stream) {
            if (Files.isRegularFile(file)) {
              Path destFile = destination.resolve(file.getFileName());
              Files.move(file, destFile);
              logger.info("Moved file: {}", file.getFileName());
            }
          }
        }
      }
    } catch (Exception e) {
      logger.error("Unexpected error while moving files from: {} to {}", srcDirPath, destDirPath, e);
      throw new FileStoreException("Error while moving files from: " + srcDirPath + " to: " + destDirPath, FileStoreErrorCode.FileStoreMoveFilesError);
    }
    logger.info("All regular files are moved from: {} to: {}", srcDirPath, destDirPath);
  }

  /**
   * Allocates a file in the specified path.
   * @param path The path where the file should be allocated
   * @param storeId The store ID for which the file is requested
   * @throws IOException if an I/O error occurs during the allocation
   */
  @Override
  public void allocateFile(String path, String storeId) throws IOException {
    String targetPath = updatePathWithResetCompactionIndex(path);

    try {
      // Verify service is running.
      validateIfFileStoreIsRunning();

      // Validate inputs.
      Objects.requireNonNull(path, "path must not be null");
      Objects.requireNonNull(storeId, "storeId must not be null");
      // throw error if the file exists
      File file = new File(targetPath);
      if (file.exists()) {
        throw new IOException("File already exists in " + targetPath);
      }

      // Allocate space for the file
      synchronized (storeWriteLock) {
        diskSpaceAllocator.allocate(file, segmentSize, storeId, false);
      }
    } catch (Exception e) {
      logger.error("Unexpected error while allocating file in path {} for store {}", targetPath, storeId, e);
      throw new FileStoreException("Error while allocating file in path " + targetPath + " for store: " + storeId,
          FileStoreErrorCode.FileStoreFileAllocationFailed);
    }

    logger.info("Disk Space Allocator has allocated space for file in {}", targetPath);
  }

  /**
   * Cleans up the specified file.
   * @param path The path of the file to clean
   * @throws IOException if an I/O error occurs during the cleanup
   */
  @Override
  public void cleanLogFile(String path, String storeId) throws IOException {
    try {
      // Verify service is running.
      validateIfFileStoreIsRunning();

      // Validate inputs.
      Objects.requireNonNull(path, "target file must not be null");

      // throw error if the file does not exist
      File file = new File(path);
      if (!file.exists()) {
        throw new IOException("File doesn't exist in " + path);
      }

      // free space for the file
      synchronized (storeWriteLock) {
        diskSpaceAllocator.free(file, segmentSize, storeId, false);
      }
    } catch (Exception e) {
      logger.error("Unexpected error while freeing file in path {} for store {}", path, storeId, e);
      throw new FileStoreException("Error while freeing file in path " + path + " for store: " + storeId,
          FileStoreErrorCode.FileStoreFileFailedCleanUp);
    }

    logger.info("Disk Space Allocator has free-ed space for file in {}", path);
  }



  /**
   * Cleans up the staging directory by deleting all files and directories within it.
   * It also deletes any files in the root directory that match the prefixes of the staging files.
   *
   * @param targetPath The path to the root directory where the staging directory is located
   * @param stagingDirectoryName The name of the staging directory to clean up
   * @param storeId The store ID for which the cleanup is being performed
   * @throws IOException if an I/O error occurs during cleanup
   */
  @Override
  public void cleanUpStagingDirectory(@Nonnull String targetPath, @Nonnull String stagingDirectoryName,@Nonnull String storeId)
      throws IOException {
    Objects.requireNonNull(targetPath, "targetPath must not be null");
    Objects.requireNonNull(stagingDirectoryName, "stagingDirectoryName must not be null");
    Objects.requireNonNull(storeId, "storeId must not be null");

    File rootDir = new File(targetPath);
    if (!rootDir.exists() || !rootDir.isDirectory()) {
      throw new IOException("Invalid target path: " + targetPath);
    }

    File stagingDir = new File(rootDir, stagingDirectoryName);
    if (!stagingDir.exists() || !stagingDir.isDirectory()) {
      logger.info("Staging Directory Does not Exist: {}", stagingDir.getAbsolutePath());
      return;
    }

    // 1. Find prefixes from *_index and *_log files in staging directory
    Set<String> prefixes = new HashSet<>();
    File[] stagingFiles = stagingDir.listFiles();
    if (stagingFiles != null) {
      for (File file : stagingFiles) {
        String name = file.getName();
        if (name.endsWith(INDEX_SUFFIX) || name.endsWith(LOG_SUFFIX)) {
          int lastUnderscore = name.lastIndexOf('_');
          if(lastUnderscore != -1){
            if(name.endsWith(LOG_SUFFIX)){
              // If the file ends with _log, we add the prefix without the last underscore
              prefixes.add(name.substring(0, lastUnderscore));
            } else {
              // If the file ends with _index, we add the prefix without the last underscore
              int secondLastUnderscore = name.lastIndexOf('_', lastUnderscore - 1);
              prefixes.add(name.substring(0, secondLastUnderscore));
            }
          }
        }
      }

      // 2. Delete all files in staging directory
      for (File file : stagingFiles) {
        if(!file.isDirectory() && file.getName().endsWith(LOG_SUFFIX)) {
          cleanLogFile(file.getAbsolutePath(), storeId);
        } else {
          deleteRecursively(file);
        }
      }
    }
    if (!stagingDir.delete()) {
      throw new IOException("Failed to delete staging directory: " + stagingDir.getAbsolutePath());
    }

    // 3. Delete files in RootDir with matching prefixes
    File[] targetFiles = rootDir.listFiles();
    if (targetFiles != null) {
      for (File file : targetFiles) {
        String name = file.getName();
        for (String prefix : prefixes) {
          if (name.startsWith(prefix)) {
            if(name.endsWith(LOG_SUFFIX)) {
              try {
                cleanLogFile(file.getAbsolutePath(), storeId);
              } catch (IOException e) {
                logger.error("Failed to clean log file: {}", file.getAbsolutePath(), e);
                throw new FileStoreException("Error while cleaning log file: " + file.getAbsolutePath(),
                    FileStoreErrorCode.FileStoreFileFailedCleanUp);
              }
            } else {
              file.delete();
            }
          }
        }
      }
    }
  }

  private void deleteRecursively(File file) {
    if (file.isDirectory()) {
      File[] children = file.listFiles();
      if (children != null) {
        for (File child : children) {
          deleteRecursively(child);
        }
      }
    }
    file.delete();
  }

  public String updatePathWithResetCompactionIndex(String outputFileString){
    Path outputFilePath = Paths.get(outputFileString);
    Path parent = outputFilePath.getParent();
    String targetFileName = resetCompactionCycleIndexInFileName(
        outputFilePath.getFileName().toString());
    return parent != null ? parent.resolve(targetFileName).toString() : Paths.get(targetFileName).toString();
  }
  /**
   * Resets the compaction cycle index in the file name.
   * @param fileName the file name to reset the compaction cycle index in.
   * @return the file name with the compaction cycle index reset to 0.
   */
  @Override
  public String resetCompactionCycleIndexInFileName(String fileName) {
    String[] parts = fileName.split("_");

    if (parts.length < 3) {
      logger.error("Not enough parts to have a middle index For Resseting Compaction Cycle Index in FileName: {}",
          fileName);
      throw new FileStoreException("Error while splitting file name: " + fileName,
          FileStoreLogAndIndexFileNamingConventionError);
    }
    parts[1] = "0";
    return String.join("_", parts);
  }



  /**
   * Returns the size of the allocated segment in bytes.
   */
  @Override
  public long getSegmentCapacity() {
    // Verify service is running.
    validateIfFileStoreIsRunning();
    return segmentSize;
  }

  /**
   * Calculates checksums for specified byte ranges in a file.
   * @param partitionId The partition ID for which the file belongs
   * @param fileName The name of the file to read
   * @param ranges List of byte ranges for which checksums are to be calculated
   * @return List of checksums for each specified range
   * @throws StoreException if there are issues reading the file or calculating checksums
   */
  public List<String> getChecksumsForRanges(@Nonnull PartitionId partitionId, String fileName, @Nonnull List<Pair<Integer, Integer>> ranges) throws StoreException {
    validateIfFileStoreIsRunning();

    List<String> checksums = new ArrayList<>();
    File file = validateAndGetFile(fileName);
    try {
      for (Pair<Integer, Integer> range : ranges) {
        if (range.getFirst() < 0 || range.getSecond() < 0 || range.getFirst() > range.getSecond()) {
          throw new IllegalArgumentException("Invalid byte range: [" + range.getFirst() + ", " + range.getSecond() + "]");
        }
        try (RandomAccessFile raf = new RandomAccessFile(file, "r"); FileChannel channel = raf.getChannel()) {
          long size = range.getSecond() - range.getFirst();
          ByteBuffer buffer = ByteBuffer.allocateDirect((int) size);
          channel.position(range.getFirst());
          channel.read(buffer);
          buffer.flip();

          byte[] arr = new byte[buffer.remaining()];
          buffer.get(arr);
          checksums.add(Long.toString(Utils.crc32(arr)));
        }
      }
    } catch (FileNotFoundException e) {
      logger.error("File not found: {}", fileName, e);
      throw new FileStoreException("File not found: " + fileName, FileStoreReadError);
    } catch (IOException e) {
      logger.error("IO error while reading file: {}", fileName, e);
      throw new FileStoreException("IO error while reading file: " + fileName, FileStoreReadError);
    } catch (Exception e) {
      logger.error("Unexpected error while calculating checksums for ranges in file: {}", fileName, e);
      throw new FileStoreException("Unexpected error while calculating checksums for ranges in file: " + fileName, e,
          FileStoreErrorCode.UnknownError);
    }
    return checksums;
  }

  /**
   * Performs cleanup operations when shutting down the FileStore.
   */
  public void shutdown() {
    isRunning = false;
    executor.shutdown();
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
    public void persist(List<LogInfo> logInfoList, OutputStream outputStream) throws IOException {
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
          for (FileInfo fileInfo : logInfo.getIndexSegments()) {
            writer.writeLong(fileInfo.getFileSize());
            writer.writeLong(fileInfo.getFileName().getBytes().length);
            writer.write(fileInfo.getFileName().getBytes());
          }

          // Write bloom filters information
          writer.writeInt(logInfo.getBloomFilters().size());
          for (FileInfo fileInfo : logInfo.getBloomFilters()) {
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

          for (int i = 0; i < logInfoListSize; i++) {
            // Read sealed segment information
            Long logSegmentSize = stream.readLong();
            byte[] logSegmentNameBytes = new byte[(int) stream.readLong()];
            stream.readFully(logSegmentNameBytes);
            String logSegmentName = new String(logSegmentNameBytes);
            FileInfo logSegment = new StoreFileInfo(logSegmentName, logSegmentSize);

            // Read index segments
            int indexSegmentsSize = stream.readInt();
            List<FileInfo> indexSegments = new ArrayList<>();
            for (int j = 0; j < indexSegmentsSize; j++) {
              Long fileSize = stream.readLong();
              byte[] indexSegmentNameBytes = new byte[(int) stream.readLong()];
              stream.readFully(indexSegmentNameBytes);
              String indexSegmentName = new String(indexSegmentNameBytes);
              indexSegments.add(new StoreFileInfo(indexSegmentName, fileSize));
            }

            // Read bloom filters
            int bloomFiltersSize = stream.readInt();
            List<FileInfo> bloomFilters = new ArrayList<>();
            for (int j = 0; j < bloomFiltersSize; j++) {
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
          logger.error(
              "Crc mismatch during filecopy metadata deserialization, computed " + computedCrc + ", read " + readCrc);
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
