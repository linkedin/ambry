/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link DiskSpaceAllocator} handles the allocation of disk space to entities that require disk segments, such as the
 * {@link BlobStore}s and {@link BlobStoreCompactor}s. The disk segments will be preallocated on platforms that support
 * the fallocate syscall. The entities that require disk space register there requirements using the
 * {@link #initializePool(Collection)} method. Disk space is allocated using the
 * {@link DiskSpaceAllocator#allocate(File, long)} method. If a segment matching the requested size is not in the pool,
 * it will be created at runtime. Disk space is returned to the pool using the
 * {@link DiskSpaceAllocator#free(File, long)} method. It is up to the requester to keep track of the size of the
 * segment requested.
 *
 * NOTE: Segments can be allocated and freed before pool initialization.  This will fall back to preallocating at
 * runtime if the current pool does not include a segment of the correct size.
 */
class DiskSpaceAllocator {
  private static final Logger logger = LoggerFactory.getLogger(DiskSpaceAllocator.class);
  private static final String RESERVE_FILE_PREFIX = "reserve_";
  private static final String FILE_SIZE_DIR_PREFIX = "reserve_size_";
  private static final FileFilter RESERVE_FILE_FILTER =
      pathname -> pathname.isFile() && pathname.getName().startsWith(RESERVE_FILE_PREFIX);
  private static final FileFilter FILE_SIZE_DIR_FILTER =
      pathname -> pathname.isDirectory() && getFileSizeForDirName(pathname.getName()) != null;
  private final boolean enablePooling;
  private final File reserveDir;
  private final long requiredSwapSegmentsPerSize;
  private final StorageManagerMetrics metrics;
  private final ReserveFileMap reserveFiles = new ReserveFileMap();
  private PoolState poolState = PoolState.NOT_INVENTORIED;
  private Exception inventoryException = null;

  /**
   * This constructor will inventory any existing reserve files in the pool (adding them to the in-memory map) and
   * create the reserve directory if it does not exist. Any files that already exist in the pool can be checked out
   * prior to calling {@link #initializePool(Collection)}. If a file of the correct size does not yet exist, a new file
   * will be allocated at runtime.
   * @param enablePooling if set to {@code false}, the reserve pool will not be initialized/used and new files will be
   *                      created each time a segment is allocated.
   * @param reserveDir the directory where reserve files will reside. If this directory does not exist yet, it will
   *                   be created. This can be {@code null} if pooling is disabled.
   * @param requiredSwapSegmentsPerSize the number of swap segments needed for each segment size in the pool.
   * @param metrics a {@link StorageManagerMetrics} instance.
   * @throws StoreException
   */
  DiskSpaceAllocator(boolean enablePooling, File reserveDir, long requiredSwapSegmentsPerSize,
      StorageManagerMetrics metrics) {
    this.enablePooling = enablePooling;
    this.reserveDir = reserveDir;
    this.requiredSwapSegmentsPerSize = requiredSwapSegmentsPerSize;
    this.metrics = metrics;
    try {
      if (enablePooling) {
        prepareDirectory(reserveDir);
        inventoryExistingReserveFiles();
        poolState = PoolState.INVENTORIED;
      }
    } catch (Exception e) {
      inventoryException = e;
      logger.error("Could not inventory preexisting reserve directory", e);
    }
  }

  /**
   * This method will initialize the pool such that it matches the {@link DiskSpaceRequirements} passed in.
   * The following steps are performed:
   * <ol>
   *   <li>
   *     The requirements list is aggregated into an overall requirements map. This stage will ensure that the swap
   *     segments requirements per each segment size described in a DiskSpaceRequirements struct (even if a blob store
   *     needs 0 additional non-swap segments). The number of swap segments needed is calculated by counting the total
   *     number of swap segments allocated to the stores (as described by the disk space requirements). If the number
   *     of swap segments allocated is less than the required swap segment count (passed in via configuration) for a
   *     certain segment size, the difference is added into the overall requirements map. This ensures that the total
   *     (allocated and pooled) swap segment count equals the required count.
   *   </li>
   *   <li>
   *     Unneeded segments are deleted. These are segments that were present in the pool on disk, but are not needed
   *     according to the overall requirements.
   *   </li>
   *   <li>
   *     Any additional required segments are added. For each segment size, additional files will be preallocated to
   *     until the number of files at that size match the overall requirements map. If fallocate is supported on the
   *     system, and the call fails, pool initialization will fail.
   *   </li>
   * </ol>
   * WARNING: No calls to {@link #allocate(File, long)} and {@link #free(File, long)} may occur while this method is
   *          being executed.
   * @param requirementsList a collection of {@link DiskSpaceRequirements}s objects that describe the number and
   *                         segment size needed by each store.
   * @throws StoreException if the pool could not be allocated to meet the provided requirements
   */
  void initializePool(Collection<DiskSpaceRequirements> requirementsList) throws StoreException {
    long startTime = System.currentTimeMillis();
    try {
      if (enablePooling) {
        if (poolState == PoolState.NOT_INVENTORIED) {
          throw inventoryException;
        }
        Map<Long, Long> overallRequirements = getOverallRequirements(requirementsList);
        deleteExtraSegments(overallRequirements);
        addRequiredSegments(overallRequirements);
        // TODO fill the disk with additional swap segments
        poolState = PoolState.INITIALIZED;
      } else {
        logger.info("Disk segment pooling disabled; pool will not be initialized.");
      }
    } catch (Exception e) {
      metrics.diskSpaceAllocatorInitFailureCount.inc();
      poolState = PoolState.NOT_INVENTORIED;
      throw new StoreException("Exception while initializing DiskSpaceAllocator pool", e,
          StoreErrorCodes.Initialization_Error);
    } finally {
      long elapsedTime = System.currentTimeMillis() - startTime;
      logger.info("initializePool took {} ms", elapsedTime);
      metrics.diskSpaceAllocatorStartTimeMs.update(elapsedTime);
    }
  }

  /**
   * Allocate a file, that is, take a file matching the requested size from the reserve directory and move it to the
   * provided file path
   * @param destinationFile the file path to move the allocated file to.
   * @param sizeInBytes the size in bytes of the requested file.
   * @throws IOException if the file could not be moved to the destination.
   */
  void allocate(File destinationFile, long sizeInBytes) throws IOException {
    long startTime = System.currentTimeMillis();
    try {
      if (enablePooling && poolState != PoolState.INITIALIZED) {
        logger.info("Allocating segment of size {} to {} before pool is fully initialized", sizeInBytes,
            destinationFile.getAbsolutePath());
        metrics.diskSpaceAllocatorAllocBeforeInitCount.inc();
      } else {
        logger.debug("Allocating segment of size {} to {}", sizeInBytes, destinationFile.getAbsolutePath());
      }
      if (destinationFile.exists()) {
        throw new IOException("Destination file already exists: " + destinationFile.getAbsolutePath());
      }
      File reserveFile = null;
      if (poolState != PoolState.NOT_INVENTORIED) {
        reserveFile = reserveFiles.remove(sizeInBytes);
      }
      if (reserveFile == null) {
        if (enablePooling) {
          logger.info(
              "Segment of size {} not found in pool; attempting to create a new preallocated file; poolState: {}",
              sizeInBytes, poolState);
          metrics.diskSpaceAllocatorSegmentNotFoundCount.inc();
        }
        Utils.preAllocateFileIfNeeded(destinationFile, sizeInBytes);
      } else {
        try {
          Files.move(reserveFile.toPath(), destinationFile.toPath());
        } catch (Exception e) {
          reserveFiles.add(sizeInBytes, reserveFile);
          throw e;
        }
      }
    } finally {
      long elapsedTime = System.currentTimeMillis() - startTime;
      logger.debug("allocate took {} ms", elapsedTime);
      metrics.diskSpaceAllocatorAllocTimeMs.update(elapsedTime);
    }
  }

  /**
   * Return a file to the pool. The user must keep track of the size of the file allocated.
   * @param fileToReturn the file to return to the pool.
   * @param sizeInBytes the size of the file to return.
   * @throws IOException if the file to return does not exist or cannot be cleaned or recreated correctly.
   */
  void free(File fileToReturn, long sizeInBytes) throws IOException {
    long startTime = System.currentTimeMillis();
    try {
      if (enablePooling && poolState != PoolState.INITIALIZED) {
        logger.info("Freeing segment of size {} from {} before pool is fully initialized", sizeInBytes,
            fileToReturn.getAbsolutePath());
        metrics.diskSpaceAllocatorFreeBeforeInitCount.inc();
      } else {
        logger.debug("Freeing segment of size {} from {}", sizeInBytes, fileToReturn.getAbsolutePath());
      }
      // For now, we delete the file and create a new one. Newer linux kernel versions support
      // additional fallocate flags, which will be useful for cleaning up returned files.
      Files.delete(fileToReturn.toPath());
      if (poolState == PoolState.INITIALIZED) {
        fileToReturn = createReserveFile(sizeInBytes);
        reserveFiles.add(sizeInBytes, fileToReturn);
      }
    } finally {
      long elapsedTime = System.currentTimeMillis() - startTime;
      logger.debug("free took {} ms", elapsedTime);
      metrics.diskSpaceAllocatorFreeTimeMs.update(elapsedTime);
    }
  }

  /**
   * Inventory existing reserve directories and add entries to {@link #reserveFiles}
   * @return a populated {@link ReserveFileMap}
   */
  private void inventoryExistingReserveFiles() throws IOException {
    File[] fileSizeDirs = reserveDir.listFiles(FILE_SIZE_DIR_FILTER);
    if (fileSizeDirs == null) {
      throw new IOException("Error while listing directories in " + reserveDir.getAbsolutePath());
    }
    for (File fileSizeDir : fileSizeDirs) {
      long sizeInBytes = getFileSizeForDirName(fileSizeDir.getName());
      File[] reserveFilesForSize = fileSizeDir.listFiles(RESERVE_FILE_FILTER);
      if (reserveFilesForSize == null) {
        throw new IOException("Error while listing files in " + fileSizeDir.getAbsolutePath());
      }
      for (File reserveFile : reserveFilesForSize) {
        reserveFiles.add(sizeInBytes, reserveFile);
      }
    }
  }

  /**
   * Iterates over the provided requirements and creates a map that describes the number of segments that need to be in
   * the pool for each segment size.
   * @param requirementsList the collection of {@link DiskSpaceRequirements} objects to be accumulated
   * @return a {@link Map} from segment sizes to the number of reserve segments needed at that size.
   */
  private Map<Long, Long> getOverallRequirements(Collection<DiskSpaceRequirements> requirementsList) {
    Map<Long, Long> overallRequirements = new HashMap<>();
    Map<Long, Long> swapSegmentsUsed = new HashMap<>();

    for (DiskSpaceRequirements requirements : requirementsList) {
      long segmentSizeInBytes = requirements.getSegmentSizeInBytes();
      overallRequirements.put(segmentSizeInBytes,
          overallRequirements.getOrDefault(segmentSizeInBytes, 0L) + requirements.getSegmentsNeeded());
      swapSegmentsUsed.put(segmentSizeInBytes,
          swapSegmentsUsed.getOrDefault(segmentSizeInBytes, 0L) + requirements.getSwapSegmentsInUse());
    }
    for (Map.Entry<Long, Long> sizeAndSegmentsNeeded : overallRequirements.entrySet()) {
      long sizeInBytes = sizeAndSegmentsNeeded.getKey();
      long origSegmentsNeeded = sizeAndSegmentsNeeded.getValue();
      // Ensure that swap segments are only added to the requirements if the number of swap segments allocated to
      // stores is lower than the minimum required swap segments.
      long swapSegmentsNeeded = Math.max(requiredSwapSegmentsPerSize - swapSegmentsUsed.get(sizeInBytes), 0L);
      overallRequirements.put(sizeInBytes, origSegmentsNeeded + swapSegmentsNeeded);
    }
    return Collections.unmodifiableMap(overallRequirements);
  }

  /**
   * Delete the currently-present reserve files that are not required by {@code overallRequirements}.
   * @param overallRequirements a map between segment sizes in bytes and the number of segments needed for that size.
   * @throws IOException
   */
  private void deleteExtraSegments(Map<Long, Long> overallRequirements) throws IOException {
    for (long sizeInBytes : reserveFiles.getFileSizeSet()) {
      Long segmentsNeeded = overallRequirements.get(sizeInBytes);
      if (segmentsNeeded == null || segmentsNeeded == 0) {
        File dirToDelete = new File(reserveDir, generateFileSizeDirName(sizeInBytes));
        Utils.deleteFileOrDirectory(dirToDelete);
      } else {
        while (reserveFiles.getCount(sizeInBytes) > segmentsNeeded) {
          File fileToDelete = reserveFiles.remove(sizeInBytes);
          if (!fileToDelete.delete()) {
            throw new IOException("Could not delete the following reserve file: " + fileToDelete.getAbsolutePath());
          }
        }
      }
    }
  }

  /**
   * Add additional reserve files that are required by {@code overallRequirements}.
   * @param overallRequirements a map between segment sizes in bytes and the number of segments needed for that size.
   * @throws IOException
   */
  private void addRequiredSegments(Map<Long, Long> overallRequirements) throws IOException {
    for (Map.Entry<Long, Long> sizeAndSegmentsNeeded : overallRequirements.entrySet()) {
      long sizeInBytes = sizeAndSegmentsNeeded.getKey();
      long segmentsNeeded = sizeAndSegmentsNeeded.getValue();
      while (reserveFiles.getCount(sizeInBytes) < segmentsNeeded) {
        reserveFiles.add(sizeInBytes, createReserveFile(sizeInBytes));
      }
    }
  }

  /**
   * Create and preallocate (if supported) a reserve file of the specified size.
   * @param sizeInBytes the size to preallocate for the reserve file.
   * @return the created file.
   * @throws IOException if the file could not be created, or if an error occured during the fallocate call.
   */
  private File createReserveFile(long sizeInBytes) throws IOException {
    File fileSizeDir = prepareDirectory(new File(reserveDir, FILE_SIZE_DIR_PREFIX + sizeInBytes));
    File reserveFile;
    do {
      reserveFile = new File(fileSizeDir, generateFilename());
    } while (!reserveFile.createNewFile());
    Utils.preAllocateFileIfNeeded(reserveFile, sizeInBytes);
    return reserveFile;
  }

  /**
   * Create a directory if it does not yet exist and check that it is accessible.
   * @param dir The directory to possibly create.
   * @return the passed-in directory
   * @throws IOException if the specified directory could not be created/read, or if a non-directory file with the same
   *                     name already exists.
   */
  private static File prepareDirectory(File dir) throws IOException {
    if (!dir.exists()) {
      dir.mkdir();
    }
    if (!dir.isDirectory()) {
      throw new IOException(dir.getAbsolutePath() + " is not a directory or could not be created");
    }
    return dir;
  }

  /**
   * @param sizeInBytes the size of the files in this directory
   * @return a directory name for this size. This is reserve_size_{n} where {n} is {@code sizeInBytes}
   */
  static String generateFileSizeDirName(long sizeInBytes) {
    return FILE_SIZE_DIR_PREFIX + sizeInBytes;
  }

  /**
   * @return a filename for a reserve file. This is reserve_ followed by a random UUID.
   */
  private static String generateFilename() {
    return RESERVE_FILE_PREFIX + UUID.randomUUID();
  }

  /**
   * Parse the file size from a file size directory name.
   * @param fileSizeDirName the name of the file size directory.
   * @return the parsed size in bytes, or {@code null} if the directory name did not start with the correct prefix.
   * @throws NumberFormatException if a valid long does not follow the filename prefix.
   */
  private static Long getFileSizeForDirName(String fileSizeDirName) {
    Long sizeInBytes = null;
    if (fileSizeDirName.startsWith(FILE_SIZE_DIR_PREFIX)) {
      String sizeString = fileSizeDirName.substring(FILE_SIZE_DIR_PREFIX.length());
      sizeInBytes = Long.parseLong(sizeString);
    }
    return sizeInBytes;
  }

  /**
   * This is a thread safe data structure that is used to keep track of the files in the reserve pool.
   */
  private static class ReserveFileMap {
    private final ConcurrentMap<Long, Queue<File>> internalMap = new ConcurrentHashMap<>();

    /**
     * Add a file of the specified file size to the reserve file map.
     * @param sizeInBytes the size of the reserve file
     * @param reserveFile the reserve file to add.
     */
    void add(long sizeInBytes, File reserveFile) {
      internalMap.computeIfAbsent(sizeInBytes, key -> new ConcurrentLinkedQueue<>()).add(reserveFile);
    }

    /**
     * Remove and return a file of the specified size from the reserve file map.
     * @param sizeInBytes the size of the file to look for.
     * @return the {@link File}, or {@code null} if no file exists in the map for the specified size.
     */
    File remove(long sizeInBytes) {
      File reserveFile = null;
      Queue<File> reserveFilesForSize = internalMap.get(sizeInBytes);
      if (reserveFilesForSize != null && reserveFilesForSize.size() != 0) {
        reserveFile = reserveFilesForSize.remove();
      }
      return reserveFile;
    }

    /**
     * @param sizeInBytes the size of files of interest
     * @return the number of files in the map of size {@code sizeInBytes}
     */
    int getCount(long sizeInBytes) {
      Queue<File> reserveFilesForSize = internalMap.get(sizeInBytes);
      return reserveFilesForSize != null ? reserveFilesForSize.size() : 0;
    }

    /**
     * @return the set of file sizes present in the map.
     */
    Set<Long> getFileSizeSet() {
      return internalMap.keySet();
    }
  }

  /**
   * Represents the state of pool initialization.
   */
  private enum PoolState {
    /**
     * If the pool is not yet created/inventoried or failed initialization.
     */
    NOT_INVENTORIED,

    /**
     * If the pool has been inventoried but not initialized to match new {@link DiskSpaceRequirements}.
     */
    INVENTORIED,

    /**
     * If the pool was successfully initialized.
     */
    INITIALIZED
  }
}
