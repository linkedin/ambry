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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link DiskSpaceAllocator} handles the allocation of disk space to entities that require disk segments, such as the
 * {@link BlobStore}s and {@link BlobStoreCompactor}s. The disk segments will be preallocated on platforms that support
 * the fallocate syscall. The entities that require disk space register there requirements using the
 * {@link #initializePool(Collection)} method. Disk space is allocated using the
 * {@link DiskSpaceAllocator#allocate(File, long, String, boolean)} method. If a segment matching the requested size is not in the pool,
 * it will be created at runtime. Disk space is returned to the pool using the
 * {@link DiskSpaceAllocator#free(File, long, String, boolean)} method. It is up to the requester to keep track of the size of the
 * segment requested.
 *
 * NOTE: Segments can be allocated and freed before pool initialization.  This will fall back to preallocating at
 * runtime if the current pool does not include a segment of the correct size.
 */
class DiskSpaceAllocator {
  static final String STORE_DIR_PREFIX = "reserve_store_";
  static final String SWAP_DIR_NAME = "reserve_swap";
  static final String FILE_SIZE_DIR_PREFIX = "reserve_size_";
  static final String RESERVE_FILE_PREFIX = "reserve_";
  private static final FileFilter RESERVE_FILE_FILTER =
      pathname -> pathname.isFile() && pathname.getName().startsWith(RESERVE_FILE_PREFIX);
  private static final FileFilter FILE_SIZE_DIR_FILTER =
      pathname -> pathname.isDirectory() && getFileSizeForDirName(pathname.getName()) != null;
  private static final Logger logger = LoggerFactory.getLogger(DiskSpaceAllocator.class);

  private final boolean enablePooling;
  private final File reserveDir;
  private final File swapReserveDir;
  private final long requiredSwapSegmentsPerSize;
  private final StorageManagerMetrics metrics;
  // In storeReserveFiles hashmap, key is store id and value is each store's reserve pool represented by ReserveFileMap.
  // The reason to use ReserveFileMap here is to ensure reserve pool of each store is thread-safe. Note that different
  // threads may perform operation on same store. For example, request handler thread and compaction manager thread operates
  // on same store. Hence store's reserve pool should handle concurrent access in this case.
  private final Map<String, ReserveFileMap> storeReserveFiles = new HashMap<>();
  private final ReserveFileMap swapReserveFiles = new ReserveFileMap();
  // A map keeps total number of swap segments by size that include both segments in use and segments available in pool.
  private final Map<Long, Long> swapSegmentNumBySize = new HashMap<>();
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
   */
  DiskSpaceAllocator(boolean enablePooling, File reserveDir, long requiredSwapSegmentsPerSize,
      StorageManagerMetrics metrics) {
    this.enablePooling = enablePooling;
    this.reserveDir = reserveDir;
    swapReserveDir = new File(reserveDir, SWAP_DIR_NAME);
    this.requiredSwapSegmentsPerSize = requiredSwapSegmentsPerSize;
    this.metrics = metrics;
    try {
      if (enablePooling) {
        prepareDirectory(reserveDir);
        prepareDirectory(swapReserveDir);
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
   * WARNING: No calls to {@link #allocate(File, long, String, boolean)} and {@link #free(File, long, String, boolean)} may occur while this method is
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
        // The requirements for each store and swap segments pool
        Map<String, Map<Long, Long>> overallRequirements = getOverallRequirements(requirementsList);
        deleteExtraSegments(overallRequirements);
        addRequiredSegments(overallRequirements, true);
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
   * @param storeId the id of store that requests segment from pool.
   * @param isSwapSegment {@code true} if the segment is used for swap purpose in compaction. {@code false} otherwise.
   * @throws IOException if the file could not be moved to the destination.
   */
  void allocate(File destinationFile, long sizeInBytes, String storeId, boolean isSwapSegment) throws IOException {
    long startTime = System.currentTimeMillis();
    try {
      if (enablePooling && poolState != PoolState.INITIALIZED) {
        logger.info("Allocating segment of size {} to {} before pool is fully initialized", sizeInBytes,
            destinationFile.getAbsolutePath());
        metrics.diskSpaceAllocatorAllocBeforeInitCount.inc();
      } else {
        logger.info("Allocating segment of size {} to {}", sizeInBytes, destinationFile.getAbsolutePath());
      }
      if (destinationFile.exists()) {
        throw new IOException("Destination file already exists: " + destinationFile.getAbsolutePath());
      }
      File reserveFile = null;
      if (poolState != PoolState.NOT_INVENTORIED) {
        if (isSwapSegment) {
          // attempt to get segment from swap reserve directory
          reserveFile = swapReserveFiles.remove(sizeInBytes);
        } else {
          // attempt to get segment from corresponding store reserve directory
          if (storeReserveFiles.containsKey(storeId)) {
            reserveFile = storeReserveFiles.get(storeId).remove(sizeInBytes);
          }
        }
      }
      if (reserveFile == null) {
        if (enablePooling) {
          logger.info("Segment of size {} not found in {}; attempting to create a new preallocated file; poolState: {}",
              sizeInBytes, isSwapSegment ? SWAP_DIR_NAME + " directory" : STORE_DIR_PREFIX + storeId + " directory",
              poolState);
          metrics.diskSpaceAllocatorSegmentNotFoundCount.inc();
        }
        Utils.preAllocateFileIfNeeded(destinationFile, sizeInBytes);
      } else {
        logger.info("Moving segment with size {} from {} to {}", sizeInBytes,
            isSwapSegment ? SWAP_DIR_NAME + " directory" : STORE_DIR_PREFIX + storeId + " directory",
            destinationFile.getAbsolutePath());
        try {
          Files.move(reserveFile.toPath(), destinationFile.toPath());
        } catch (Exception e) {
          // return the segment (if exists) to corresponding store reserve dir or swap reserve dir which it belongs to
          if (reserveFile.exists()) {
            if (isSwapSegment) {
              swapReserveFiles.add(sizeInBytes, reserveFile);
            } else {
              storeReserveFiles.get(storeId).add(sizeInBytes, reserveFile);
            }
          } else {
            logger.warn("Reserve file {} doesn't exist when adding it back to in-mem file map",
                reserveFile.getAbsolutePath());
          }
          throw e;
        }
      }
    } finally {
      long elapsedTime = System.currentTimeMillis() - startTime;
      logger.debug("allocating new segment took {} ms for {}", elapsedTime, "store[" + storeId + "]");
      metrics.diskSpaceAllocatorAllocTimeMs.update(elapsedTime);
    }
  }

  /**
   * Return a file to the pool. The user must keep track of the size of the file allocated.
   * @param fileToReturn the file to return to the pool.
   * @param sizeInBytes the size of the file to return.
   * @param storeId the id of store from which the segment is released.
   * @param isSwapSegment {@code true} if segment to return was used for swap purpose in compaction.
   *                      {@code false} otherwise.
   * @throws IOException if the file to return does not exist or cannot be cleaned or recreated correctly.
   */
  void free(File fileToReturn, long sizeInBytes, String storeId, boolean isSwapSegment) throws IOException {
    long startTime = System.currentTimeMillis();
    try {
      if (enablePooling && poolState != PoolState.INITIALIZED) {
        logger.info("Freeing segment of size {} from {} before pool is fully initialized", sizeInBytes,
            fileToReturn.getAbsolutePath());
        metrics.diskSpaceAllocatorFreeBeforeInitCount.inc();
      } else {
        logger.info("Freeing {} segment of size {} from {}", isSwapSegment ? "swap" : "store[" + storeId + "]",
            sizeInBytes, fileToReturn.getAbsolutePath());
      }
      // For now, we delete the file and create a new one. Newer linux kernel versions support
      // additional fallocate flags, which will be useful for cleaning up returned files.
      Files.delete(fileToReturn.toPath());
      if (poolState == PoolState.INITIALIZED) {
        if (isSwapSegment) {
          fileToReturn = createReserveFile(sizeInBytes, swapReserveDir);
          swapReserveFiles.add(sizeInBytes, fileToReturn);
        } else {
          File storeReserveDir = new File(reserveDir, STORE_DIR_PREFIX + storeId);
          fileToReturn = createReserveFile(sizeInBytes, storeReserveDir);
          ReserveFileMap sizeToFilesMap = storeReserveFiles.getOrDefault(storeId, new ReserveFileMap());
          sizeToFilesMap.add(sizeInBytes, fileToReturn);
          storeReserveFiles.put(storeId, sizeToFilesMap);
        }
      }
    } finally {
      long elapsedTime = System.currentTimeMillis() - startTime;
      logger.debug("free took {} ms for store[{}]", elapsedTime, storeId);
      metrics.diskSpaceAllocatorFreeTimeMs.update(elapsedTime);
    }
  }

  /**
   * Inventory existing reserve directories and add entries to {@link #storeReserveFiles} and {@link #swapReserveFiles}
   * The hierarchy of reserve pool on a single disk is as follows.
   * <pre>
   * --- reserve_pool
   *       |--- reserve_store_1
   *       |              |--- reserve_size_2147483648
   *       |
   *       |--- reserve_store_2
   *                      |--- reserve_size_2147483648
   *       ...
   *       |--- reserve_store_n
   *       |              |--- reserve_size_2147483648
   *       |
   *       |--- reserve_swap (# shared by all stores on same disk)
   *                      |--- reserve_size_2147483648
   *                      |--- reserve_size_8589934592
   * </pre>
   */
  private void inventoryExistingReserveFiles() throws IOException {
    File[] allFiles = reserveDir.listFiles();
    if (allFiles == null) {
      throw new IOException("Error while listing directories in " + reserveDir.getAbsolutePath());
    }
    for (File file : allFiles) {
      String storeId = getStoreIdForDirName(file.getName());
      if (file.isDirectory() && storeId != null) {
        inventoryStoreReserveFiles(file, storeId);
      } else if (file.isDirectory() && file.getName().equals(SWAP_DIR_NAME)) {
        inventorySwapReserveFiles(file);
      } else {
        // if it is neither store reserved segment directory nor swap segment directory, then delete it.
        Utils.deleteFileOrDirectory(file);
      }
    }
  }

  /**
   * Inventory existing store reserve directories and add entries to {@link #storeReserveFiles}
   * @param storeReserveDir the directory where store reserve files reside.
   * @param storeId the id of store which files in this directory belong to.
   * @throws IOException
   */
  private void inventoryStoreReserveFiles(File storeReserveDir, String storeId) throws IOException {
    File[] fileSizeDirs = storeReserveDir.listFiles(FILE_SIZE_DIR_FILTER);
    if (fileSizeDirs == null) {
      throw new IOException("Error while listing directories in " + storeReserveDir.getAbsolutePath());
    }
    ReserveFileMap sizeToFileMap = storeReserveFiles.getOrDefault(storeId, new ReserveFileMap());
    for (File fileSizeDir : fileSizeDirs) {
      long sizeInBytes = getFileSizeForDirName(fileSizeDir.getName());
      File[] reserveFilesForSize = fileSizeDir.listFiles(RESERVE_FILE_FILTER);
      if (reserveFilesForSize == null) {
        throw new IOException("Error while listing store reserve files in " + fileSizeDir.getAbsolutePath());
      }
      for (File reserveFile : reserveFilesForSize) {
        sizeToFileMap.add(sizeInBytes, reserveFile);
      }
    }
    storeReserveFiles.put(storeId, sizeToFileMap);
  }

  /**
   * Inventory existing swap segment directory and add entries to {@link #swapReserveFiles}
   * @param swapReserveDir the directory where swap segment files reside.
   * @throws IOException
   */
  private void inventorySwapReserveFiles(File swapReserveDir) throws IOException {
    File[] fileSizeDirs = swapReserveDir.listFiles(FILE_SIZE_DIR_FILTER);
    if (fileSizeDirs == null) {
      throw new IOException("Error while listing directories in " + swapReserveDir.getAbsolutePath());
    }
    for (File fileSizeDir : fileSizeDirs) {
      long sizeInBytes = getFileSizeForDirName(fileSizeDir.getName());
      File[] reserveFilesForSize = fileSizeDir.listFiles(RESERVE_FILE_FILTER);
      if (reserveFilesForSize == null) {
        throw new IOException("Error while listing swap segment files in " + fileSizeDir.getAbsolutePath());
      }
      for (File reserveFile : reserveFilesForSize) {
        swapReserveFiles.add(sizeInBytes, reserveFile);
      }
    }
  }

  /**
   * Iterates over the provided requirements and creates a map that describes the number of segments that need to be in
   * the pool for each segment size.
   * @param requirementsList the collection of {@link DiskSpaceRequirements} objects to be accumulated
   * @return a {@link Map} from segment sizes to the number of reserve segments needed at that size.
   */
  Map<String, Map<Long, Long>> getOverallRequirements(Collection<DiskSpaceRequirements> requirementsList) {
    Map<String, Map<Long, Long>> overallRequirements = new HashMap<>();
    Map<Long, Long> swapSegmentsUsed = new HashMap<>();
    Map<Long, Long> swapSegmentsRequired = new HashMap<>();

    for (DiskSpaceRequirements requirements : requirementsList) {
      long segmentSizeInBytes = requirements.getSegmentSizeInBytes();
      String storeId = requirements.getStoreId();
      Map<Long, Long> storeSegmentRequirements = new HashMap<>();
      storeSegmentRequirements.put(segmentSizeInBytes, requirements.getSegmentsNeeded());
      overallRequirements.put(storeId, storeSegmentRequirements);
      swapSegmentsUsed.put(segmentSizeInBytes,
          swapSegmentsUsed.getOrDefault(segmentSizeInBytes, 0L) + requirements.getSwapSegmentsInUse());
    }
    for (Map.Entry<Long, Long> sizeAndSwapSegments : swapSegmentsUsed.entrySet()) {
      long sizeInBytes = sizeAndSwapSegments.getKey();
      long existingSegmentsNum = swapSegmentNumBySize.getOrDefault(sizeInBytes, 0L);
      long swapSegmentsNeededNum =
          Math.max(requiredSwapSegmentsPerSize - swapSegmentsUsed.get(sizeInBytes) - existingSegmentsNum, 0L);
      // Update swapSegmentNumBySize map by adding up existing segments, segments in use and segments needed
      // During server startup, existing segments = 0;
      // During new store addition, segments in use = 0;
      swapSegmentNumBySize.put(sizeInBytes,
          existingSegmentsNum + swapSegmentsUsed.get(sizeInBytes) + swapSegmentsNeededNum);
      swapSegmentsRequired.put(sizeInBytes, swapSegmentsNeededNum);
    }
    overallRequirements.put(SWAP_DIR_NAME, swapSegmentsRequired);
    return Collections.unmodifiableMap(overallRequirements);
  }

  /**
   * Delete all the unneeded segments associated with stores that are being removed from current node.
   * @param storeIds a list of store ids whose reserved segments should be deleted.
   * @throws IOException
   */
  void deleteAllSegmentsForStoreIds(List<String> storeIds) throws IOException {
    for (String storeId : storeIds) {
      // remove store reserve dir from in-mem data structure and then delete whole directory
      storeReserveFiles.remove(storeId);
      File storeReserveDir = new File(reserveDir, STORE_DIR_PREFIX + storeId);
      logger.info("Deleting reserve store dir: {}", storeReserveDir.getAbsolutePath());
      Utils.deleteFileOrDirectory(storeReserveDir);
    }
  }

  /**
   * Delete the currently-present reserve files that are not required by {@code overallRequirements}.
   * @param overallRequirements a map between segment sizes in bytes and the number of segments needed for that size.
   * @throws IOException
   */
  private void deleteExtraSegments(Map<String, Map<Long, Long>> overallRequirements) throws IOException {
    // delete extra store segments
    Iterator<Map.Entry<String, ReserveFileMap>> iter = storeReserveFiles.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<String, ReserveFileMap> entry = iter.next();
      String storeId = entry.getKey();
      File storeReserveDir = new File(reserveDir, STORE_DIR_PREFIX + storeId);
      if (!overallRequirements.containsKey(storeId)) {
        // remove store id from in-mem store reserve map
        iter.remove();
        Utils.deleteFileOrDirectory(storeReserveDir);
      } else {
        ReserveFileMap sizeToFilesMap = storeReserveFiles.get(storeId);
        Map<Long, Long> storeRequirement = overallRequirements.get(storeId);
        for (Long sizeInBytes : sizeToFilesMap.getFileSizeSet()) {
          Long segmentsNeeded = storeRequirement.get(sizeInBytes);
          if (segmentsNeeded == null || segmentsNeeded == 0) {
            // 1. empty file queue to update in-mem store reserve map
            while (sizeToFilesMap.getCount(sizeInBytes) > 0) {
              sizeToFilesMap.remove(sizeInBytes);
            }
            // 2. delete directory of store segments with certain size
            File dirToDelete = new File(storeReserveDir, generateFileSizeDirName(sizeInBytes));
            Utils.deleteFileOrDirectory(dirToDelete);
          } else {
            while (sizeToFilesMap.getCount(sizeInBytes) > segmentsNeeded) {
              File fileToDelete = sizeToFilesMap.remove(sizeInBytes);
              if (fileToDelete != null && !fileToDelete.delete()) {
                throw new IOException("Could not delete the following reserve file: " + fileToDelete.getAbsolutePath());
              }
            }
          }
        }
      }
    }
    // delete extra swap segments
    Map<Long, Long> requiredSwapSegments = overallRequirements.get(SWAP_DIR_NAME);
    for (long sizeInBytes : swapReserveFiles.getFileSizeSet()) {
      Long segmentsNeeded = requiredSwapSegments.get(sizeInBytes);
      if (segmentsNeeded == null || segmentsNeeded == 0) {
        // 1. empty file queue to update in-mem swap reserve map
        while (swapReserveFiles.getCount(sizeInBytes) > 0) {
          swapReserveFiles.remove(sizeInBytes);
        }
        // 2. delete directory of swap segments with certain size
        File dirToDelete = new File(swapReserveDir, generateFileSizeDirName(sizeInBytes));
        Utils.deleteFileOrDirectory(dirToDelete);
      } else {
        while (swapReserveFiles.getCount(sizeInBytes) > segmentsNeeded) {
          File fileToDelete = swapReserveFiles.remove(sizeInBytes);
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
   * @param isInitialize whether the addition is called during initialization.
   * @throws IOException
   */
  void addRequiredSegments(Map<String, Map<Long, Long>> overallRequirements, boolean isInitialize) throws IOException {
    if (enablePooling) {
      for (Map.Entry<String, Map<Long, Long>> storeOrSwapRequirement : overallRequirements.entrySet()) {
        String storeOrSwapDirName = storeOrSwapRequirement.getKey();
        Map<Long, Long> requirementBySize = storeOrSwapRequirement.getValue();

        if (storeOrSwapDirName.equals(SWAP_DIR_NAME)) {
          // add required segments for swap directory
          for (Map.Entry<Long, Long> sizeAndSegmentsNeeded : requirementBySize.entrySet()) {
            long sizeInBytes = sizeAndSegmentsNeeded.getKey();
            long segmentsNeeded = sizeAndSegmentsNeeded.getValue();
            if (isInitialize) {
              // if this method is called during server start up, there is no race condition when calling getCount() from swapReserveFiles
              // and we still need to check swapReserveFiles.getCount() because there might be some inventoried segments
              while (swapReserveFiles.getCount(sizeInBytes) < segmentsNeeded) {
                swapReserveFiles.add(sizeInBytes, createReserveFile(sizeInBytes, swapReserveDir));
              }
            } else {
              // if this method is called when adding new store, swapReserveFiles.getCount() is not reliable because other
              // threads may concurrently check in and check out swap segments
              for (int i = 0; i < segmentsNeeded; ++i) {
                try {
                  swapReserveFiles.add(sizeInBytes, createReserveFile(sizeInBytes, swapReserveDir));
                } catch (IOException e) {
                  // if fail to create file, decrease number of swap segment with this size in swapSegmentNumBySize map
                  // we don't have to subtract # of swapSegmentsUsed because this exceptions happened when adding new store
                  // and there shouldn't be any swap segment in use.
                  swapSegmentNumBySize.put(sizeInBytes, swapSegmentNumBySize.get(sizeInBytes) - (segmentsNeeded - i));
                  throw e;
                }
              }
            }
          }
        } else {
          // add required segments for each store reserve directory
          File storeReserveDir = new File(reserveDir, STORE_DIR_PREFIX + storeOrSwapDirName);
          // prepare the directory if this is new store
          prepareDirectory(storeReserveDir);
          ReserveFileMap sizeToFilesMap = storeReserveFiles.getOrDefault(storeOrSwapDirName, new ReserveFileMap());
          for (Map.Entry<Long, Long> sizeAndSegmentsNeeded : requirementBySize.entrySet()) {
            long sizeInBytes = sizeAndSegmentsNeeded.getKey();
            long segmentsNeeded = sizeAndSegmentsNeeded.getValue();
            while (sizeToFilesMap.getCount(sizeInBytes) < segmentsNeeded) {
              sizeToFilesMap.add(sizeInBytes, createReserveFile(sizeInBytes, storeReserveDir));
            }
          }
          storeReserveFiles.put(storeOrSwapDirName, sizeToFilesMap);
        }
      }
    } else {
      logger.info("Pooling is turned off. No segments will be generated.");
    }
  }

  /**
   * @return store reserve files map in this DiskSpaceAllocator
   */
  Map<String, ReserveFileMap> getStoreReserveFileMap() {
    return storeReserveFiles;
  }

  /**
   * @return swap reserve files map in this DiskSpaceAllocator
   */
  ReserveFileMap getSwapReserveFileMap() {
    return swapReserveFiles;
  }

  /**
   * @return return the map kept in this DiskSpaceAllocator to record total number of swap segments by size. Here, total
   * number of swap segments include both segments in use and segments available in reserve pool.
   */
  Map<Long, Long> getSwapSegmentBySizeMap() {
    return swapSegmentNumBySize;
  }

  /**
   * Create and preallocate (if supported) a reserve file of the specified size.
   * @param sizeInBytes the size to preallocate for the reserve file.
   * @return the created file.
   * @throws IOException if the file could not be created, or if an error occurred during the fallocate call.
   */
  private File createReserveFile(long sizeInBytes, File dir) throws IOException {
    File fileSizeDir = prepareDirectory(new File(dir, FILE_SIZE_DIR_PREFIX + sizeInBytes));
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
   * Parse store id from a possible store reserve directory name.
   * @param storeDirName the name of a possible store reserve directory
   * @return the parsed store id, or {@code null} if the directory name did not start with the correct prefix.
   */
  private static String getStoreIdForDirName(String storeDirName) {
    return storeDirName.startsWith(STORE_DIR_PREFIX) ? storeDirName.substring(STORE_DIR_PREFIX.length()) : null;
  }

  /**
   * This is a thread safe data structure that is used to keep track of the files in the reserve pool.
   */
  static class ReserveFileMap {
    private final ConcurrentMap<Long, Queue<File>> internalMap = new ConcurrentHashMap<>();
    private final ReentrantLock removeLock = new ReentrantLock();

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
      removeLock.lock();
      Queue<File> reserveFilesForSize = internalMap.get(sizeInBytes);
      if (reserveFilesForSize != null && reserveFilesForSize.size() != 0) {
        reserveFile = reserveFilesForSize.poll();
      }
      removeLock.unlock();
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
