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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class DiskSpaceAllocator {
  private static final Logger logger = LoggerFactory.getLogger(DiskSpaceAllocator.class);

  private static final int REQUIRED_SWAP_SEGMENTS_PER_SIZE = 1;
  private static final String RESERVE_FILE_PREFIX = "reserve_";
  private static final FileFilter RESERVE_FILE_FILTER =
      pathname -> pathname.isFile() && pathname.getName().startsWith(RESERVE_FILE_PREFIX);
  private static final FileFilter FILE_SIZE_DIR_FILTER = pathname -> {
    try {
      return pathname.isDirectory() && Long.parseLong(pathname.getName()) >= 0;
    } catch (NumberFormatException e) {
      return false;
    }
  };

  private final ReserveFileMap reserveFiles;
  private final File reserveDir;
  private volatile boolean poolInitialized = false;

  /**
   * @param reserveDir the directory where reserve files will reside. If this directory does not exist yet, it will
   *                   be created.
   * @throws StoreException
   */
  DiskSpaceAllocator(File reserveDir) throws StoreException {
    this.reserveDir = reserveDir;
    try {
      prepareDirectory(reserveDir);
      reserveFiles = inventoryExistingReserveFiles();
    } catch (Exception e) {
      throw new StoreException("Could not load from reserve directory.", e, StoreErrorCodes.Initialization_Error);
    }
  }

  /**
   * @param requirementsList a list of {@link DiskSpaceRequirements}s objects that describe the number and segment size
   *                         needed by each store.
   * @throws StoreException
   */
  void initializePool(Collection<DiskSpaceRequirements> requirementsList) throws StoreException {
    reserveFiles.rwLock.writeLock().lock();
    try {
      try {
        Map<Long, Long> overallRequirements = getOverallRequirements(requirementsList);
        deleteUnneededSegments(overallRequirements);
        addRequiredSegments(overallRequirements);
        // TODO fill the disk with additional swap segments
        poolInitialized = true;
      } catch (Exception e) {
        throw new StoreException("Exception while initializing DiskSpaceAllocator pool", e,
            StoreErrorCodes.Initialization_Error);
      }
    } finally {
      reserveFiles.rwLock.writeLock().unlock();
    }
  }

  /**
   * Allocate a file, that is, take a file matching the requested size from the reserve directory and move it to the
   * provided file path
   * @param destinationFile the file path to move the allocated file to.
   * @param sizeInBytes the size in bytes of the requested file.
   * @throws IOException
   */
  void allocate(File destinationFile, long sizeInBytes) throws IOException {
    if (!poolInitialized) {
      logger.debug("Allocating segment before pool is fully initialized to " + destinationFile.getAbsolutePath());
    }
    File reserveFile = reserveFiles.remove(sizeInBytes);
    if (reserveFile == null) {
      logger.info("Segment of size {} not found in pool; attempting to create a new preallocated file", sizeInBytes);
      reserveFile = createReserveFile(sizeInBytes);
    }
    if (destinationFile.exists() || !reserveFile.renameTo(destinationFile)) {
      reserveFiles.add(sizeInBytes, reserveFile);
      throw new IOException("Error while moving reserve file: " + reserveFile.getAbsolutePath() + " to location: "
          + destinationFile.getAbsolutePath());
    }
  }

  /**
   * Return a file to the pool. The user must keep track of the size of the file allocated.
   * @param fileToReturn the file to return to the pool.
   * @param sizeInBytes the size of the file to return.
   * @throws IOException if the file to return does not exist or cannot be cleaned or recreated correctly.
   */
  void free(File fileToReturn, long sizeInBytes) throws IOException {
    if (!poolInitialized) {
      logger.debug("Freeing segment before pool is fully initialized");
    }
    // For now, we delete the file and create a new one. Newer linux kernel versions support
    // additional fallocate flags, which will be useful for cleaning up returned files.
    if (!fileToReturn.delete()) {
      throw new IOException("Error while cleaning up returned file: " + fileToReturn.getAbsolutePath());
    }
    fileToReturn = createReserveFile(sizeInBytes);
    reserveFiles.add(sizeInBytes, fileToReturn);
  }

  /**
   * Inventory existing reserve directories and generate a {@link ReserveFileMap}.
   * @return a populated {@link ReserveFileMap}
   */
  private ReserveFileMap inventoryExistingReserveFiles() {
    ReserveFileMap reserveFiles = new ReserveFileMap();
    File[] fileSizeDirs = reserveDir.listFiles(FILE_SIZE_DIR_FILTER);
    if (fileSizeDirs != null) {
      for (File fileSizeDir : fileSizeDirs) {
        long sizeInBytes = Long.parseLong(fileSizeDir.getName());
        File[] reserveFilesForSize = fileSizeDir.listFiles(RESERVE_FILE_FILTER);
        if (reserveFilesForSize != null) {
          for (File reserveFile : reserveFilesForSize) {
            reserveFiles.add(sizeInBytes, reserveFile);
          }
        }
      }
    }
    return reserveFiles;
  }

  /**
   * Delete the currently-present reserve files that are not required by {@code overallRequirements}.
   * @param overallRequirements a map between segment sizes in bytes and the number of segments needed for that size.
   * @throws IOException
   */
  private void deleteUnneededSegments(Map<Long, Long> overallRequirements) throws IOException {
    for (long sizeInBytes : reserveFiles.getFileSizeSet()) {
      Long segmentsNeeded = overallRequirements.get(sizeInBytes);
      if (segmentsNeeded == null || segmentsNeeded == 0) {
        File dirToDelete = new File(reserveDir, Long.toString(sizeInBytes));
        if (!Utils.deleteDirectory(dirToDelete)) {
          throw new IOException("Could not delete the following reserve directory: " + dirToDelete.getAbsolutePath());
        }
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
    File fileSizeDir = prepareDirectory(new File(reserveDir, Long.toString(sizeInBytes)));
    File reserveFile;
    do {
      reserveFile = new File(fileSizeDir, generateFilename());
    } while (!reserveFile.createNewFile());
    Utils.preAllocateFileIfNeeded(reserveFile, sizeInBytes);
    return reserveFile;
  }

  /**
   * @param requirementsList the list of {@link DiskSpaceRequirements} objects to be accumulated
   * @return a {@link Map} from segment sizes to the number of reserve segments needed at that size.
   */
  private static Map<Long, Long> getOverallRequirements(Collection<DiskSpaceRequirements> requirementsList) {
    Map<Long, Long> overallRequirements = new HashMap<>();
    Set<Long> sizesWithSwapAdded = new HashSet<>();
    for (DiskSpaceRequirements requirements : requirementsList) {
      long segmentSizeInBytes = requirements.getSegmentSizeInBytes();
      Long oldSegmentsNeeded = overallRequirements.get(segmentSizeInBytes);
      if (oldSegmentsNeeded == null) {
        oldSegmentsNeeded = 0L;
      }
      long totalSegmentsNeeded = oldSegmentsNeeded + requirements.getSegmentsNeeded();
      if (requirements.isSwapRequired() && !sizesWithSwapAdded.contains(segmentSizeInBytes)) {
        totalSegmentsNeeded += REQUIRED_SWAP_SEGMENTS_PER_SIZE;
        sizesWithSwapAdded.add(segmentSizeInBytes);
      }
      overallRequirements.put(segmentSizeInBytes, totalSegmentsNeeded);
    }
    return Collections.unmodifiableMap(overallRequirements);
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
      throw new IOException("Directory could not be created or is not a directory: " + dir.getAbsolutePath());
    }
    return dir;
  }

  /**
   * @return a filename for a reserve file. This is reserve_ followed by a random UUID.
   */
  private static String generateFilename() {
    return RESERVE_FILE_PREFIX + UUID.randomUUID();
  }

  private static class ReserveFileMap {
    private final Map<Long, Queue<File>> internalMap = new HashMap<>();
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    /**
     * Add a file of the specified file size to the reserve file map.
     * @param sizeInBytes the size of the reserve file
     * @param reserveFile the reserve file to add.
     */
    void add(long sizeInBytes, File reserveFile) {
      rwLock.writeLock().lock();
      try {
        Queue<File> reserveFilesForSize = internalMap.get(sizeInBytes);
        if (reserveFilesForSize == null) {
          reserveFilesForSize = new LinkedList<>();
          internalMap.put(sizeInBytes, reserveFilesForSize);
        }
        reserveFilesForSize.add(reserveFile);
      } finally {
        rwLock.writeLock().unlock();
      }
    }

    /**
     * Remove and return a file of the specified size from the reserve file map.
     * @param sizeInBytes the size of the file to look for.
     * @return the {@link File}, or {@code null} if no file exists in the map for the specified size.
     */
    File remove(long sizeInBytes) {
      rwLock.writeLock().lock();
      try {
        File reserveFile = null;
        Queue<File> reserveFilesForSize = internalMap.get(sizeInBytes);
        if (reserveFilesForSize != null) {
          if (reserveFilesForSize.size() != 0) {
            reserveFile = reserveFilesForSize.remove();
          }
          if (reserveFilesForSize.size() == 0) {
            internalMap.remove(sizeInBytes);
          }
        }
        return reserveFile;
      } finally {
        rwLock.writeLock().unlock();
      }
    }

    /**
     * @param sizeInBytes the size of files of interest
     * @return the number of files in the map of size {@code sizeInBytes}
     */
    int getCount(long sizeInBytes) {
      rwLock.readLock().lock();
      try {
        Queue<File> reserveFilesForSize = internalMap.get(sizeInBytes);
        return reserveFilesForSize != null ? reserveFilesForSize.size() : 0;
      } finally {
        rwLock.readLock().unlock();
      }
    }

    /**
     * @return the set of file sizes present in the map.
     */
    Set<Long> getFileSizeSet() {
      rwLock.readLock().lock();
      try {
        return internalMap.keySet();
      } finally {
        rwLock.readLock().unlock();
      }
    }
  }
}
