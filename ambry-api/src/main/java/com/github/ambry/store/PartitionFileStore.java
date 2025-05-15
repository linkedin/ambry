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

import java.io.IOException;
import java.util.List;


/**
 * Represents a store that contains log segments.
 * Provides methods to get a ByteBuffer for a file chunk and to put a chunk to a file.
 * Also provides methods to persist and read metadata to/from a partition.
 */
public interface PartitionFileStore {
  /**
   * Get a ByteBuffer for a file chunk.
   * @param fileName the name of the requested file. This could be a log segment, index segment or bloom filter.
   * @param offset the start offset of the requested chunk.
   * @param size the size of the requested chunk in bytes.
   * @param isChunked whether the request is chunked or not.
   * @return a StoreFileChunk representing the chunk stream of the file requested.
   * @throws StoreException
   */
  StoreFileChunk readStoreFileChunkFromDisk(String fileName, long offset, long size, boolean isChunked)
      throws StoreException, IOException;

  /**
   * Put a chunk to a file.
   * @param outputFilePath the path of the file to put the chunk to.
   * @param storeFileChunk the StoreFileChunk object representing chunk stream of the file to put.
   * @throws IOException
   */
  void writeStoreFileChunkToDisk(String outputFilePath, StoreFileChunk storeFileChunk) throws IOException;

  /**
   * Moves all regular files from the given source directory to the destination directory. This throws an exception in
   * case any file already exists with the same name in the destination.
   *
   * @param srcDirPath   the path to the source directory
   * @param destDirPath  the path to the destination directory
   * @throws IOException if an I/O error occurs during the move operation
   */
  void moveAllRegularFiles(String srcDirPath, String destDirPath) throws IOException;

  void cleanUpDirectory(String srcPath);
  /**
   * move a pre-allocated file in the target path from diskSpaceAllocator's pre-allocated files for the store
   *
   * @param targetPath the path of the directory where the files need to be created
   * @param storeId    storeId of the store for which file is requested for
   * @throws IOException if an I/O error occurs during the move operation
   */
  void allocateFile(String targetPath, String storeId) throws IOException;

  /**
   * move an allocated file back to diskSpaceAllocator's pre-allocated files store
   * @param targetPath the path to the directory
   * @param storeId    storeId of the store for which file is requested for
   * @throws IOException if an I/O error occurs during the operation
   */
  void cleanFile(String targetPath, String storeId) throws IOException;

  /**
   * @return size of allocated segment in bytes
   */
  long getSegmentCapacity();
}
