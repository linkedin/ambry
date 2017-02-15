/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.utils.CrcOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;


/**
 * Utility class for common functions used in tests of store classes.
 */
class StoreTestUtils {

  /**
   * Creates a temporary directory whose name starts with the given {@code prefix}.
   * @param prefix the prefix of the directory name.
   * @return the directory created as a {@link File} instance.
   * @throws IOException
   */
  static File createTempDirectory(String prefix) throws IOException {
    File tempDir = Files.createTempDirectory(prefix).toFile();
    tempDir.deleteOnExit();
    return tempDir;
  }

  /**
   * Cleans up the {@code dir} and deletes it.
   * @param dir the directory to be cleaned up and deleted.
   * @param deleteDirectory if {@code true}, the directory is deleted too.
   * @throws IOException
   */
  static boolean cleanDirectory(File dir, boolean deleteDirectory) throws IOException {
    if (!dir.exists()) {
      return true;
    }
    if (!dir.isDirectory()) {
      throw new IllegalArgumentException(dir.getAbsolutePath() + " is not a directory");
    }
    File[] files = dir.listFiles();
    if (files == null) {
      throw new IOException("Could not list files in directory: " + dir.getAbsolutePath());
    }
    boolean success = true;
    for (File file : files) {
      success = file.delete() && success;
    }
    return deleteDirectory ? dir.delete() && success : success;
  }

  /**
   * Writes the index to a persistent file. Writes the data in the following format for {@link PersistentIndex#VERSION_0}
   * For {@link PersistentIndex#VERSION_1} refer {@link IndexSegment}
   *  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
   * | version | keysize | valuesize | fileendpointer |   key 1  | value 1  |  ...  |   key n   | value n   | crc      |
   * |(2 bytes)|(4 bytes)| (4 bytes) |    (8 bytes)   | (n bytes)| (n bytes)|       | (n bytes) | (n bytes) | (8 bytes)|
   *  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
   *  version         - the index format version
   *  keysize         - the size of the key in this index segment
   *  valuesize       - the size of the value in this index segment
   *  fileendpointer  - the log end pointer that pertains to the index being persisted
   *  key n / value n - the key and value entries contained in this index segment
   *  crc             - the crc of the index segment content
   *
   * @param safeEndPoint the end point (that is relevant to this segment) until which the log has been flushed.
   * @throws IOException
   * @throws StoreException
   */
  static void writeIndexSegmentToFile(IndexSegment indexSegment, Offset safeEndPoint)
      throws IOException, StoreException {
    if (safeEndPoint.compareTo(indexSegment.getStartOffset()) < 0) {
      return;
    }
    if (!safeEndPoint.equals(indexSegment.prevSafeEndPoint)) {
      if (safeEndPoint.compareTo(indexSegment.getEndOffset()) > 0) {
        throw new StoreException(
            "SafeEndOffSet " + safeEndPoint + " is greater than current end offset for current " + "index segment "
                + indexSegment.getEndOffset(), StoreErrorCodes.Illegal_Index_Operation);
      }
      File temp = new File(indexSegment.getFile().getAbsolutePath() + ".tmp");
      FileOutputStream fileStream = new FileOutputStream(temp);
      CrcOutputStream crc = new CrcOutputStream(fileStream);
      DataOutputStream writer = new DataOutputStream(crc);
      try {
        // write the current version
        writer.writeShort(PersistentIndex.VERSION_0);
        // write key, value size, file end pointer, last modified time and reset key for this index
        writer.writeInt(indexSegment.getKeySize());
        writer.writeInt(indexSegment.getValueSize());
        writer.writeLong(safeEndPoint.getOffset());

        // write the entries
        for (Map.Entry<StoreKey, IndexValue> entry : indexSegment.index.entrySet()) {
          if (entry.getValue().getOffset().getOffset() + entry.getValue().getSize() <= safeEndPoint.getOffset()) {
            writer.write(entry.getKey().toBytes());
            writer.write(entry.getValue().getBytes().array());
          }
        }
        indexSegment.prevSafeEndPoint = safeEndPoint;
        long crcValue = crc.getValue();
        writer.writeLong(crcValue);

        // flush and overwrite old file
        fileStream.getChannel().force(true);
        // swap temp file with the original file
        temp.renameTo(indexSegment.getFile());
      } catch (IOException e) {
        throw new StoreException(
            "IndexSegment : " + indexSegment.getFile().getAbsolutePath() + " IO error while persisting index to disk",
            e, StoreErrorCodes.IOError);
      } finally {
        writer.close();
      }
    }
  }
}
