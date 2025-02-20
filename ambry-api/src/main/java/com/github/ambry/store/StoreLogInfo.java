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

import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;


/**
 * Contains information about a log segment,
 * FileInfo info for the log segment,
 * FileInfo info for the linked index segments and
 * FileInfo info for the linked bloom filters.
 */
public class StoreLogInfo implements LogInfo {
  /**
   * FileInfo for the log segment.
   */
  private final FileInfo logSegment;

  /**
   * FileInfo for the linked index segments.
   */
  private final List<FileInfo> indexSegments;

  /**
   * FileInfo for the linked bloom filters.
   */
  private final List<FileInfo> bloomFilters;

  // TODO: Add isSealed prop
  // private final boolean isSealed;

  /**
   * The size of the file name field in bytes.
   */
  private static final int FILE_NAME_FIELD_SIZE_IN_BYTES = 4;

  /**
   * The size of the file size field in bytes.
   */
  private static final int FILE_SIZE_FIELD_SIZE_IN_BYTES = 8;

  /**
   * The size of the list field in bytes.
   */
  private static final int LIST_SIZE_IN_BYTES = 4;

  /**
   * Constructs a LogInfo with the given log segment, index segments and bloom filters.
   * @param logSegment FileInfo for the log segment.
   * @param indexSegments FileInfo for the linked index segments.
   * @param bloomFilters FileInfo for the linked bloom filters.
   */
  public StoreLogInfo(FileInfo logSegment, List<FileInfo> indexSegments, List<FileInfo> bloomFilters) {
    this.logSegment = logSegment;
    this.indexSegments = indexSegments;
    this.bloomFilters = bloomFilters;
  }

  // TODO: Add isSealed prop
  // private final boolean isSealed;

  /**
   * @return FileInfo for the log segment.
   */
  @Override
  public FileInfo getLogSegment() {
    return logSegment;
  }

  /**
   * @return FileInfo for the linked index segments.
   */
  @Override
  public List<FileInfo> getIndexSegments() {
    return Collections.unmodifiableList(indexSegments);
  }

  /**
   * @return FileInfo for the linked bloom filters.
   */
  @Override
  public List<FileInfo> getBloomFilters() {
    return Collections.unmodifiableList(bloomFilters);
  }

  /**
   * Get the size of the LogInfo object in bytes.
   * @return The size of the LogInfo object in bytes.
   */
  public long sizeInBytes() {
    long size = FILE_NAME_FIELD_SIZE_IN_BYTES + logSegment.getFileName().length() +
        FILE_SIZE_FIELD_SIZE_IN_BYTES + LIST_SIZE_IN_BYTES;
    for (FileInfo storeFileInfo : indexSegments) {
      size += storeFileInfo.sizeInBytes();
    }
    size += LIST_SIZE_IN_BYTES;
    for (FileInfo storeFileInfo : bloomFilters) {
      size += storeFileInfo.sizeInBytes();
    }
    return size;
  }

  /**
   * Read the LogInfo object from the input stream.
   * @param stream The input stream.
   * @return The LogInfo object.
   */
  public static LogInfo readFrom(DataInputStream stream) throws IOException {
    Objects.requireNonNull(stream, "stream should not be null");

    String fileName = Utils.readIntString(stream);
    long fileSize = stream.readLong();
    List<FileInfo> listOfIndexFiles = new ArrayList<>();
    List<FileInfo> listOfBloomFilters = new ArrayList<>();

    int indexFilesCount = stream.readInt();
    for (int i = 0; i < indexFilesCount; i++) {
      listOfIndexFiles.add(StoreFileInfo.readFrom(stream));
    }

    int bloomFiltersCount = stream.readInt();
    for(int i = 0; i < bloomFiltersCount; i++){
      listOfBloomFilters.add(StoreFileInfo.readFrom(stream));
    }
    return new StoreLogInfo(new StoreFileInfo(fileName, fileSize), listOfIndexFiles, listOfBloomFilters);
  }

  /**
   * Write the LogInfo object to the output stream.
   * @param buf The output stream.
   */
  public void writeTo(ByteBuf buf){
    Objects.requireNonNull(buf, "buf should not be null");

    Utils.serializeString(buf, logSegment.getFileName(), Charset.defaultCharset());
    buf.writeLong(logSegment.getFileSize());
    buf.writeInt(indexSegments.size());
    for(FileInfo storeFileInfo : indexSegments){
      storeFileInfo.writeTo(buf);
    }
    buf.writeInt(bloomFilters.size());
    for(FileInfo storeFileInfo : bloomFilters){
      storeFileInfo.writeTo(buf);
    }
  }

  @Override
  public String toString(){
    StringBuilder sb = new StringBuilder();
    sb.append(logSegment);

    if(!indexSegments.isEmpty()) {
      sb.append(", IndexFiles=[");
      for (FileInfo storeFileInfo : indexSegments) {
        sb.append(storeFileInfo.toString());
      }
      sb.append("]");
      if(!bloomFilters.isEmpty()) {
        sb.append(", ");
      }
    }
    if(!bloomFilters.isEmpty()){
      sb.append(" BloomFilters=[");
      for(FileInfo storeFileInfo : bloomFilters){
        sb.append(storeFileInfo.toString());
      }
      sb.append("]");
    }
    sb.append("]");
    return sb.toString();
  }
}
