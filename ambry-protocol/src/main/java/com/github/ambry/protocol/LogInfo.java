/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.protocol;

import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;


/**
 * Contains the fileName, fileSizeInBytes, indexFiles and bloomFilters for a local partition. This is used
 * by filecopy metadata request.
 */
public class LogInfo {
  // TODO: Replace these fields with FileInfo
  // private FileInfo fileInfo;

  /**
   * Contains the information about a file.
   */
  private final String fileName;

  /**
   * The size of the file in bytes.
   */
  private final long fileSizeInBytes;

  /**
   * The list of index files for the log.
   */
  private final List<FileInfo> indexFiles;

  /**
   * The list of bloom filter files for the log.
   */
  private final List<FileInfo> bloomFilters;

  // TODO: Add isSealed prop
  // private final boolean isSealed;

  /**
   * The size of the file name field in bytes.
   */
  private static final int FileName_Field_Size_In_Bytes = 4;

  /**
   * The size of the file size field in bytes.
   */
  private static final int FileSize_Field_Size_In_Bytes = 8;

  /**
   * The size of the list field in bytes.
   */
  private static final int ListSize_In_Bytes = 4;

  /**
   * Constructor to create a LogInfo object.
   * @param fileName The name of the file.
   * @param fileSizeInBytes The size of the file in bytes.
   * @param indexFiles The list of index files for the log.
   * @param bloomFilters The list of bloom filter files for the log.
   */
  public LogInfo(
      String fileName,
      long fileSizeInBytes,
      @Nonnull List<FileInfo> indexFiles,
      @Nonnull List<FileInfo> bloomFilters) {
    Objects.requireNonNull(indexFiles, "indexFiles must not be null");
    Objects.requireNonNull(bloomFilters, "bloomFilters must not be null");

    this.fileName = fileName;
    this.fileSizeInBytes = fileSizeInBytes;
    this.indexFiles = indexFiles;
    this.bloomFilters = bloomFilters;
  }

  /**
   * Get the file name.
   * @return The file name.
   */
  public String getFileName() {
    return fileName;
  }

  /**
   * Get the file size in bytes.
   * @return The file size in bytes.
   */
  public long getFileSizeInBytes() {
    return fileSizeInBytes;
  }

  /**
   * Get the list of index files in a read-only format.
   * @return The list of index files.
   */
  public List<FileInfo> getBloomFilters() {
    return Collections.unmodifiableList(bloomFilters);
  }

  /**
   * Get the list of bloom filter files in a read-only format.
   * @return The list of bloom filter files.
   */
  public List<FileInfo> getIndexFiles() {
    return Collections.unmodifiableList(indexFiles);
  }

  /**
   * Get the size of the LogInfo object in bytes.
   * @return The size of the LogInfo object in bytes.
   */
  public long sizeInBytes() {
    long size = FileName_Field_Size_In_Bytes + fileName.length() + FileSize_Field_Size_In_Bytes + ListSize_In_Bytes;
    for (FileInfo fileInfo : indexFiles) {
      size += fileInfo.sizeInBytes();
    }
    size += ListSize_In_Bytes;
    for (FileInfo fileInfo : bloomFilters) {
      size += fileInfo.sizeInBytes();
    }
    return size;
  }

  /**
   * Read the LogInfo object from the input stream.
   * @param stream The input stream.
   * @return The LogInfo object.
   */
  public static LogInfo readFrom(@Nonnull DataInputStream stream) throws IOException {
    Objects.requireNonNull(stream, "stream should not be null");

    String fileName = Utils.readIntString(stream );
    long fileSize = stream.readLong();
    List<FileInfo> listOfIndexFiles = new ArrayList<>();
    List<FileInfo> listOfBloomFilters = new ArrayList<>();

    int indexFilesCount = stream.readInt();
    for (int i = 0; i < indexFilesCount; i++) {
      listOfIndexFiles.add(FileInfo.readFrom(stream));
    }

    int bloomFiltersCount = stream.readInt();
    for(int i= 0;i< bloomFiltersCount; i++){
      listOfBloomFilters.add(FileInfo.readFrom(stream));
    }
    return new LogInfo(fileName, fileSize, listOfIndexFiles, listOfBloomFilters);
  }

  /**
   * Write the LogInfo object to the output stream.
   * @param buf The output stream.
   */
  public void writeTo(@Nonnull ByteBuf buf){
    Objects.requireNonNull(buf, "buf should not be null");

    Utils.serializeString(buf, fileName, Charset.defaultCharset());
    buf.writeLong(fileSizeInBytes);
    buf.writeInt(indexFiles.size());
    for(FileInfo fileInfo : indexFiles){
      fileInfo.writeTo(buf);
    }
    buf.writeInt(bloomFilters.size());
    for(FileInfo fileInfo: bloomFilters){
      fileInfo.writeTo(buf);
    }
  }

  public String toString(){
    StringBuilder sb = new StringBuilder();
    sb.append("LogInfo[");
    sb.append("FileName=").append(fileName).append(", FileSizeInBytes=").append(fileSizeInBytes);

    if(!indexFiles.isEmpty()) {
      sb.append(", IndexFiles=[");
      for (FileInfo fileInfo : indexFiles) {
        sb.append(fileInfo.toString());
      }
      sb.append("]");
      if(!bloomFilters.isEmpty()) {
        sb.append(", ");
      }
    }
    if(!bloomFilters.isEmpty()){
      sb.append(" BloomFilters=[");
      for(FileInfo fileInfo: bloomFilters){
        sb.append(fileInfo.toString());
      }
      sb.append("]");
    }
    sb.append("]");
    return sb.toString();
  }
}