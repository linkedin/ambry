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
import javax.annotation.Nonnull;


/**
 * Contains the fileName, fileSizeInBytes, indexFiles and bloomFilters for a local partition. This is used
 * by filecopy metadata request.
 */
public class LogInfo {
  // TODO: Replace these fields with FileInfo
  // private FileInfo fileInfo;
  private final String fileName;
  private final long fileSizeInBytes;
  private final List<FileInfo> indexFiles;
  private final List<FileInfo> bloomFilters;

  // TODO: Add isSealed prop
  // private final boolean isSealed;

  private static final int FileName_Field_Size_In_Bytes = 4;
  private static final int FileSize_Field_Size_In_Bytes = 8;
  private static final int ListSize_In_Bytes = 4;

  public LogInfo(
      @Nonnull String fileName,
      long fileSizeInBytes,
      @Nonnull List<FileInfo> indexFiles,
      @Nonnull List<FileInfo> bloomFilters) {
    this.fileName = fileName;
    this.fileSizeInBytes = fileSizeInBytes;
    this.indexFiles = indexFiles;
    this.bloomFilters = bloomFilters;
  }

  public String getFileName() {
    return fileName;
  }

  public long getFileSizeInBytes() {
    return fileSizeInBytes;
  }

  public List<FileInfo> getBloomFilters() {
    return Collections.unmodifiableList(bloomFilters);
  }

  public List<FileInfo> getIndexFiles() {
    return Collections.unmodifiableList(indexFiles);
  }

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

  public static LogInfo readFrom(@Nonnull DataInputStream stream) throws IOException {
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

  public void writeTo(@Nonnull ByteBuf buf){
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