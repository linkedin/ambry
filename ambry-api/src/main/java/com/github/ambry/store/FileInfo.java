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
package com.github.ambry.store;

/**
 * Represents a file saved in store.
 * Contains basic info like fileName and fileSize
 */
public class FileInfo {
  /**
   * Name of the file
   */
  private final String fileName;

  /**
   * Size of the file in bytes
   */
  private final long fileSize;

  /**
   * Constructor to create a FileInfo
   * @param fileName name of the file
   * @param fileSize file size in bytes
   */
  public FileInfo(
      String fileName,
      Long fileSize) {
    this.fileName = fileName;
    this.fileSize = fileSize;
  }

  /**
   * Get the name of the file
   */
  public String getFileName() {
    return fileName;
  }

  /**
   * Get the size of the file
   */
  public Long getFileSize() {
    return fileSize;
  }

  @Override
  public String toString() {
    return "FileInfo{" +
        "fileName='" + fileName + '\'' +
        ", fileSize=" + fileSize +
        '}';
  }
}