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

/**
 * Represents metadata about a file in the Ambry storage system.
 * Contains basic file information like name and size.
 * Used for tracking file details during copy operations and metadata management.
 */
public class FileInfo {
  // Name of the file, including any path components
  private String fileName;

  // Size of the file in bytes
  // Marked final as size shouldn't change after creation
  private final long fileSize;

  /**
   * Creates a new FileInfo instance.
   *
   * @param fileName Name or path of the file
   * @param fileSize Size of the file in bytes
   */
  public FileInfo(String fileName, Long fileSize) {
    this.fileName = fileName;
    this.fileSize = fileSize;
  }

  /**
   * Gets the name of the file.
   *
   * @return The file name or path
   */
  public String getFileName() {
    return fileName;
  }

  /**
   * Gets the size of the file in bytes.
   *
   * @return The file size
   */
  public Long getFileSize() {
    return fileSize;
  }

  /**
   * Returns a string representation of the FileInfo object.
   * Useful for logging and debugging.
   *
   * @return String containing file name and size
   */
  @Override
  public String toString() {
    return "FileInfo{" +
        "fileName='" + fileName + '\'' +
        ", fileSize=" + fileSize +
        '}';
  }
}
