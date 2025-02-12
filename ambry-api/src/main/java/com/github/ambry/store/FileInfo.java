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

public class FileInfo {
  private String fileName;
  private final long fileSize;

  public FileInfo(String fileName, Long fileSize) {
    this.fileName = fileName;
    this.fileSize = fileSize;
  }
  public String getFileName() {
    return fileName;
  }

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