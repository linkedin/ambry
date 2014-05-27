package com.github.ambry.store;

/**
 * Represents a portion of a file. Provides the start and end offset of a file
 */
public class FileSpan {
  private long fileStartOffset;
  private long fileEndOffset;

  public FileSpan(long fileStartOffset, long fileEndOffset) {
    if (fileEndOffset < fileStartOffset) {
      throw new IllegalArgumentException("File span needs to be positive");
    }
    this.fileStartOffset = fileStartOffset;
    this.fileEndOffset = fileEndOffset;
  }

  public long getStartOffset() {
    return fileStartOffset;
  }

  public long getEndOffset() {
    return fileEndOffset;
  }
}
