package com.github.ambry.protocol;

import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.Charset;


public class FileInfo{
  private String fileName;
  private long fileSizeInBytes;

  private static final int FileName_Field_Size_In_Bytes = 4;

  private static final int FileSize_Field_Size_In_Bytes = 8;


  public FileInfo(String fileName, long fileSize) {
    this.fileName = fileName;
    this.fileSizeInBytes = fileSize;
  }

  public long sizeInBytes() {
    return FileName_Field_Size_In_Bytes + fileName.length() + FileSize_Field_Size_In_Bytes;
  }
  public static FileInfo readFrom(DataInputStream stream) throws IOException {
    String fileName = Utils.readIntString(stream);
    long fileSize = stream.readLong();
    return new FileInfo(fileName, fileSize);
  }
  public void writeTo(ByteBuf buf) {
    Utils.serializeString(buf, fileName, Charset.defaultCharset());
    buf.writeLong(fileSizeInBytes);
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("FileInfo[").append("FileName=").append(fileName).append(", FileSizeInBytes=").append(fileSizeInBytes)
        .append("]");
    return sb.toString();
  }

  public String getFileName() {
    return fileName;
  }

  public long getFileSizeInBytes() {
    return fileSizeInBytes;
  }
}