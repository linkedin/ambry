package com.github.ambry.protocol;

import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class LogInfo {
  private String fileName;
  private long fileSizeInBytes;
  List<FileInfo> listOfIndexFiles;
  List<FileInfo> listOfBloomFilters;

  private static final int FileName_Field_Size_In_Bytes = 4;
  private static final int FileSize_Field_Size_In_Bytes = 8;

  private static final int ListSize_In_Bytes = 4;
  public LogInfo(String fileName, long fileSizeInBytes, List<FileInfo> listOfIndexFiles, List<FileInfo> listOfBloomFilters) {
    this.fileName = fileName;
    this.fileSizeInBytes = fileSizeInBytes;
    this.listOfIndexFiles = listOfIndexFiles;
    this.listOfBloomFilters = listOfBloomFilters;
  }

  public String getFileName() {
    return fileName;
  }

  public long getFileSizeInBytes() {
    return fileSizeInBytes;
  }

  public List<FileInfo> getListOfBloomFilters() {
    return listOfBloomFilters;
  }

  public List<FileInfo> getListOfIndexFiles() {
    return listOfIndexFiles;
  }

  public long sizeInBytes() {
    long size = FileName_Field_Size_In_Bytes + fileName.length() + FileSize_Field_Size_In_Bytes + ListSize_In_Bytes;
    for (FileInfo fileInfo : listOfIndexFiles) {
      size += fileInfo.sizeInBytes();
    }
    size += ListSize_In_Bytes;
    for (FileInfo fileInfo : listOfBloomFilters) {
      size += fileInfo.sizeInBytes();
    }
    return size;
  }

  public static LogInfo readFrom(DataInputStream stream) throws IOException {
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

  public void writeTo(ByteBuf buf){
    Utils.serializeString(buf, fileName, Charset.defaultCharset());
    buf.writeLong(fileSizeInBytes);
    buf.writeInt(listOfIndexFiles.size());
    for(FileInfo fileInfo : listOfIndexFiles){
      fileInfo.writeTo(buf);
    }
    buf.writeInt(listOfBloomFilters.size());
    for(FileInfo fileInfo: listOfBloomFilters){
      fileInfo.writeTo(buf);
    }
  }

  public String toString(){
    StringBuilder sb = new StringBuilder();
    sb.append("LogInfo[");
    sb.append("FileName=").append(fileName).append(", FileSizeInBytes=").append(fileSizeInBytes).append(",");
    for(FileInfo fileInfo : listOfIndexFiles) {
      sb.append(fileInfo.toString());
    }
    for(FileInfo fileInfo: listOfBloomFilters){
      sb.append(fileInfo.toString());
    }
    sb.append("]");
    return sb.toString();
  }

}
