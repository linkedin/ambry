package com.github.ambry;

import com.github.ambry.network.ResponseInfo;
import com.sun.org.apache.xpath.internal.operations.Bool;


public class SegmentOperationTracker {
  private String fileName;
  private String partitionName;
  private boolean isFileHydrationComplete = false;

  private long currentOffset;

  public SegmentOperationTracker(String fileName, String partitionName){
    this.fileName = fileName;
    this.partitionName = partitionName;

  }

  public String getFileName(){
    return fileName;
  }

  public void setResponse(){}

  public void onResponse(ResponseInfo responseInfo){
    responseInfo.getResponse();

  }
  public String getPartitionName(){
    return partitionName;
  }

  public void setFileHydrationComplete(){
    isFileHydrationComplete = true;
  }

  public boolean isFileHydrationComplete(){
    return isFileHydrationComplete;
  }

  public void writeToFile(){

  }




}
