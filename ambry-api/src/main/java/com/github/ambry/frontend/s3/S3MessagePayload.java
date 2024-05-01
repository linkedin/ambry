/*
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
 *
 */
package com.github.ambry.frontend.s3;

import java.util.List;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;


/**
 * This class contains the payload for S3 requests rendered in XML.
 */
public class S3MessagePayload {
  public static class CompleteMultipartUpload {
    @JacksonXmlProperty(localName = "Part")
    @JacksonXmlElementWrapper(useWrapping = false)
    private Part[] part;

    private CompleteMultipartUpload() {
    }

    public CompleteMultipartUpload(Part[] part) {
      this.part = part;
    }

    public Part[] getPart() {
      return part;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      for (Part part : part) {
        sb.append(part).append(",");
      }
      return sb.toString();
    }
  }

  public static class CompleteMultipartUploadResult {
    @JacksonXmlProperty(localName = "Bucket")
    private String bucket;
    @JacksonXmlProperty(localName = "Key")
    private String key;
    @JacksonXmlProperty(localName = "Location")
    private String location;
    @JacksonXmlProperty(localName = "ETag")
    private String eTag;

    private CompleteMultipartUploadResult() {
    }

    public CompleteMultipartUploadResult(String bucket, String key, String location, String eTag) {
      this.bucket = bucket;
      this.key = key;
      this.location = location;
      this.eTag = eTag;
    }

    public String getBucket() {
      return bucket;
    }

    public String getKey() {
      return key;
    }

    public String getLocation() {
      return location;
    }

    public String geteTag() {
      return eTag;
    }

    @Override
    public String toString() {
      return "Bucket=" + bucket + ", " + "Key=" + key + ", " + "Location=" + location + ", " + "ETag=" + eTag;
    }
  }

  public static class Part {
    @JacksonXmlProperty(localName = "PartNumber")
    private String partNumber;
    @JacksonXmlProperty(localName = "ETag")
    private String eTag;

    private Part() {
    }

    public Part(String partNumber, String eTag) {
      this.partNumber = partNumber;
      this.eTag = eTag;
    }

    public String geteTag() {
      return eTag;
    }

    public int getPartNumber() throws NumberFormatException {
      return Integer.parseInt(partNumber);
    }

    @Override
    public String toString() {
      return "Part number = " + partNumber + ", eTag = " + eTag;
    }
  }

  public static class InitiateMultipartUploadResult {
    @JacksonXmlProperty(localName = "Bucket")
    private String bucket;
    @JacksonXmlProperty(localName = "Key")
    private String key;
    @JacksonXmlProperty(localName = "UploadId")
    private String uploadId;

    private InitiateMultipartUploadResult() {
    }

    public String getBucket() {
      return bucket;
    }

    public String getKey() {
      return key;
    }

    public String getUploadId() {
      return uploadId;
    }

    public InitiateMultipartUploadResult(String bucket, String key, String uploadId) {
      this.bucket = bucket;
      this.key = key;
      this.uploadId = uploadId;
    }

    @Override
    public String toString() {
      return "Bucket=" + bucket + ", Key=" + key + ", UploadId=" + uploadId;
    }
  }

  public static class ListBucketResult {
    @JacksonXmlProperty(localName = "Name")
    private String name;
    @JacksonXmlProperty(localName = "Prefix")
    private String prefix;
    @JacksonXmlProperty(localName = "MaxKeys")
    private int maxKeys;
    @JacksonXmlProperty(localName = "KeyCount")
    private int keyCount;
    @JacksonXmlProperty(localName = "Delimiter")
    private String delimiter;
    @JacksonXmlProperty(localName = "Contents")
    @JacksonXmlElementWrapper(useWrapping = false)
    private List<Contents> contents;
    @JacksonXmlProperty(localName = "EncodingType")
    private String encodingType;

    private ListBucketResult() {

    }

    public ListBucketResult(String name, String prefix, int maxKeys, int keyCount, String delimiter,
        List<Contents> contents, String encodingType) {
      this.name = name;
      this.prefix = prefix;
      this.maxKeys = maxKeys;
      this.keyCount = keyCount;
      this.delimiter = delimiter;
      this.contents = contents;
      this.encodingType = encodingType;
    }

    public String getPrefix() {
      return prefix;
    }

    public int getMaxKeys() {
      return maxKeys;
    }

    public String getDelimiter() {
      return delimiter;
    }

    public List<Contents> getContents() {
      return contents;
    }

    public String getEncodingType() {
      return encodingType;
    }

    public int getKeyCount() {
      return keyCount;
    }

    @Override
    public String toString() {
      return "Name=" + name + ", Prefix=" + prefix + ", MaxKeys=" + maxKeys + ", KeyCount=" + keyCount + ", Delimiter="
          + delimiter + ", Contents=" + contents + ", Encoding type=" + encodingType;
    }
  }

  public static class Contents {
    @JacksonXmlProperty(localName = "Key")
    private String key;
    @JacksonXmlProperty(localName = "LastModified")
    private String lastModified;
    @JacksonXmlProperty(localName = "Size")
    private long size;

    private Contents() {
    }

    public Contents(String key, String lastModified, long size) {
      this.key = key;
      this.lastModified = lastModified;
      this.size = size;
    }

    public String getKey() {
      return key;
    }

    public long getSize() { return size; }

    @Override
    public String toString() {
      return "Key=" + key + ", " + "LastModified=" + lastModified + ", " + "Size=" + size;
    }
  }

  public static class ListPartsResult {

    @JacksonXmlProperty(localName = "Bucket")
    private String bucket;
    @JacksonXmlProperty(localName = "Key")
    private String key;
    @JacksonXmlProperty(localName = "UploadId")
    private String uploadId;

    public ListPartsResult() {

    }

    public ListPartsResult(String bucket, String key, String uploadId) {
      this.bucket = bucket;
      this.key = key;
      this.uploadId = uploadId;
    }

    @Override
    public String toString() {
      return "Bucket=" + bucket + ", Key=" + key + ", UploadId=" + uploadId;
    }
  }
}
