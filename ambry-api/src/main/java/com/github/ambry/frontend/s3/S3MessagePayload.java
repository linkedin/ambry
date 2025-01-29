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

import java.util.ArrayList;
import java.util.List;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import java.util.concurrent.ConcurrentLinkedQueue;


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

  public static abstract class AbstractListBucketResult {
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
    @JacksonXmlProperty(localName = "isTruncated")
    private Boolean isTruncated;

    private AbstractListBucketResult() {

    }

    public AbstractListBucketResult(String name, String prefix, int maxKeys, int keyCount, String delimiter,
        List<Contents> contents, String encodingType, boolean isTruncated) {
      this.name = name;
      this.prefix = prefix;
      this.maxKeys = maxKeys;
      this.keyCount = keyCount;
      this.delimiter = delimiter;
      this.contents = contents;
      this.encodingType = encodingType;
      this.isTruncated = isTruncated;
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

    public boolean getIsTruncated() {
      return isTruncated;
    }

    @Override
    public String toString() {
      return "Name=" + name + ", Prefix=" + prefix + ", MaxKeys=" + maxKeys + ", KeyCount=" + keyCount + ", Delimiter="
          + delimiter + ", Contents=" + contents + ", Encoding type=" + encodingType + ", IsTruncated=" + isTruncated;
    }
  }

  /**
   * ListBucketResult for listObjects API.
   */
  public static class ListBucketResult extends AbstractListBucketResult {
    @JacksonXmlProperty(localName = "Marker")
    private String marker;
    @JacksonXmlProperty(localName = "NextMarker")
    private String nextMarker;

    private ListBucketResult() {
      super();
    }

    public ListBucketResult(String name, String prefix, int maxKeys, int keyCount, String delimiter,
        List<Contents> contents, String encodingType, String marker, String nextMarker, boolean isTruncated) {
      super(name, prefix, maxKeys, keyCount, delimiter, contents, encodingType, isTruncated);
      this.marker = marker;
      this.nextMarker = nextMarker;
    }

    public String getMarker() {
      return marker;
    }

    public String getNextMarker() {
      return nextMarker;
    }

    @Override
    public String toString() {
      return super.toString() + ", Marker=" + marker + ", NextMarker=" + nextMarker;
    }
  }

  /**
   * ListBucketResult for listObjectsV2 API.
   */
  public static class ListBucketResultV2 extends AbstractListBucketResult {
    @JacksonXmlProperty(localName = "ContinuationToken")
    private String continuationToken;
    @JacksonXmlProperty(localName = "NextContinuationToken")
    private String nextContinuationToken;

    private ListBucketResultV2() {
      super();
    }

    public ListBucketResultV2(String name, String prefix, int maxKeys, int keyCount, String delimiter,
        List<Contents> contents, String encodingType, String continuationToken, String nextContinuationToken,
        boolean isTruncated) {
      super(name, prefix, maxKeys, keyCount, delimiter, contents, encodingType, isTruncated);
      this.continuationToken = continuationToken;
      this.nextContinuationToken = nextContinuationToken;
    }

    public String getContinuationToken() {
      return continuationToken;
    }

    public String getNextContinuationToken() {
      return nextContinuationToken;
    }

    @Override
    public String toString() {
      return super.toString() + ", ContinuationToken=" + continuationToken + ", NextContinuationToken="
          + nextContinuationToken;
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

  public static class S3BatchDeleteObjects {

    // Ensure that the "Delete" wrapper element is mapped correctly to the list of "Object" elements
    @JacksonXmlElementWrapper(useWrapping = false)  // Avoids wrapping the <Delete> element itself
    @JacksonXmlProperty(localName = "Object")      // Specifies that each <Object> element maps to an instance of S3BatchDeleteKeys
    private List<S3BatchDeleteKey> objects;

    public List<S3BatchDeleteKey> getObjects() {
      return objects;
    }

    public void setObjects(List<S3BatchDeleteKey> objects) {
      this.objects = objects;
    }

    @Override
    public String toString() {
      return "S3BatchDeleteObjects{" +
          "objects=" + objects +
          '}';
    }
  }

  public static class S3BatchDeleteKey {

    // Maps the <Key> element inside each <Object> to the 'key' property in S3BatchDeleteKeys
    @JacksonXmlProperty(localName = "Key")
    private String key;

    public String getKey() {
      return key;
    }

    public void setKey(String key) {
      this.key = key;
    }

    @Override
    public String toString() {
      return "S3BatchDeleteKeys{" +
          "key='" + key + '\'' +
          '}';
    }
  }

  // exact naming from S3BatchDelete response
  public static class DeleteResult {

    @JacksonXmlElementWrapper(useWrapping = false)
    @JacksonXmlProperty(localName = "Error") // Maps to <Error> in XML
    private ConcurrentLinkedQueue<S3MessagePayload.S3ErrorObject> errors;

    @JacksonXmlElementWrapper(useWrapping = false)
    @JacksonXmlProperty(localName = "Deleted") // Maps to <Deleted> in XML
    private ConcurrentLinkedQueue<S3MessagePayload.S3DeletedObject> deleted;

    // Getters and setters
    public ConcurrentLinkedQueue<S3MessagePayload.S3ErrorObject> getErrors() {
      return errors;
    }

    public void setErrors(ConcurrentLinkedQueue<S3ErrorObject> errors) {
      this.errors = errors;
    }

    public ConcurrentLinkedQueue<S3MessagePayload.S3DeletedObject> getDeleted() {
      return deleted;
    }

    public void setDeleted(ConcurrentLinkedQueue<S3MessagePayload.S3DeletedObject> deleted) {
      this.deleted = deleted;
    }
  }

  public static class S3ErrorObject {

    @JacksonXmlProperty(localName = "Key")
    private String key;

    @JacksonXmlProperty(localName = "Code")
    private String code;

    public S3ErrorObject(){}

    public S3ErrorObject(String key, String code) {
      this.key = key;
      this.code = code;
    }

    // Getters and setters
    public String getKey() {
      return key;
    }

    public void setKey(String key) {
      this.key = key;
    }

    public String getCode() {
      return code;
    }

    public void setCode(String code) {
      this.code = code;
    }

    @Override
    public String toString() {
      return "S3ErrorObject{key='" + key + "', code='" + code + "'}";
    }
  }

  public static class S3DeletedObject {

    @JacksonXmlProperty(localName = "Key")
    private String key;

    public S3DeletedObject() {}

    public S3DeletedObject(String key) {
      this.key = key;
    }

    // Getters and setters
    public String getKey() {
      return key;
    }

    public void setKey(String key) {
      this.key = key;
    }

    @Override
    public String toString() {
      return "S3DeletedObject{key='" + key + "'}";
    }
  }

}


