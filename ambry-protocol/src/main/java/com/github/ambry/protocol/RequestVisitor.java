/**
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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

/**
 * Used to visit the request and performs any actions related to the request.
 */
public interface RequestVisitor {

  /**
   * Performs any actions related to Put request.
   * @param putRequest element to visit.
   */
  void visit(PutRequest putRequest);

  /**
   * Performs any actions related to Get request.
   * @param getRequest to visit.
   */
  void visit(GetRequest getRequest);

  /**
   * Performs any actions related to TTL update request.
   * @param ttlUpdateRequest to visit.
   */
  void visit(TtlUpdateRequest ttlUpdateRequest);

  /**
   * Performs any actions related to Delete request.
   * @param deleteRequest to visit.
   */
  void visit(DeleteRequest deleteRequest);

  /**
   * Performs any actions related to Un-delete request.
   * @param undeleteRequest to visit.
   */
  void visit(UndeleteRequest undeleteRequest);

  /**
   * Performs any actions related to Replica metadata request.
   * @param replicaMetadataRequest to visit.
   */
  void visit(ReplicaMetadataRequest replicaMetadataRequest);

  /**
   * Performs any actions related to Replicate blob request.
   * @param replicateBlobRequest to visit.
   */
  void visit(ReplicateBlobRequest replicateBlobRequest);

  /**
   * Performs any actions related to Admin request.
   * @param adminRequest to visit.
   */
  void visit(AdminRequest adminRequest);
}
