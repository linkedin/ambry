/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
 * Enum of types of administration requests/responses.
 * </p>
 * The order of these enums should not be changed since their relative position goes into the serialized form of
 * requests/responses
 */
public enum AdminRequestOrResponseType {
  TriggerCompaction, RequestControl, ReplicationControl, CatchupStatus, BlobStoreControl, HealthCheck, BlobIndex, ForceDelete
}
