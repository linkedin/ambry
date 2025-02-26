/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.commons;

import com.github.ambry.messageformat.MessageFormatErrorCodes;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.store.StoreErrorCodes;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


/**
 * Mapping of errors from all packages into server error codes
 */
public class ErrorMapping {
  private static final Map<StoreErrorCodes, ServerErrorCode> storeErrorMapping;
  private static final Map<MessageFormatErrorCodes, ServerErrorCode> messageFormatErrorMapping;

  static {
    Map<StoreErrorCodes, ServerErrorCode> tempMap = new HashMap<StoreErrorCodes, ServerErrorCode>();
    tempMap.put(StoreErrorCodes.IOError, ServerErrorCode.IOError);
    tempMap.put(StoreErrorCodes.IDDeleted, ServerErrorCode.BlobDeleted);
    tempMap.put(StoreErrorCodes.IDNotFound, ServerErrorCode.BlobNotFound);
    tempMap.put(StoreErrorCodes.TTLExpired, ServerErrorCode.BlobExpired);
    tempMap.put(StoreErrorCodes.AlreadyExist, ServerErrorCode.BlobAlreadyExists);
    tempMap.put(StoreErrorCodes.AuthorizationFailure, ServerErrorCode.BlobAuthorizationFailure);
    tempMap.put(StoreErrorCodes.AlreadyUpdated, ServerErrorCode.BlobAlreadyUpdated);
    tempMap.put(StoreErrorCodes.UpdateNotAllowed, ServerErrorCode.BlobUpdateNotAllowed);
    tempMap.put(StoreErrorCodes.LifeVersionConflict, ServerErrorCode.BlobLifeVersionConflict);
    tempMap.put(StoreErrorCodes.IDNotDeleted, ServerErrorCode.BlobNotDeleted);
    tempMap.put(StoreErrorCodes.IDUndeleted, ServerErrorCode.BlobAlreadyUndeleted);
    tempMap.put(StoreErrorCodes.IDDeletedPermanently, ServerErrorCode.BlobDeletedPermanently);
    storeErrorMapping = Collections.unmodifiableMap(tempMap);
  }

  static {
    Map<MessageFormatErrorCodes, ServerErrorCode> tempMap = new HashMap<MessageFormatErrorCodes, ServerErrorCode>();
    tempMap.put(MessageFormatErrorCodes.DataCorrupt, ServerErrorCode.DataCorrupt);
    tempMap.put(MessageFormatErrorCodes.UnknownFormatVersion, ServerErrorCode.DataCorrupt);
    tempMap.put(MessageFormatErrorCodes.IOError, ServerErrorCode.IOError);
    messageFormatErrorMapping = Collections.unmodifiableMap(tempMap);
  }

  public static ServerErrorCode getStoreErrorMapping(StoreErrorCodes code) {
    ServerErrorCode errorCode = storeErrorMapping.get(code);
    if (errorCode == null) {
      return ServerErrorCode.UnknownError;
    }
    return errorCode;
  }

  public static ServerErrorCode getMessageFormatErrorMapping(MessageFormatErrorCodes code) {
    ServerErrorCode errorCode = messageFormatErrorMapping.get(code);
    if (errorCode == null) {
      return ServerErrorCode.UnknownError;
    }
    return errorCode;
  }
}
