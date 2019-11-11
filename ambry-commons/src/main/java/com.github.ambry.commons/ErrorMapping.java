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
    tempMap.put(StoreErrorCodes.IOError, ServerErrorCode.IO_Error);
    tempMap.put(StoreErrorCodes.ID_Deleted, ServerErrorCode.Blob_Deleted);
    tempMap.put(StoreErrorCodes.ID_Not_Found, ServerErrorCode.Blob_Not_Found);
    tempMap.put(StoreErrorCodes.TTL_Expired, ServerErrorCode.Blob_Expired);
    tempMap.put(StoreErrorCodes.Already_Exist, ServerErrorCode.Blob_Already_Exists);
    tempMap.put(StoreErrorCodes.Authorization_Failure, ServerErrorCode.Blob_Authorization_Failure);
    tempMap.put(StoreErrorCodes.Already_Updated, ServerErrorCode.Blob_Already_Updated);
    tempMap.put(StoreErrorCodes.Update_Not_Allowed, ServerErrorCode.Blob_Update_Not_Allowed);
    tempMap.put(StoreErrorCodes.Life_Version_Conflict, ServerErrorCode.Blob_Life_Version_Conflict);
    tempMap.put(StoreErrorCodes.ID_Not_Deleted, ServerErrorCode.Blob_Not_Deleted);
    tempMap.put(StoreErrorCodes.ID_Undeleted, ServerErrorCode.Blob_Already_Undeleted);
    tempMap.put(StoreErrorCodes.ID_Deleted_Permanently, ServerErrorCode.Blob_Deleted_Permanently);
    storeErrorMapping = Collections.unmodifiableMap(tempMap);
  }

  static {
    Map<MessageFormatErrorCodes, ServerErrorCode> tempMap = new HashMap<MessageFormatErrorCodes, ServerErrorCode>();
    tempMap.put(MessageFormatErrorCodes.Data_Corrupt, ServerErrorCode.Data_Corrupt);
    tempMap.put(MessageFormatErrorCodes.Unknown_Format_Version, ServerErrorCode.Data_Corrupt);
    tempMap.put(MessageFormatErrorCodes.IO_Error, ServerErrorCode.IO_Error);
    messageFormatErrorMapping = Collections.unmodifiableMap(tempMap);
  }

  public static ServerErrorCode getStoreErrorMapping(StoreErrorCodes code) {
    ServerErrorCode errorCode = storeErrorMapping.get(code);
    if (errorCode == null) {
      return ServerErrorCode.Unknown_Error;
    }
    return errorCode;
  }

  public static ServerErrorCode getMessageFormatErrorMapping(MessageFormatErrorCodes code) {
    ServerErrorCode errorCode = messageFormatErrorMapping.get(code);
    if (errorCode == null) {
      return ServerErrorCode.Unknown_Error;
    }
    return errorCode;
  }
}
