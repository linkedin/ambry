package com.github.ambry.server;

import com.github.ambry.messageformat.MessageFormatErrorCodes;
import com.github.ambry.shared.ServerErrorCode;
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
    storeErrorMapping = Collections.unmodifiableMap(tempMap);
  }

  static {
    Map<MessageFormatErrorCodes, ServerErrorCode> tempMap = new HashMap<MessageFormatErrorCodes, ServerErrorCode>();
    tempMap.put(MessageFormatErrorCodes.Data_Corrupt, ServerErrorCode.Data_Corrupt);
    tempMap.put(MessageFormatErrorCodes.Unknown_Format_Version, ServerErrorCode.Data_Corrupt);
    messageFormatErrorMapping = Collections.unmodifiableMap(tempMap);
  }

  public static ServerErrorCode getStoreErrorMapping(StoreErrorCodes code) {
    ServerErrorCode errorCode = storeErrorMapping.get(code);
    if (errorCode == null)
      return ServerErrorCode.Unknown_Error;
    return errorCode;
  }

  public static ServerErrorCode getMessageFormatErrorMapping(MessageFormatErrorCodes code) {
    ServerErrorCode errorCode = messageFormatErrorMapping.get(code);
    if (errorCode == null)
      return ServerErrorCode.Unknown_Error;
    return errorCode;
  }
}
