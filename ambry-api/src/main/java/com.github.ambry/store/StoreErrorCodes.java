package com.github.ambry.store;

/**
 * The error codes that the store returns
 */
public enum StoreErrorCodes {
  ID_Not_Found,
  TTL_Expired,
  ID_Deleted,
  IOError,
  Initialization_Error,
  Already_Exist,
  Store_Not_Started,
  Store_Already_Started,
  Illegal_Index_Operation,
  Index_Creation_Failure,
  Index_Version_Error,
  Unknown_Error
}
