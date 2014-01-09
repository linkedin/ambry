package com.github.ambry.shared;

/**
 * The error codes that the server returns on a failed request
 */
public enum ServerErrorCode {
  No_Error,
  IO_Error,
  Blob_Not_Found,
  Blob_Deleted,
  Blob_Expired,
  Data_Corrupt,
  Partition_Unknown,
  Disk_Unavailable,
  Partition_ReadOnly,
  Unknown_Error
}
