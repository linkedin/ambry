package com.github.ambry.filetransfer;

public enum FileCopyOperationState {
  Start,
  META_DATA_REQUEST_SENT,
  META_DATA_RESPONSE_RECEIVED,
  CHUNK_DATA_REQUEST_IN_PROGRESS,
  CHUNK_DATA_EXCHANGE_COMPLETE,
}
