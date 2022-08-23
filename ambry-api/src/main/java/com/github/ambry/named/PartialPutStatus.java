package com.github.ambry.named;

public enum PartialPutStatus {
  /**
   * The put operation for the partially readable blob has finished successfully, with all the chunks saved to server
   * successfully.
   */
  SUCCESS,

  /**
   * The put operation for the partially readable blob is still in progress.
   */
  PENDING,

  /**
   * There is an error with the put process for the partially readable blob
   */
  ERROR,

}