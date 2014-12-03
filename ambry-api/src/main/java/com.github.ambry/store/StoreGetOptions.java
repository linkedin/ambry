package com.github.ambry.store;

/**
 * The list of options that can be used while reading messages
 * from the store
 */
public enum StoreGetOptions {
  /**
   * This option indicates that the store needs to return the message even if it is expired
   * as long as the message has not been physically deleted from the store.
   */
  Store_Include_Expired,
  /**
   * This option indicates that the store needs to return the message even if it has been
   * marked for deletion as long as the message has not been physically deleted from the
   * store.
   */
  Store_Ignore_Deleted
}
