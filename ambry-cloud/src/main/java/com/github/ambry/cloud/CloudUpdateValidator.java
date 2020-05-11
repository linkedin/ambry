package com.github.ambry.cloud;

import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreKey;
import java.util.Map;


/**
 * Interface for Ambry validation logic for updates requested from cloud destination.
 */
public interface CloudUpdateValidator {
  /**
   * Validate operation on {@link CloudBlobMetadata} in cloud destination for the operation on blob with
   * given {@link StoreKey} and new requested life version.
   * @param metadata {@link CloudBlobMetadata} object obtained from cloud destination.
   * @param key {@link StoreKey} of the blob being updated.
   * @param updateFields {@link Map} of fields and new values requested for update.
   * @throws StoreException if validation fails.
   */
  void validateUpdate(CloudBlobMetadata metadata, StoreKey key, Map<String, Object> updateFields) throws StoreException;
}
