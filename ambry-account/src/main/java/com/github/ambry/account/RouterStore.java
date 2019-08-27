/*
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.account;

import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.ReadableStreamChannelInputStream;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.router.GetBlobOptionsBuilder;
import com.github.ambry.router.GetBlobResult;
import com.github.ambry.router.PutBlobOptions;
import com.github.ambry.router.Router;
import com.github.ambry.utils.Utils;
import com.google.common.base.Charsets;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.zookeeper.data.Stat;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * RouterStore stores the full set of {@link Account} metadata in {@code AmbryServer} and keep the list of blob ids in
 * a single {@link ZNRecord} in {@link HelixPropertyStore} at {@link #ACCOUNT_METADATA_BLOB_IDS_PATH}. Each item in the
 * list contains the blob id, as well as the version number. In this way, RouterStore keeps track of various versions
 * of {@link Account} metadata, and making rolling back to previous version much easier.
 *
 * When saving {@link Account} metadata as a blob in the {@code AmbryServer}, it's serialized as a json object, where
 * each {@link Account}'id is the key and the {@link Account}'s json format string is the value. Each blob has a predefined
 * {@link Account} id and {@link Container} id. The Account and Container are not actually created but serve as dummy ones
 * to bootstrap the creation of {@link Account} metadata.
 */
class RouterStore extends AccountMetadataStore {
  static final String ACCOUNT_METADATA_BLOB_IDS_PATH = "/account_metadata/blobids";
  static final String ACCOUNT_METADATA_BLOB_IDS_LIST_KEY = "accountMetadataBlobIds";
  private static final String ZN_RECORD_ID = "account_metadata_version_list";
  private static final Logger logger = LoggerFactory.getLogger(RouterStore.class);

  private static final Short ACCOUNT_ID = Account.HELIX_ACCOUNT_SERVICE_ACCOUNT_ID;
  private static final Short CONTAINER_ID = Container.HELIX_ACCOUNT_SERVICE_CONTAINER_ID;
  private static final String SERVICE_ID = "helixAccountService";

  private final int totalNumberOfVersionToKeep;
  private final AtomicReference<Router> router;
  // If forBackFill is true, then when updating the account metadata, we don't create backup files and we don't merge
  // accounts from ambry-server with the provided accounts set.
  private final boolean forBackFill;

  /**
   * Constructor to create the RouterStore.
   * @param accountServiceMetrics The metrics set to update metrics.
   * @param backupFileManager The {@link BackupFileManager} instance to manage backup files.
   * @param helixStore The {@link HelixPropertyStore} to fetch and update data.
   * @param router The {@link Router} instance to retrieve and put blobs.
   * @param forBackFill True if this {@link RouterStore} is created for backfill accounts to new zookeeper node.
   * @param totalNumberOfVersionToKeep The total number of previous versions of account metadata to keep.
   */
  RouterStore(AccountServiceMetrics accountServiceMetrics, BackupFileManager backupFileManager,
      HelixPropertyStore<ZNRecord> helixStore, AtomicReference<Router> router, boolean forBackFill,
      int totalNumberOfVersionToKeep) {
    super(accountServiceMetrics, backupFileManager, helixStore, ACCOUNT_METADATA_BLOB_IDS_PATH);
    this.router = router;
    this.forBackFill = forBackFill;
    this.totalNumberOfVersionToKeep = totalNumberOfVersionToKeep;
  }

  @Override
  Map<String, String> fetchAccountMetadataFromZNRecord(ZNRecord record) {
    if (router.get() == null) {
      logger.error("Router is not yet initialized");
      return null;
    }
    List<String> blobIDAndVersionsJson = record.getListField(ACCOUNT_METADATA_BLOB_IDS_LIST_KEY);
    if (blobIDAndVersionsJson == null || blobIDAndVersionsJson.size() == 0) {
      logger.info("ZNRecord={} to read on path={} does not have a simple list with key={}", record,
          ACCOUNT_METADATA_BLOB_IDS_PATH, ACCOUNT_METADATA_BLOB_IDS_LIST_KEY);
      return null;
    } else {
      // Parse the json string list and get the blob id with the latest version
      // Since the the blobIDAndVersionsJson.size() is greater than 0, max will return an optional with solid value.
      BlobIDAndVersion blobIDAndVersion = blobIDAndVersionsJson.stream()
          .map(BlobIDAndVersion::fromJson)
          .max(Comparator.comparing(BlobIDAndVersion::getVersion))
          .get();

      logger.trace("Start reading remote account data from blob {} and versioned at {}.", blobIDAndVersion.blobID,
          blobIDAndVersion.version);
      return readAccountMetadataFromBlobID(blobIDAndVersion.blobID);
    }
  }

  /**
   * Fetch the {@link Account} metadata from the given blob id.
   * @param blobID The blobID to fetch {@link Account} metadata from.
   * @return {@link Account} metadata in a map, and null when there is any error.
   */
  Map<String, String> readAccountMetadataFromBlobID(String blobID) {
    long startTimeMs = System.currentTimeMillis();
    Future<GetBlobResult> resultF = router.get().getBlob(blobID, new GetBlobOptionsBuilder().build());
    try {
      GetBlobResult result = resultF.get();
      accountServiceMetrics.accountFetchFromAmbryTimeInMs.update(System.currentTimeMillis() - startTimeMs);

      int blobSize = (int) result.getBlobInfo().getBlobProperties().getBlobSize();
      InputStream input = new ReadableStreamChannelInputStream(result.getBlobDataChannel());
      byte[] bytes = Utils.readBytesFromStream(input, blobSize);

      JSONObject object = new JSONObject(new String(bytes, Charsets.UTF_8));
      Map<String, String> map = new HashMap<>();
      object.keySet().forEach(key -> map.put(key, object.getString(key)));
      return map;
    } catch (Exception e) {
      logger.error("Failed to read account metadata from blob id={}", blobID, e);
      accountServiceMetrics.accountFetchFromAmbryServerErrorCount.inc();
    }
    return null;
  }

  /**
   * Get the list of {@link BlobIDAndVersion} from {@link ZNRecord}. This function returns null when there
   * is any error.
   * @return list of {@link BlobIDAndVersion}.
   */
  private List<BlobIDAndVersion> fetchAllBlobIDAndVersions() {
    if (router.get() == null) {
      logger.error("Router is not yet initialized");
      return null;
    }
    long startTimeMs = System.currentTimeMillis();
    logger.trace("Start reading ZNRecord from path={}", znRecordPath);
    Stat stat = new Stat();
    ZNRecord znRecord = helixStore.get(znRecordPath, stat, AccessOption.PERSISTENT);
    logger.trace("Fetched ZNRecord from path={}, took time={} ms", znRecordPath,
        System.currentTimeMillis() - startTimeMs);
    if (znRecord == null) {
      logger.info("The ZNRecord to read does not exist on path={}", znRecordPath);
      return null;
    }

    List<String> blobIDAndVersionsJson = znRecord.getListField(ACCOUNT_METADATA_BLOB_IDS_LIST_KEY);
    if (blobIDAndVersionsJson == null || blobIDAndVersionsJson.size() == 0) {
      logger.info("ZNRecord={} to read on path={} does not have a simple list with key={}", znRecord,
          ACCOUNT_METADATA_BLOB_IDS_PATH, ACCOUNT_METADATA_BLOB_IDS_LIST_KEY);
      return null;
    } else {
      return blobIDAndVersionsJson.stream().map(BlobIDAndVersion::fromJson).collect(Collectors.toList());
    }
  }

  /**
   * Returns all the versions of {@link Account} metadata as a list. It will return null when the versions are empty.
   * @return All the versions of {@link Account} metadata.
   */
  public List<Integer> getAllVersions() {
    List<BlobIDAndVersion> blobIDAndVersions = fetchAllBlobIDAndVersions() ;
    if (blobIDAndVersions == null) {
      return null;
    } else {
      return blobIDAndVersions.stream().map(BlobIDAndVersion::getVersion).collect(Collectors.toList());
    }
  }

  /**
   * Return a map from account id to {@link Account} metadata at given version. It returns null when there is any error.
   * @param version The version of {@link Account} metadata to return.
   * @return A map from account id to {@link Account} metadata in json format.
   * @throws IllegalArgumentException When the version is not valid.
   */
  public Map<String, String> fetchAccountMetadataAtVersion(int version) throws IllegalArgumentException {
    List<BlobIDAndVersion> blobIDAndVersions = fetchAllBlobIDAndVersions();
    if (blobIDAndVersions == null) {
      return null;
    }
    for (BlobIDAndVersion blobIDAndVersion: blobIDAndVersions) {
      if (blobIDAndVersion.getVersion() == version) {
        return readAccountMetadataFromBlobID(blobIDAndVersion.getBlobID());
      }
    }
    throw new IllegalArgumentException("Version " + version  + " doesn't exist");
  }

  @Override
  AccountMetadataStore.ZKUpdater createNewZKUpdater(Collection<Account> accounts) {
    Objects.requireNonNull(router.get(), "Router is null");
    return new ZKUpdater(accounts);
  }

  /**
   * Save the given {@link Account} metadata, as json object, in a blob in {@code AmbryServer}.
   * @param accountMap The {@link Account} metadata to save.
   * @param router The {@link Router} instance.
   * @return A blob id if the operation is finished successfully.
   * @throws Exception If there is any exceptions while saving the bytes to {@code AmbryServer}.
   */
  static String writeAccountMapToRouter(Map<String, String> accountMap, Router router) throws Exception {
    Objects.requireNonNull(router, "Router is null");
    // Construct the json object and save it to ambry server.
    JSONObject object = new JSONObject();
    for (Map.Entry<String, String> entry : accountMap.entrySet()) {
      object.put(entry.getKey(), entry.getValue());
    }

    ByteBufferReadableStreamChannel channel =
        new ByteBufferReadableStreamChannel(ByteBuffer.wrap(object.toString().getBytes(Charsets.UTF_8)));
    BlobProperties properties = new BlobProperties(channel.getSize(), SERVICE_ID, ACCOUNT_ID, CONTAINER_ID, false);
    return router.putBlob(properties, null, channel, PutBlobOptions.DEFAULT).get();
  }

  /**
   * Helper function to log the error message out and throw an {@link IllegalStateException}.
   * @param errorMessage The error message.
   * @param cause The cause exception.
   */
  private void logAndThrowIllegalStateException(String errorMessage, Exception cause) {
    logger.error(errorMessage, cause);
    throw new IllegalStateException(errorMessage, cause);
  }

  /**
   * A {@link DataUpdater} to be used for updating {@link #ACCOUNT_METADATA_BLOB_IDS_PATH} inside of
   * {@link #updateAccounts(Collection)}
   */
  private class ZKUpdater implements AccountMetadataStore.ZKUpdater {
    private final Collection<Account> accounts;
    private String newBlobID = null;
    private List<String> oldBlobIDsToDelete = null;

    /**
     * @param accounts The {@link Account}s to update.
     */
    ZKUpdater(Collection<Account> accounts) {
      this.accounts = accounts;
    }

    @Override
    public ZNRecord update(ZNRecord znRecord) {
      /* There are several steps to finish an update
       * 1. Fetch the list from the ZNRecord
       * 2. Fetch the AccountMetadata from the blob id if the list exist in the ZNRecord
       * 3. Construct a new AccountMetadata
       * 4. Save it as a blob in the ambry server
       * 5. Remove oldest version if number of version exceeds the maximum value.
       * 6. Add the new blob id to the list.
       */

      // Start step 1:
      ZNRecord recordToUpdate;
      if (znRecord == null) {
        logger.info(
            "ZNRecord does not exist on path={} in HelixPropertyStore when updating accounts. Creating a new ZNRecord.",
            ACCOUNT_METADATA_BLOB_IDS_PATH);
        recordToUpdate = new ZNRecord(ZN_RECORD_ID);
      } else {
        recordToUpdate = znRecord;
      }
      // This is the version number for the new blob id.
      int newVersion = 1;
      List<BlobIDAndVersion> blobIDAndVersions = new ArrayList<>();
      List<String> blobIDAndVersionsJson = recordToUpdate.getListField(ACCOUNT_METADATA_BLOB_IDS_LIST_KEY);
      blobIDAndVersionsJson =
          blobIDAndVersionsJson == null ? new LinkedList<>() : new LinkedList<>(blobIDAndVersionsJson);
      Map<String, String> accountMap = new HashMap<>();

      if (blobIDAndVersionsJson.size() != 0) {
        try {
          // Parse the json string list and get the BlobIDAndVersion with the latest version number.
          blobIDAndVersionsJson.forEach(
              accountBlobIDInJson -> blobIDAndVersions.add(BlobIDAndVersion.fromJson(accountBlobIDInJson)));
          Collections.sort(blobIDAndVersions, Comparator.comparing(BlobIDAndVersion::getVersion));
          BlobIDAndVersion blobIDAndVersion = blobIDAndVersions.get(blobIDAndVersions.size() - 1);
          newVersion = blobIDAndVersion.version + 1;

          // Start Step 2:
          // If this is not for backfill, then just read account metadata from blob, otherwise, initialize it with
          // an empty map and fill it up with the accountMap passed to constructor.
          accountMap = (!forBackFill) ? readAccountMetadataFromBlobID(blobIDAndVersion.blobID) : new HashMap<>();
          if (accountMap == null) {
            logAndThrowIllegalStateException("Fail to read account metadata from blob id " + blobIDAndVersion.blobID,
                null);
          }
        } catch (JSONException e) {
          accountServiceMetrics.remoteDataCorruptionErrorCount.inc();
          logAndThrowIllegalStateException(
              "Exception occurred when parsing the blob id list from " + blobIDAndVersionsJson, e);
        } catch (Exception e) {
          logAndThrowIllegalStateException(
              "Unexpected exception occurred when parsing the blob id list from " + blobIDAndVersionsJson, e);
        }
      }

      if (!forBackFill) {
        // Start step 3:
        AccountInfoMap localAccountInfoMap = null;
        try {
          localAccountInfoMap = new AccountInfoMap(accountServiceMetrics, accountMap);
        } catch (JSONException e) {
          accountServiceMetrics.remoteDataCorruptionErrorCount.inc();
          logAndThrowIllegalStateException(
              "Exception occurred when building AccountInfoMap from accountMap " + accountMap, e);
        }

        // If there is any conflict with the existing record, fail the update. Exception thrown in this updater will
        // be caught by Helix and helixStore#update will return false.
        if (localAccountInfoMap.hasConflictingAccount(this.accounts)) {
          // Throw exception, so that helixStore can capture and terminate the update operation
          logAndThrowIllegalStateException(
              "Updating accounts failed because one account to update conflicts with existing accounts", null);
        }
      }

      for (Account account : this.accounts) {
        try {
          accountMap.put(String.valueOf(account.getId()), account.toJson(true).toString());
        } catch (Exception e) {
          logAndThrowIllegalStateException(
              "Updating accounts failed because unexpected exception occurred when updating accountId="
                  + account.getId() + " accountName=" + account.getName(), e);
        }
      }

      // Start step 4:
      long startTimeMs = System.currentTimeMillis();
      try {
        this.newBlobID = writeAccountMapToRouter(accountMap, router.get());
        accountServiceMetrics.accountUpdateToAmbryTimeInMs.update(System.currentTimeMillis() - startTimeMs);
      } catch (Exception e) {
        accountServiceMetrics.accountUpdatesToAmbryServerErrorCount.inc();
        logAndThrowIllegalStateException(
            "Updating accounts failed because unexpected error occurred when uploading AccountMetadata to ambry", e);
      }

      // Start step 5:
      if (blobIDAndVersions.size() + 1 > totalNumberOfVersionToKeep) {
        Iterator<BlobIDAndVersion> iter = blobIDAndVersions.iterator();
        oldBlobIDsToDelete = new ArrayList<>();
        while (blobIDAndVersions.size() + 1 > totalNumberOfVersionToKeep) {
          BlobIDAndVersion blobIDAndVersion = iter.next();
          iter.remove();
          logger.info("Adding blob " + blobIDAndVersion.getBlobID() + " at version " + blobIDAndVersion.getVersion()
              + " to delete");
          oldBlobIDsToDelete.add(blobIDAndVersion.getBlobID());
        }
        blobIDAndVersionsJson = blobIDAndVersions.stream().map(BlobIDAndVersion::toJson).collect(Collectors.toList());
      }

      // Start step 6:
      blobIDAndVersionsJson.add(new BlobIDAndVersion(this.newBlobID, newVersion).toJson());
      recordToUpdate.setListField(ACCOUNT_METADATA_BLOB_IDS_LIST_KEY, blobIDAndVersionsJson);
      return recordToUpdate;
    }

    @Override
    public void afterUpdate(boolean isUpdateSucceeded) {
      if (!isUpdateSucceeded && newBlobID != null) {
        // Delete the ambry blob regardless what error fails the update.
        try {
          logger.info("Removing blob " + newBlobID + " since the update failed");
          // Block this execution? or maybe wait for a while then get out?
          router.get().deleteBlob(newBlobID, SERVICE_ID).get();
        } catch (Exception e) {
          logger.error("Failed to delete blob=" + newBlobID, e);
          accountServiceMetrics.accountDeletesToAmbryServerErrorCount.inc();
        }
      }
      // Notice this logic might end up with the dangling blobs, when the process crashes before the for loop.
      // But since the frequency to update account metadata is pretty rare, it won't be a big problem.
      if (isUpdateSucceeded && oldBlobIDsToDelete != null) {
        for (String blobID : oldBlobIDsToDelete) {
          try {
            logger.info("Removing blob " + blobID);
            // Block this execution? or maybe wait for a while then get out?
            router.get().deleteBlob(blobID, SERVICE_ID).get();
          } catch (Exception e) {
            logger.error("Failed to delete blob=" + blobID, e);
            accountServiceMetrics.accountDeletesToAmbryServerErrorCount.inc();
          }
        }
      }
    }
  }

  /**
   * Helper class that encapsulates the blob id and version number to serve as each item in the blob id list that would
   * eventually be persisted in {@link ZNRecord} at {@link #ACCOUNT_METADATA_BLOB_IDS_PATH}.
   */
  static class BlobIDAndVersion {
    private final String blobID;
    private final int version;

    private static final String BLOBID_KEY = "blob_id";
    private static final String VERSION_KEY = "version";

    /**
     * Constructor to create a {@link BlobIDAndVersion}.
     * @param blobID The blob id.
     * @param version The version associated with this blob id.
     */
    BlobIDAndVersion(String blobID, int version) {
      this.blobID = blobID;
      this.version = version;
    }

    /**
     * Return a string in json format of this object.
     * @return A string in json format of this object.
     */
    public String toJson() {
      JSONObject object = new JSONObject();
      object.put(BLOBID_KEY, blobID);
      object.put(VERSION_KEY, version);
      return object.toString();
    }

    /**
     * Return version number.
     * @return Version number.
     */
    public int getVersion() {
      return version;
    }

    /**
     * Return blob id.
     * @return Blob id.
     */
    public String getBlobID() {
      return blobID;
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (!(o instanceof BlobIDAndVersion)) {
        return false;
      }
      BlobIDAndVersion other = (BlobIDAndVersion) o;
      return blobID.equals(other.blobID) && version == other.version;
    }

    /**
     * Deserialize a string that carries a json object to an {@link BlobIDAndVersion}.
     * @param json The string that carries a json object.
     * @return A {@link BlobIDAndVersion} object.
     * @throws JSONException If parsing the string to {@link JSONObject} fails.
     */
    static BlobIDAndVersion fromJson(String json) throws JSONException {
      JSONObject object = new JSONObject(json);
      String blobID = object.getString(BLOBID_KEY);
      int version = object.getInt(VERSION_KEY);
      return new BlobIDAndVersion(blobID, version);
    }
  }
}
