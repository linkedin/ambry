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
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Utils;
import com.google.common.base.Charsets;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.HelixPropertyStore;
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

  private final AtomicReference<Router> router;

  // If forBackFill is true, then when updating the account metadata, we don't create backup files and we don't merge
  // accounts from ambry-server with the provided accounts set.
  private final boolean forBackFill;

  /**
   * Constructor to create the RouterStore.
   * @param accountServiceMetrics The metrics set to update metrics.
   * @param backup The {@link LocalBackup} instance to manage backup files.
   * @param helixStore The {@link HelixPropertyStore} to fetch and update data.
   * @param router The {@link Router} instance to retrieve and put blobs.
   * @param forBackFill True if this {@link RouterStore} is created for backfill accounts to new zookeeper node.
   */
  RouterStore(AccountServiceMetrics accountServiceMetrics, LocalBackup backup, HelixPropertyStore<ZNRecord> helixStore,
      AtomicReference<Router> router, boolean forBackFill) {
    super(accountServiceMetrics, backup, helixStore, ACCOUNT_METADATA_BLOB_IDS_PATH);
    this.router = router;
    this.forBackFill = forBackFill;
  }

  @Override
  Map<String, String> fetchAccountMetadataFromZNRecord(ZNRecord record) {
    if (router.get() == null) {
      logger.error("Router is not yet initialized");
      return null;
    }
    List<String> accountBlobIDs = record.getListField(ACCOUNT_METADATA_BLOB_IDS_LIST_KEY);
    if (accountBlobIDs == null || accountBlobIDs.size() == 0) {
      logger.info("ZNRecord={} to read on path={} does not have a simple list with key={}", record,
          ACCOUNT_METADATA_BLOB_IDS_PATH, ACCOUNT_METADATA_BLOB_IDS_LIST_KEY);
      return null;
    } else {
      // parse the json string list and get the blob id with the latest version
      BlobIDAndVersion blobIDAndVersion = null;
      for (String accountBlobIDInJson : accountBlobIDs) {
        BlobIDAndVersion current = BlobIDAndVersion.fromJson(accountBlobIDInJson);
        if (blobIDAndVersion == null || blobIDAndVersion.version < current.version) {
          blobIDAndVersion = current;
        }
      }

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
      object.keySet().stream().forEach(key -> map.put(key, object.getString(key)));
      return map;
    } catch (Exception e) {
      logger.error("Failed to read account metadata from blob id={}", blobID, e);
      accountServiceMetrics.accountFetchFromAmbryServerErrorCount.inc();
    }
    return null;
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
   * A {@link DataUpdater} to be used for updating {@link #ACCOUNT_METADATA_BLOB_IDS_PATH} inside of
   * {@link #updateAccounts(Collection)}
   */
  private class ZKUpdater implements AccountMetadataStore.ZKUpdater {
    private final Collection<Account> accounts;
    private final Pair<String, Path> backupPrefixAndPath;
    private Map<String, String> potentialNewState;
    private String newBlobID = null;

    /**
     * @param accounts The {@link Account}s to update.
     */
    ZKUpdater(Collection<Account> accounts) {
      this.accounts = accounts;
      if (forBackFill) {
        // setting backupPrefixAndPath to be null effectily disable creating backup files.
        this.backupPrefixAndPath = null;
        return;
      }
      Pair<String, Path> backupPrefixAndPath = null;
      try {
        backupPrefixAndPath = backup.reserveBackupFile();
      } catch (IOException e) {
        logger.error("Error reserving backup file", e);
      }
      this.backupPrefixAndPath = backupPrefixAndPath;
    }

    @Override
    public ZNRecord update(ZNRecord znRecord) {
      // There are several steps to finish an update
      // 1. Fetch the list from the ZNRecord
      // 2. Fetch the AccountMetadata from the blob id if the list exist in the ZNRecord
      // 3. Construct a new AccountMetadata
      // 4. save it as a blob in the ambry server
      // 5. Add the new blob id back to the list.

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

      String errorMessage = null;
      List<String> accountBlobIDs = recordToUpdate.getListField(ACCOUNT_METADATA_BLOB_IDS_LIST_KEY);
      int newVersion = 1;
      Map<String, String> accountMap = null;
      if (accountBlobIDs != null && accountBlobIDs.size() != 0) {
        // parse the json string list and get the blob id with the latest version
        try {
          BlobIDAndVersion blobIDAndVersion = null;
          for (String accountBlobIDInJson : accountBlobIDs) {
            BlobIDAndVersion current = BlobIDAndVersion.fromJson(accountBlobIDInJson);
            if (blobIDAndVersion == null || blobIDAndVersion.version < current.version) {
              blobIDAndVersion = current;
            }
          }
          newVersion = blobIDAndVersion.version + 1;

          // Start Step 2:
          if (!forBackFill) {
            // if this is not for backfill, then just read account metadata from blob
            accountMap = readAccountMetadataFromBlobID(blobIDAndVersion.blobID);
          } else {
            accountMap = new HashMap<>();
          }
          // make this list mutable
          accountBlobIDs = new ArrayList<>(accountBlobIDs);
        } catch (JSONException e) {
          accountServiceMetrics.remoteDataCorruptionErrorCount.inc();
          errorMessage = "Exception occurred when parsing the blob id list from " + accountBlobIDs;
          logger.error(errorMessage);
          throw new IllegalStateException(errorMessage, e);
        } catch (Exception e) {
          errorMessage = "Unexpected exception occurred when parsing the blob id list from " + accountBlobIDs;
          logger.error(errorMessage, e);
          throw new IllegalStateException(errorMessage, e);
        }
      }
      // This ZNRecord doesn't exist when first time we update this ZNRecord, thus accountMap will be null.
      if (accountMap == null) {
        accountMap = new HashMap<>();
        accountBlobIDs = new ArrayList<>();
      }

      if (!forBackFill) {
        // Start step 3:
        AccountInfoMap localAccountInfoMap;
        try {
          localAccountInfoMap = new AccountInfoMap(accountServiceMetrics, accountMap);
        } catch (JSONException e) {
          accountServiceMetrics.remoteDataCorruptionErrorCount.inc();
          errorMessage = "Exception occurred when building AccountInfoMap from accountMap " + accountMap;
          logger.error(errorMessage, e);
          throw new IllegalStateException(errorMessage, e);
        }
        backup.maybePersistOldState(backupPrefixAndPath, accountMap);

        // if there is any conflict with the existing record, fail the update. Exception thrown in this updater will
        // be caught by Helix and helixStore#update will return false.
        if (localAccountInfoMap.hasConflictingAccount(this.accounts)) {
          // Throw exception, so that helixStore can capture and terminate the update operation
          errorMessage = "Updating accounts failed because one account to update conflicts with existing accounts";
          logger.error(errorMessage);
          throw new IllegalArgumentException(errorMessage);
        }
      }

      for (Account account : this.accounts) {
        try {
          accountMap.put(String.valueOf(account.getId()), account.toJson(true).toString());
        } catch (Exception e) {
          errorMessage = "Updating accounts failed because unexpected exception occurred when updating accountId="
              + account.getId() + " accountName=" + account.getName();
          // Do not depend on Helix to log, so log the error message here.
          logger.error(errorMessage, e);
          throw new IllegalStateException(errorMessage, e);
        }
      }

      // Start step 4:
      long startTimeMs = System.currentTimeMillis();
      try {
        this.newBlobID = writeAccountMapToRouter(accountMap, router.get());
        accountServiceMetrics.accountUpdateToAmbryTimeInMs.update(System.currentTimeMillis() - startTimeMs);
      } catch (Exception e) {
        accountServiceMetrics.accountUpdatesToAmbryServerErrorCount.inc();
        errorMessage =
            "Updating accounts failed because unexpected error occurred when uploading AccountMetadata to ambry";
        logger.error(errorMessage, e);
        throw new IllegalStateException(errorMessage, e);
      }

      // The new account map to backup locally
      potentialNewState = accountMap;

      // Start step 5:
      accountBlobIDs.add(new BlobIDAndVersion(this.newBlobID, newVersion).toJson());
      recordToUpdate.setListField(ACCOUNT_METADATA_BLOB_IDS_LIST_KEY, accountBlobIDs);
      return recordToUpdate;
    }

    @Override
    public void afterUpdate(boolean isUpdateSucceeded) {
      if (isUpdateSucceeded) {
        backup.maybePersistNewState(backupPrefixAndPath, potentialNewState);
      } else if (newBlobID != null) {
        // Delete the ambry blob regardless what error fails the update.
        try {
          // Block this execution? or maybe wait for a while then get out?
          router.get().deleteBlob(newBlobID, SERVICE_ID).get();
        } catch (Exception e) {
          logger.error("Failed to delete blob={} because of {}", newBlobID, e);
          accountServiceMetrics.accountDeletesToAmbryServerErrorCount.inc();
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

    BlobIDAndVersion(String blobID, int version) {
      this.blobID = blobID;
      this.version = version;
    }

    public String toJson() {
      JSONObject object = new JSONObject();
      object.put(BLOBID_KEY, blobID);
      object.put(VERSION_KEY, version);
      return object.toString();
    }

    public int getVersion() {
      return version;
    }

    public String getBlobID() {
      return blobID;
    }

    static BlobIDAndVersion fromJson(String json) throws JSONException {
      JSONObject object = new JSONObject(json);
      String blobID = object.getString(BLOBID_KEY);
      int version = object.getInt(VERSION_KEY);
      return new BlobIDAndVersion(blobID, version);
    }
  }
}
