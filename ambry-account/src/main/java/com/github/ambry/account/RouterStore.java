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
import com.github.ambry.config.HelixAccountServiceConfig;
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
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;
import org.I0Itec.zkclient.DataUpdater;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.HelixPropertyStore;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class RouterStore implements AccountMetadataStore {
  static final String ACCOUNT_METADATA_BLOB_IDS_PATH = "/account_metadata/blobids";
  static final String ACCOUNT_METADATA_BLOB_IDS_LIST_KEY = "accountMetadataBlobIds";
  private static final String ZN_RECORD_ID = "account_metadata_version_list";
  private static final Logger logger = LoggerFactory.getLogger(RouterStore.class);

  private final HelixAccountService accountService;
  private final HelixPropertyStore<ZNRecord> helixStore;
  private final HelixAccountServiceConfig config;
  private final ReentrantLock lock = new ReentrantLock();

  private static final Short ACCOUNT_ID = Account.HELIX_ACCOUNT_SERVICE_ACCOUNT_ID;
  private static final Short CONTAINER_ID = Container.HELIX_ACCOUNT_SERVICE_CONTAINER_ID;
  private static final String SERVICE_ID = "helixAccountService";

  RouterStore(HelixAccountService accountService, HelixPropertyStore<ZNRecord> helixStore,
      HelixAccountServiceConfig config) {
    this.accountService = accountService;
    this.helixStore = helixStore;
    this.config = config;
  }

  @Override
  public void fetchAndUpdateCache(boolean isCalledFromListener) {
    // when fetching account metadata, we need to fetch the list of blob ids that point to different versions of
    // account metadata as well the latest version of account metadata.
    lock.lock();
    try {
      long startTimeMs = System.currentTimeMillis();
      logger.trace("Start reading account metadata blob ids list from path={}", ACCOUNT_METADATA_BLOB_IDS_PATH);
      ZNRecord zNRecord = helixStore.get(ACCOUNT_METADATA_BLOB_IDS_PATH, null, AccessOption.PERSISTENT);
      logger.trace("Fetched ZNRecord from path={}, took time={} ms", ACCOUNT_METADATA_BLOB_IDS_PATH, startTimeMs);
      if (zNRecord == null) {
        logger.debug("The ZNRecord to read does not exist on path={}", ACCOUNT_METADATA_BLOB_IDS_PATH);
      } else {
        List<String> accountBlobIDs = zNRecord.getListField(ACCOUNT_METADATA_BLOB_IDS_LIST_KEY);
        if (accountBlobIDs == null || accountBlobIDs.size() == 0) {
          logger.debug("ZNRecord={} to read on path={} does not have a simple list with key={}", zNRecord,
              ACCOUNT_METADATA_BLOB_IDS_PATH, ACCOUNT_METADATA_BLOB_IDS_LIST_KEY);
        } else {
          // parse the json string list and get the blob id with the latest version
          BlobIDAndVersion blobIDAndVersion = null;
          for (String accountBlobIDInJson : accountBlobIDs) {
            BlobIDAndVersion current = BlobIDAndVersion.fromJson(accountBlobIDInJson);
            if (blobIDAndVersion == null || blobIDAndVersion.version < current.version) {
              blobIDAndVersion = current;
            }
          }

          Map<String, String> accountMap = readAccountMetadataFromBlobID(blobIDAndVersion.blobID);
          if (accountMap == null) {
            logger.debug("BlobID={} to read but no account map returned");
          } else {
            logger.trace("Start parsing remote account data from blob {} and versioned at {}.", blobIDAndVersion.blobID,
                blobIDAndVersion.version);
            AccountInfoMap newAccountInfoMap = new AccountInfoMap(accountService, accountMap);
            AccountInfoMap oldAccountInfoMap = accountService.updateCache(newAccountInfoMap);
            accountService.notifyAccountUpdateConsumers(newAccountInfoMap, oldAccountInfoMap, isCalledFromListener);
          }
        }
      }
    } finally {
      lock.unlock();
    }
  }

  private Map<String, String> readAccountMetadataFromBlobID(String blobID) {
    long startTimeMs = System.currentTimeMillis();
    Future<GetBlobResult> resultF = accountService.router.get().getBlob(blobID, new GetBlobOptionsBuilder().build());
    try {
      GetBlobResult result = resultF.get();
      accountService.accountServiceMetrics.accountFetchFromAmbryTimeInMs.update(
          System.currentTimeMillis() - startTimeMs);

      int blobSize = (int) result.getBlobInfo().getBlobProperties().getBlobSize();
      InputStream input = new ReadableStreamChannelInputStream(result.getBlobDataChannel());
      byte[] bytes = Utils.readBytesFromStream(input, blobSize);

      JSONObject object = new JSONObject(new String(bytes, Charsets.UTF_8));
      Map<String, String> map = new HashMap<>();
      object.keySet().stream().forEach(key -> map.put(key, object.getString(key)));
      return map;
    } catch (Exception e) {
      logger.debug("Failed to read account metadata from blob id={}", blobID, e);
      accountService.accountServiceMetrics.accountFetchFromAmbryServerErrorCount.inc();
    }
    return null;
  }

  @Override
  public boolean updateAccounts(Collection<Account> accountsToUpdate) {
    ZKUpdater zkUpdater = new ZKUpdater(accountsToUpdate);
    boolean hasSucceeded = helixStore.update(ACCOUNT_METADATA_BLOB_IDS_PATH, zkUpdater, AccessOption.PERSISTENT);
    zkUpdater.cleanup(hasSucceeded);
    return hasSucceeded;
  }

  static String writeAccountMapToRouter(Map<String, String> accountMap, Router router) throws Exception {
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

  private class ZKUpdater implements DataUpdater<ZNRecord> {
    private final Collection<Account> accounts;
    private final Pair<String, Path> backupPrefixAndPath;
    private Map<String, String> potentialNewState;
    private String newBlobID = null;

    ZKUpdater(Collection<Account> accounts) {
      this.accounts = accounts;
      Pair<String, Path> backupPrefixAndPath = null;
      if (accountService.backupDirPath != null) {
        try {
          backupPrefixAndPath = accountService.reserveBackupFile();
        } catch (IOException e) {
          logger.error("Error reserving backup file", e);
        }
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
        logger.debug(
            "ZNRecord does not exist on path={} in HelixPropertyStore when updating accounts. Creating a new ZNRecord.",
            ACCOUNT_METADATA_BLOB_IDS_PATH);
        recordToUpdate = new ZNRecord(ZN_RECORD_ID);
      } else {
        recordToUpdate = znRecord;
      }

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
          accountMap = readAccountMetadataFromBlobID(blobIDAndVersion.blobID);
          // make this list mutable
          accountBlobIDs = new ArrayList<>(accountBlobIDs);
        } catch (JSONException e) {
          logger.error("Exception occurred when parsing the blob id list from {}", accountBlobIDs);
          accountService.accountServiceMetrics.remoteDataCorruptionErrorCount.inc();
          throw new IllegalStateException("Exception occurred when parsing blob id list", e);
        } catch (Exception e) {
          logger.error("Unexpected exception occurred when parsing the blob id list from {}", accountBlobIDs, e);
          throw new IllegalStateException("Unexpected exception occurred when parsing blob id list", e);
        }
      }
      if (accountMap == null) {
        accountMap = new HashMap<>();
        accountBlobIDs = new ArrayList<>();
      }

      // Start step 3:
      AccountInfoMap localAccountInfoMap;
      try {
        localAccountInfoMap = new AccountInfoMap(accountService, accountMap);
      } catch (JSONException e) {
        // Do not depend on Helix to log, so log the error message here.
        logger.error("Exception occurred when building AccountInfoMap from accountMap={}", accountMap, e);
        accountService.accountServiceMetrics.remoteDataCorruptionErrorCount.inc();
        throw new IllegalStateException("Exception occurred when parsing blob id list", e);
      }
      accountService.maybePersistOldState(backupPrefixAndPath, accountMap);

      // if there is any conflict with the existing record, fail the update. Exception thrown in this updater will
      // be caught by Helix and helixStore#update will return false.
      if (accountService.hasConflictingAccount(this.accounts, localAccountInfoMap)) {
        // Throw exception, so that helixStore can capture and terminate the update operation
        throw new IllegalArgumentException(
            "Updating accounts failed because one account to update conflicts with existing accounts");
      }
      for (Account account : this.accounts) {
        try {
          accountMap.put(String.valueOf(account.getId()), account.toJson(true).toString());
        } catch (Exception e) {
          String message = "Updating accounts failed because unexpected exception occurred when updating accountId="
              + account.getId() + " accountName=" + account.getName();
          // Do not depend on Helix to log, so log the error message here.
          logger.error(message, e);
          throw new IllegalStateException(message, e);
        }
      }

      // Start step 4:
      long startTimeMs = System.currentTimeMillis();
      try {
        this.newBlobID = writeAccountMapToRouter(accountMap, accountService.router.get());
        accountService.accountServiceMetrics.accountUpdateToAmbryTimeInMs.update(
            System.currentTimeMillis() - startTimeMs);
      } catch (Exception e) {
        accountService.accountServiceMetrics.accountUpdatesToAmbryServerErrorCount.inc();
        String message =
            "Updating accounts failed because unexpected error occurred when uploading AccountMetadata to ambry";
        logger.error(message, e);
        throw new IllegalStateException(message, e);
      }

      // The new account map to backup locally
      potentialNewState = accountMap;

      // Start step 5:
      accountBlobIDs.add(new BlobIDAndVersion(this.newBlobID, newVersion).toJson());
      recordToUpdate.setListField(ACCOUNT_METADATA_BLOB_IDS_LIST_KEY, accountBlobIDs);
      return recordToUpdate;
    }

    void cleanup(boolean isUpdateSucceeded) {
      if (isUpdateSucceeded) {
        accountService.maybePersistNewState(backupPrefixAndPath, potentialNewState);
      } else if (newBlobID != null) {
        // Delete the ambry blob regardless what error fails the update.
        try {
          // Block this execution? or maybe wait for a while then get out?
          accountService.router.get().deleteBlob(newBlobID, SERVICE_ID).get();
        } catch (Exception e) {
          logger.error("Failed to delete blob={} because of {}", newBlobID, e);
          accountService.accountServiceMetrics.accountDeletesToAmbryServerErrorCount.inc();
        }
      }
    }
  }

  static class BlobIDAndVersion implements Comparable<BlobIDAndVersion> {
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

    private static BlobIDAndVersion fromJson(String json) throws JSONException {
      JSONObject object = new JSONObject(json);
      String blobID = object.getString(BLOBID_KEY);
      int version = object.getInt(VERSION_KEY);
      return new BlobIDAndVersion(blobID, version);
    }

    @Override
    public int compareTo(BlobIDAndVersion o) {
      return version - o.version;
    }
  }
}
