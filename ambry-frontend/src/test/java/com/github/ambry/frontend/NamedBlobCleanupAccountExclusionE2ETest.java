/*
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.frontend;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.Account;
import com.github.ambry.account.AccountService;
import com.github.ambry.account.Container;
import com.github.ambry.commons.InMemNamedBlobDb;
import com.github.ambry.named.NamedBlobDb;
import com.github.ambry.named.NamedBlobRecord;
import com.github.ambry.protocol.NamedBlobState;
import com.github.ambry.router.Router;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;


/**
 * End-to-end test for ACCOUNT-LEVEL named-blob cleanup exclusion, driving the REAL {@link NamedBlobsCleanupRunner}
 * against a REAL in-memory named-blob store ({@link InMemNamedBlobDb}) that holds real superseded (stale) versions.
 * The store, the stale detection, the cleanup removal and the runner's per-container skip decision are all the real
 * code; only the peripheral {@link Router} and {@link AccountService} are stubbed.
 *
 * <p>It proves the property that matters operationally for a bare {@code "accountName"} entry: the stale versions of
 * EVERY container in that account are RETAINED, while a container that lives in a different account still has its
 * stale versions DELETED.
 */
public class NamedBlobCleanupAccountExclusionE2ETest {
  private static final String FIRST_BLOB_NAME = "\0";
  private static final String EXCLUDED_ACCOUNT = "excluded-account";
  private static final String OTHER_ACCOUNT = "other-account";
  private static final short EXCLUDED_ACCOUNT_ID = 100;
  private static final short OTHER_ACCOUNT_ID = 200;

  @Test
  public void bareAccountEntryRetainsEveryContainerWhileOtherAccountsAreCleaned() throws Exception {
    // Real in-memory store. The only adaptation is returning a null page cursor so the resilient runner's page
    // loop terminates -- InMemNamedBlobDb otherwise returns "" forever (a store quirk unrelated to the exclusion
    // feature). put(), the real stale detection, and the real cleanupStaleData removal are all the real code.
    InMemNamedBlobDb db = new InMemNamedBlobDb(SystemTime.getInstance(), 100, false) {
      @Override
      public CompletableFuture<NamedBlobDb.StaleBlobsWithLatestBlobName> pullStaleBlobs(Container c, String blobName) {
        return super.pullStaleBlobs(c, blobName)
            .thenApply(r -> new NamedBlobDb.StaleBlobsWithLatestBlobName(r.getStaleBlobs(), null));
      }

      @Override
      public CompletableFuture<NamedBlobDb.StaleBlobsWithLatestBlobName> pullStaleBlobs(Container c, String blobName,
          int maxResults) {
        return pullStaleBlobs(c, blobName);
      }
    };

    // The excluded account has TWO containers; the other account has one. Each holds two versions of the same blob
    // name, so each container ends up with exactly one stale (superseded) version. Container names are distinct so
    // the store's container-name-keyed stale scan does not bleed across accounts.
    putVersion(db, EXCLUDED_ACCOUNT, "container-a", "artifact", "exc-a-v1");
    putVersion(db, EXCLUDED_ACCOUNT, "container-a", "artifact", "exc-a-v2");
    putVersion(db, EXCLUDED_ACCOUNT, "container-b", "artifact", "exc-b-v1");
    putVersion(db, EXCLUDED_ACCOUNT, "container-b", "artifact", "exc-b-v2");
    putVersion(db, OTHER_ACCOUNT, "container-c", "artifact", "oth-c-v1");
    putVersion(db, OTHER_ACCOUNT, "container-c", "artifact", "oth-c-v2");

    Container excludedA = container("container-a", EXCLUDED_ACCOUNT_ID);
    Container excludedB = container("container-b", EXCLUDED_ACCOUNT_ID);
    Container other = container("container-c", OTHER_ACCOUNT_ID);

    // Precondition: exactly one stale version in each container before cleanup runs.
    assertEquals(1, db.pullStaleBlobs(excludedA, FIRST_BLOB_NAME).get().getStaleBlobs().size());
    assertEquals(1, db.pullStaleBlobs(excludedB, FIRST_BLOB_NAME).get().getStaleBlobs().size());
    assertEquals(1, db.pullStaleBlobs(other, FIRST_BLOB_NAME).get().getStaleBlobs().size());

    Account excludedAccount = mock(Account.class);
    when(excludedAccount.getName()).thenReturn(EXCLUDED_ACCOUNT);
    Account otherAccount = mock(Account.class);
    when(otherAccount.getName()).thenReturn(OTHER_ACCOUNT);
    AccountService accountService = mock(AccountService.class);
    when(accountService.getAccountById(EXCLUDED_ACCOUNT_ID)).thenReturn(excludedAccount);
    when(accountService.getAccountById(OTHER_ACCOUNT_ID)).thenReturn(otherAccount);
    when(accountService.getContainersByStatus(Container.ContainerStatus.ACTIVE)).thenReturn(
        new HashSet<>(Arrays.asList(excludedA, excludedB, other)));
    when(accountService.getContainersByStatus(Container.ContainerStatus.INACTIVE)).thenReturn(Collections.emptySet());

    Router router = mock(Router.class);
    when(router.deleteBlob(anyString(), anyString())).thenReturn(CompletableFuture.completedFuture(null));

    // Run the REAL cleanup runner with the exclusion set to a BARE ACCOUNT NAME -- it must exempt every container
    // in that account.
    new NamedBlobsCleanupRunner(router, db, accountService, 0, Collections.singleton(EXCLUDED_ACCOUNT),
        new MetricRegistry()).run();

    // Every container in the excluded account retains its stale version...
    assertEquals("excluded account container-a stale version must be retained", 1,
        db.pullStaleBlobs(excludedA, FIRST_BLOB_NAME).get().getStaleBlobs().size());
    assertEquals("excluded account container-b stale version must be retained", 1,
        db.pullStaleBlobs(excludedB, FIRST_BLOB_NAME).get().getStaleBlobs().size());
    // ...while the container in the other account is cleaned.
    assertEquals("other account container stale version must be cleaned", 0,
        db.pullStaleBlobs(other, FIRST_BLOB_NAME).get().getStaleBlobs().size());
    // The router was asked to delete only the other account's stale blob, never the excluded account's stale blobs.
    verify(router, times(1)).deleteBlob(eq("oth-c-v1"), anyString());
    verify(router, never()).deleteBlob(eq("exc-a-v1"), anyString());
    verify(router, never()).deleteBlob(eq("exc-b-v1"), anyString());
  }

  private void putVersion(InMemNamedBlobDb db, String account, String container, String blobName, String blobId)
      throws Exception {
    NamedBlobRecord record = new NamedBlobRecord(account, container, blobName, blobId, Utils.Infinite_Time);
    db.put(record, NamedBlobState.READY, true).get();
  }

  private Container container(String name, short parentAccountId) {
    Container c = mock(Container.class);
    when(c.getName()).thenReturn(name);
    when(c.getNamedBlobMode()).thenReturn(Container.NamedBlobMode.OPTIONAL);
    when(c.getParentAccountId()).thenReturn(parentAccountId);
    when(c.getId()).thenReturn((short) name.hashCode());
    return c;
  }
}
