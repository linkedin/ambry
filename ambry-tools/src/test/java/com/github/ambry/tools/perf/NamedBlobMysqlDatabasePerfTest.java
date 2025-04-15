package com.github.ambry.tools.perf;

import com.github.ambry.account.AccountService;
import com.github.ambry.named.NamedBlobDb;
import java.util.Collections;
import java.util.Properties;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.*;


public class NamedBlobMysqlDatabasePerfTest {
  @Test
  public void testTestTypeConstructor() throws Exception {
    AccountService accountService = mock(AccountService.class);
    when(accountService.getAllAccounts()).thenReturn(Collections.emptyList());
    NamedBlobDb db = mock(NamedBlobDb.class);
    Properties props = new Properties();

    NamedBlobMysqlDatabasePerf.PerformanceTestWorker worker =
        (NamedBlobMysqlDatabasePerf.PerformanceTestWorker) NamedBlobMysqlDatabasePerf.TestType.LIST.getWorkerClass()
            .getConstructor(int.class, NamedBlobDb.class, AccountService.class, int.class, Properties.class)
            .newInstance(0, db, accountService, 10, props);
    Assert.assertNotNull(worker);
  }
}
