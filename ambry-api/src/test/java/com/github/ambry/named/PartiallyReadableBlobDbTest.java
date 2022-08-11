package com.github.ambry.named;

import com.github.ambry.rest.RestServiceException;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.*;


public class PartiallyReadableBlobDbTest {

  @Test
  public void insertAndRetrieveSuccessTest() {
    PartiallyReadableBlobDb db = new MySqlPartiallyReadableBlobDb();
    long currentTimeMs = System.currentTimeMillis();
    PartiallyReadableBlobRecord record = new PartiallyReadableBlobRecord("Account B",
        "Container B", "Name B", "dummy chunk id 3", 6, 2,
        currentTimeMs, PartialPutStatus.PENDING.name());
    try {
      db.put(record);
    }
    catch (RestServiceException e) {

    }
    List<PartiallyReadableBlobRecord> list = new ArrayList<>();
    try {
      list = db.get("Account B", "Container B", "Name B", 6);
    }
    catch (RestServiceException e) {

    }
    assertEquals(1, list.size());
    for (PartiallyReadableBlobRecord getRecord : list) {
      assertEquals("Account B", getRecord.getAccountName());
      assertEquals("Container B", getRecord.getContainerName());
      assertEquals("Name B", getRecord.getBlobName());
      assertEquals("dummy chunk id 3", getRecord.getChunkId());
      assertEquals(2, getRecord.getChunkSize());
      assertEquals(6, getRecord.getChunkOffset());
      assertEquals(currentTimeMs, getRecord.getLastUpdatedTs());
      assertEquals(PartialPutStatus.PENDING.name(), getRecord.getStatus());
    }
  }
}
