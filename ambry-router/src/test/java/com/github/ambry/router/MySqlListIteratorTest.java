package com.github.ambry.router;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.messageformat.CompositeBlobInfo;
import com.github.ambry.named.MySqlPartiallyReadableBlobDb;
import com.github.ambry.named.PartiallyReadableBlobDb;
import com.github.ambry.named.PartiallyReadableBlobRecord;
import com.github.ambry.rest.RestServiceException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;


public class MySqlListIteratorTest {

//  private final MySqlListIterator mySqlListIterator;
//  private final List<CompositeBlobInfo.ChunkMetadata> parentList = new ArrayList<>();
//  private final String accountName = "account-test";
//  private final String containerName = "container-test";
//  private final String blobName = "blob-test";
//  private final PartiallyReadableBlobDb partiallyReadableBlobDb = new MySqlPartiallyReadableBlobDb();
//  private final ClusterMap clusterMap = new MockClusterMap();
//
//  public MySqlListIteratorTest() throws IOException {
//    this.mySqlListIterator = new MySqlListIterator(parentList, accountName, containerName, blobName,
//        -1, partiallyReadableBlobDb, false, clusterMap);
//  }
//
//  @Test
//  public void testHasNext() {
//    boolean hasNext = mySqlListIterator.hasNext();
//    //Assert.assertEquals(hasNext, false);
//    parentList.add(null);
//    hasNext = mySqlListIterator.hasNext();
//    //Assert.assertEquals(hasNext, true);
//    PartiallyReadableBlobRecord record = new PartiallyReadableBlobRecord(accountName, containerName, blobName,
//        "AAYQAf____8AAQAAAAAAAAAA1IaNBDJtTReTu-xTfs_QkA", 0L, 4194304L,
//        1660707189892L, "PENDING");
//    try {
//      partiallyReadableBlobDb.put(record);
//    }
//    catch (RestServiceException e) {
//
//    }
//    hasNext = mySqlListIterator.hasNext();
//    //Assert.assertEquals(hasNext, true);
//    PartiallyReadableBlobRecord recordWithDifferentName = new PartiallyReadableBlobRecord(accountName, containerName,
//        "dummyName", "AAYQAf____8AAQAAAAAAAAAA1IaNBDJtTReTu-xTfs_QTT", 0L, 4194304L,
//        1660707189892L, "PENDING");
//    try {
//      partiallyReadableBlobDb.put(record);
//    }
//    catch (RestServiceException e) {
//
//    }
//    hasNext = mySqlListIterator.hasNext();
//    //Assert.assertEquals(hasNext, false);
//  }
}
