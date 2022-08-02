package com.github.ambry.named;

import com.github.ambry.utils.Utils;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ambry.account.Account;
import com.github.ambry.account.AccountService;
import com.github.ambry.account.Container;


public class MySqlPartiallyReadableBlobDb implements PartiallyReadableBlobDb {
  private static final Logger logger = LoggerFactory.getLogger(MySqlPartiallyReadableBlobDb.class);

  private static final String USERNAME = "root";
  private static final String PASSWORD = "L!nhX!nh1803";
  private static final String JDBC_URL = "jdbc:mysql://localhost:3306/partial?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";

  // table name
  private static final String BLOB_CHUNKS = "blob_chunks";
  // column names
  private static final String ACCOUNT_ID = "account_id";
  private static final String CONTAINER_ID = "container_id";
  private static final String BLOB_NAME = "blob_name";
  private static final String CHUNK_ORDER = "chunk_order";
  private static final String CHUNK_ID = "chunk_id";
  private static final String STATUS = "status";
  private static final String LAST_UPDATED_TS = "last_updated_ts";

  private static final String PK_MATCH = String.format("(%s, %s, %s) = (?, ?, ?)", ACCOUNT_ID, CONTAINER_ID, BLOB_NAME);
  private static final String ORDER_MATCH = String.format("(%s) >= (?)", CHUNK_ORDER);

  private static final String INSERT_QUERY =
      String.format("INSERT INTO %s (%s, %s, %s, %5$s, %6$s, %7$s, %8$s) VALUES (?, ?, ?, ?, ?, ?, ?)", BLOB_CHUNKS, ACCOUNT_ID,
          CONTAINER_ID, BLOB_NAME, CHUNK_ORDER, CHUNK_ID, STATUS, LAST_UPDATED_TS);

  private static final String GET_QUERY =
      String.format("SELECT %s, %s, %s, %s FROM %s WHERE %s AND %s", CHUNK_ORDER, CHUNK_ID, STATUS, LAST_UPDATED_TS, BLOB_CHUNKS, PK_MATCH, ORDER_MATCH);

  public MySqlPartiallyReadableBlobDb() {
  }

  @Override
  public List<PartiallyReadableBlobRecord> get(String accountId, String containerId, String blobName, int startingChunkOrder) {
    List<PartiallyReadableBlobRecord> chunksRecord = new ArrayList<>();

    try (Connection connection = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD);
           PreparedStatement statement = connection.prepareStatement(GET_QUERY)) {

        statement.setString(1, accountId);
        statement.setString(2, containerId);
        statement.setString(3, blobName);
        statement.setInt(4, startingChunkOrder);

        try (ResultSet resultSet = statement.executeQuery()) {
          while (resultSet.next()) {
            int chunkOrder = resultSet.getInt(1);
            String chunkId = resultSet.getString(2);
            String status = resultSet.getString(3);
            long lastUpdatedTs = resultSet.getLong(4);

            long currentTime = System.currentTimeMillis();

            if ((currentTime - lastUpdatedTs) >= 500) {
              // handle error for timeout
            }

            if (status == "ERROR") {
              // handle error status
            } else {
               chunksRecord.add(new PartiallyReadableBlobRecord(accountId, containerId, blobName, chunkId, chunkOrder,
                  lastUpdatedTs, status));
            }
          }
        } catch (SQLException e) {
            System.out.println(e);
        }
      }
      catch (SQLException e) {
        System.out.println(e);
      }
      return chunksRecord;
  }

  @Override
  public void put(PartiallyReadableBlobRecord record) {

    try (Connection connection = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD);
         PreparedStatement statement = connection.prepareStatement(INSERT_QUERY)) {

      statement.setString(1, record.getAccountId());
      statement.setString(2, record.getContainerId());
      statement.setString(3, record.getBlobName());
      statement.setInt(4, record.getChunkOrder());
      statement.setString(5, record.getChunkId());
      statement.setString(6, record.getStatus());
      statement.setLong(7, record.getLastUpdatedTs());

      try {
        int rowUpdated = statement.executeUpdate();

        if (rowUpdated < 1) {
          // error for put fails
        }
        else {
          System.out.println("Successfully inserted record with name " + record.getBlobName() + " and chunkId " + record.getChunkId());
        }
      }
      catch (SQLException e) {
        System.out.println(e);
      }
    }
    catch (SQLException e) {
      System.out.println(e);
    }
  }

  public static void main(String[] args) {
    PartiallyReadableBlobRecord record = new PartiallyReadableBlobRecord("A", "A", "Name",
        "112", 2, 100000, "IN_PROGRESS");
    MySqlPartiallyReadableBlobDb db = new MySqlPartiallyReadableBlobDb();
//    db.put(record);
    List<PartiallyReadableBlobRecord> list = db.get("A", "A", "Name", 2);
    for (PartiallyReadableBlobRecord item : list) {
      System.out.println(item.getChunkOrder());
      System.out.println(item.getBlobName());
    }
  }
}
