package com.github.ambry.named.filesystem;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CommonUtils {

  private static final Logger logger = LoggerFactory.getLogger(CommonUtils.class);

  public CommonUtils(){

  }

  public static boolean directoryExists(Connection connection, int accountId, int containerId, UUID parentDirId,
      String resourceName) throws SQLException {
    String sql =
        "SELECT COUNT(*) FROM directory_info WHERE account_id = ? AND container_id = ? AND parent_dir_id = ? AND resource_name = ? AND deleted_ts IS NULL";
    try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
      preparedStatement.setInt(1, accountId);
      preparedStatement.setInt(2, containerId);
      preparedStatement.setBytes(3, uuidToBytes(parentDirId));
      preparedStatement.setString(4, resourceName);

      ResultSet resultSet = preparedStatement.executeQuery();
      if (resultSet.next()) {
        return resultSet.getInt(1) > 0;
      }
    }
    return false;
  }

  public static UUID getDirectoryId(Connection connection, int accountId, int containerId, UUID parentDirId,
      String resourceName) throws SQLException {
    String sql =
        "SELECT resource_id, deleted_ts FROM directory_info WHERE account_id = ? AND container_id = ? AND parent_dir_id = ? AND resource_name = ?";
    try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
      preparedStatement.setInt(1, accountId);
      preparedStatement.setInt(2, containerId);
      preparedStatement.setBytes(3, uuidToBytes(parentDirId));
      preparedStatement.setString(4, resourceName);

      ResultSet resultSet = preparedStatement.executeQuery();
      if (resultSet.next()) {
        Timestamp deletedTs = resultSet.getTimestamp("deleted_ts");
        if(deletedTs != null){
          logger.error("Directory {} under account id {} container id {} is already deleted", resourceName, accountId, containerId);
          return null;
        }
        return bytesToUuid(resultSet.getBytes("resource_id"));
      }
    }
    return null;
  }

  public static UUID getLeafDirectoryId(Connection connection, int accountId, int containerId, String folderPath)
      throws SQLException {
    String[] pathParts = folderPath.split("/");

    UUID parentDirId = UUID.nameUUIDFromBytes("root".getBytes()); // Start with root directory ID

    for (String part : pathParts) {
      String sql = "SELECT resource_id FROM directory_info "
          + "WHERE account_id = ? AND container_id = ? AND parent_dir_id = ? AND resource_name = ? AND deleted_ts IS NULL";

      try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
        preparedStatement.setInt(1, accountId);
        preparedStatement.setInt(2, containerId);
        preparedStatement.setBytes(3, uuidToBytes(parentDirId));
        preparedStatement.setString(4, part);

        ResultSet resultSet = preparedStatement.executeQuery();
        if (resultSet.next()) {
          parentDirId = bytesToUuid(resultSet.getBytes("resource_id"));
        } else {
          return null; // Folder not found
        }
      }
    }

    return parentDirId;
  }

  public static boolean renameDirectory(Connection connection, int accountId, int containerId, String oldPath, String newName) throws SQLException {
    // Split the old path to find the parent directory and the current resource name
    String[] pathParts = oldPath.split("/");
    String oldResourceName = pathParts[pathParts.length - 1];
    String parentPath = String.join("/", java.util.Arrays.copyOf(pathParts, pathParts.length - 1));

    // Get the parent directory ID
    UUID parentDirId = getLeafDirectoryId(connection, accountId, containerId, parentPath);

    if (parentDirId == null) {
      System.out.println("Parent directory not found for path: " + parentPath);
      return false;
    }

    // Update the resource_name in directory_info
    String updateSql = "UPDATE directory_info "
        + "SET resource_name = ?, last_updated_ts = CURRENT_TIMESTAMP "
        + "WHERE account_id = ? AND container_id = ? AND parent_dir_id = ? AND resource_name = ?";

    try (PreparedStatement preparedStatement = connection.prepareStatement(updateSql)) {
      preparedStatement.setString(1, newName); // New name
      preparedStatement.setInt(2, accountId);  // Account ID
      preparedStatement.setInt(3, containerId); // Container ID
      preparedStatement.setBytes(4, uuidToBytes(parentDirId)); // Parent directory ID
      preparedStatement.setString(5, oldResourceName); // Old resource name

      int rowsUpdated = preparedStatement.executeUpdate();
      return rowsUpdated > 0;
    }
  }

  // Helper method to convert UUID to byte array
  public static byte[] uuidToBytes(UUID uuid) {
    byte[] bytes = new byte[16];
    System.arraycopy(longToBytes(uuid.getMostSignificantBits()), 0, bytes, 0, 8);
    System.arraycopy(longToBytes(uuid.getLeastSignificantBits()), 0, bytes, 8, 8);
    return bytes;
  }

  // Helper method to convert long to byte array
  public static byte[] longToBytes(long x) {
    byte[] bytes = new byte[8];
    for (int i = 7; i >= 0; i--) {
      bytes[i] = (byte) (x & 0xFF);
      x >>= 8;
    }
    return bytes;
  }

  // Helper method to convert byte array to UUID
  public static UUID bytesToUuid(byte[] bytes) {
    long mostSigBits = bytesToLong(bytes, 0);
    long leastSigBits = bytesToLong(bytes, 8);
    return new UUID(mostSigBits, leastSigBits);
  }

  public static long bytesToLong(byte[] bytes, int offset) {
    long value = 0;
    for (int i = 0; i < 8; i++) {
      value = (value << 8) | (bytes[offset + i] & 0xFF);
    }
    return value;
  }

}
