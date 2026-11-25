/*
 * Copyright 2026 LinkedIn Corp. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.github.ambry.named;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLDataException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;
import java.util.UUID;
import javax.sql.DataSource;
import com.github.ambry.commons.BlobId;
import com.github.ambry.protocol.NamedBlobState;
import com.github.ambry.store.StoreKey;

/**
 * Detached evidence for one immutable root at one named version. Seal means that all declared
 * descriptors were captured. Neither seal nor inspection grants permission to delete anything.
 * The reader data source must consistently supply connections to the same database at READ COMMITTED or stronger.
 */
final class NamedBlobManifestStore {
  // V1 page format: up to 64 entries, each an unsigned length byte followed by 1..127 blob ID bytes.
  static final int PAGE_SIZE = 64;
  private static final int MAX_ID_BYTES = 127;
  private static final int MAX_CHILDREN = 131072;
  private static final int MAX_VERSIONS = 32;
  private static final String HEADS = "named_blob_manifests_v1";
  private static final String PAGES = "named_blob_manifest_pages_v1";
  private static final String COLUMNS =
      "manifest_id,account_id,container_id,blob_name,version,root_id,child_count,publication_key";
  private static final String KEY = "account_id=? AND container_id=? AND blob_name=? AND version=?";

  private final DataSource db;

  NamedBlobManifestStore(DataSource db) {
    this.db = Objects.requireNonNull(db, "db");
  }

  private static void requireCommittedReads(Connection c) throws SQLException {
    int isolation = c.getTransactionIsolation();
    if (isolation != Connection.TRANSACTION_READ_COMMITTED && isolation != Connection.TRANSACTION_REPEATABLE_READ
        && isolation != Connection.TRANSACTION_SERIALIZABLE) {
      throw new IllegalStateException("Manifest readers require READ COMMITTED or stronger");
    }
  }

  /**
   * The caller must supply the ordered descriptors decoded from this exact root, not from a name
   * lookup. The live-row check is a nonlocking eligibility check, not a publication or TTL fence.
   * A concurrent replacement can leave an unpublished manifest; it cannot redirect its pages.
   */
  static String capture(Connection c, short account, short container, String name, long version, BlobId root, int expected,
      Iterator<? extends StoreKey> descriptors) throws SQLException {
    Objects.requireNonNull(root, "root");
    Objects.requireNonNull(descriptors, "descriptors");
    if (name == null || name.isEmpty() || expected < 1 || expected > MAX_CHILDREN
        || root.sizeInBytes() < 1 || root.sizeInBytes() > 50
        || root.getAccountId() != account || root.getContainerId() != container
        || root.getBlobDataType() != BlobId.BlobDataType.METADATA) {
      throw new SQLDataException("Unsupported named composite capture");
    }
    if (c.getAutoCommit()) {
      throw new IllegalStateException("Capture requires a caller-owned transaction");
    }
    byte[] rootBytes = root.toBytes();
    try (PreparedStatement s = c.prepareStatement("SELECT blob_name FROM named_blobs_v2 WHERE " + KEY
        + " AND blob_id=? AND blob_state=? AND (deleted_ts IS NULL OR deleted_ts > UTC_TIMESTAMP(6))")) {
      bindKey(s, account, container, name, version);
      s.setBytes(5, rootBytes);
      s.setInt(6, NamedBlobState.READY.ordinal());
      try (ResultSet r = s.executeQuery()) {
        // utf8mb4_bin on VARCHAR can compare names equal despite trailing spaces.
        if (!r.next() || !name.equals(r.getString(1))) {
          throw new SQLDataException("No matching live READY named version");
        }
      }
    }

    String candidate = UUID.randomUUID().toString();
    try (PreparedStatement s = c.prepareStatement("INSERT INTO " + HEADS
        + " (account_id,container_id,blob_name,version,root_id,child_count,manifest_id)"
        + " VALUES (?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE manifest_id=manifest_id")) {
      bindKey(s, account, container, name, version);
      s.setBytes(5, rootBytes);
      s.setInt(6, expected);
      s.setString(7, candidate);
      s.executeUpdate();
    }
    Manifest m;
    // The eligibility read may have established a snapshot before a competing capture committed.
    try (PreparedStatement s = c.prepareStatement("SELECT " + COLUMNS + " FROM " + HEADS + " WHERE " + KEY
        + " FOR UPDATE")) {
      bindKey(s, account, container, name, version);
      try (ResultSet r = s.executeQuery()) {
        if (!r.next()) {
          throw new SQLDataException("No manifest for the requested named version");
        }
        m = new Manifest(r);
      }
    }
    if (!m.name.equals(name) || !m.root.equals(root.getID()) || m.count != expected) {
      throw new SQLDataException("Conflicting manifest for named version");
    }

    boolean creating = candidate.equals(m.id);
    try (PreparedStatement insert = c.prepareStatement("INSERT INTO " + PAGES
        + " (manifest_id,first_child,payload,page_hash) VALUES (?,?,?,?)")) {
      for (int first = 0; first < expected; first += PAGE_SIZE) {
        int n = Math.min(PAGE_SIZE, expected - first);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int i = 0; i < n; i++) {
          StoreKey key = descriptors.hasNext() ? descriptors.next() : null;
          if (!(key instanceof BlobId) || key.sizeInBytes() < 1 || key.sizeInBytes() > MAX_ID_BYTES) {
            throw new SQLDataException("Missing or unsupported child descriptor");
          }
          byte[] bytes = key.toBytes();
          if (bytes.length != key.sizeInBytes() || Arrays.equals(bytes, rootBytes)) {
            throw new SQLDataException("Invalid child descriptor");
          }
          out.write(bytes.length);
          out.write(bytes, 0, bytes.length);
        }
        byte[] body = out.toByteArray();
        if (creating) {
          insert.setString(1, m.id);
          insert.setInt(2, first);
          insert.setBytes(3, body);
          insert.setBytes(4, hash(m, first, body));
          insert.executeUpdate();
        } else if (!Arrays.equals(body, readPayload(c, m, first, true))) {
          throw new SQLDataException("Capture retry disagrees with sealed page");
        }
      }
      if (descriptors.hasNext()) {
        throw new SQLDataException("More child descriptors than declared");
      }
    }
    return m.id;
  }

  private static void bindKey(PreparedStatement s, short account, short container, String name, long version)
      throws SQLException {
    s.setInt(1, account);
    s.setInt(2, container);
    s.setString(3, name);
    s.setLong(4, version);
  }

  static void checkManifestCount(List<String> ids) {
    if (ids == null || ids.isEmpty() || ids.size() > MAX_VERSIONS) {
      throw new IllegalArgumentException("Invalid number of manifests");
    }
  }

  static List<String> canonicalIds(List<String> ids) {
    checkManifestCount(ids);
    TreeSet<String> sorted = new TreeSet<>();
    for (String id : ids) {
      if (id == null || !UUID.fromString(id).toString().equals(id) || !sorted.add(id)) {
        throw new IllegalArgumentException("Expected distinct canonical manifest IDs");
      }
    }
    return new ArrayList<>(sorted);
  }

  static void retire(Connection c, short account, short container, String name, List<String> manifestIds)
      throws SQLException {
    if (c.getAutoCommit() || c.getTransactionIsolation() != Connection.TRANSACTION_REPEATABLE_READ) {
      throw new IllegalStateException("Publication requires an InnoDB REPEATABLE READ transaction");
    }
    List<String> ids = canonicalIds(manifestIds);
    byte[] receipt = sha256().digest(String.join(",", ids).getBytes(StandardCharsets.US_ASCII));
    Map<Long, Manifest> versions = new LinkedHashMap<>();
    int published = 0;
    int totalChildren = 0;
    try (PreparedStatement s = c.prepareStatement("SELECT " + COLUMNS + " FROM " + HEADS
        + " WHERE manifest_id=? FOR UPDATE")) {
      for (String id : ids) {
        s.setString(1, id);
        try (ResultSet r = s.executeQuery()) {
          if (!r.next()) {
            throw new SQLDataException("Unknown manifest");
          }
          Manifest m = new Manifest(r);
          if (!id.equals(m.id) || m.account != account || m.container != container || !m.name.equals(name)
              || versions.put(m.version, m) != null) {
            throw new SQLDataException("Manifest does not match the requested named version");
          }
          if (m.publicationKey != null) {
            if (!Arrays.equals(receipt, m.publicationKey)) {
              throw new SQLDataException("Manifest belongs to a different publication");
            }
            published++;
          }
          totalChildren += m.count;
        }
      }
    }
    // On replay do not even resolve the name: it may already refer to a different root.
    if (published == versions.size()) {
      return;
    }
    if (published != 0 || totalChildren > MAX_CHILDREN) {
      throw new SQLDataException("Mixed or oversized publication");
    }
    for (Manifest m : versions.values()) {
      verifySealed(c, m);
    }
    List<Long> history = new ArrayList<>();
    int matches = 0;
    // At REPEATABLE READ this range also locks the gap where a new, initially unfenced version must be inserted.
    try (PreparedStatement s = c.prepareStatement("SELECT blob_name,version,blob_id,blob_state,"
        + "(deleted_ts IS NOT NULL AND deleted_ts<=UTC_TIMESTAMP(6)) FROM named_blobs_v2"
        + " FORCE INDEX (named_blob_unfenced_versions) WHERE account_id=? AND container_id=? AND blob_name=?"
        + " AND retirement_fenced=FALSE ORDER BY version LIMIT ? FOR UPDATE")) {
      s.setInt(1, account);
      s.setInt(2, container);
      s.setString(3, name);
      s.setInt(4, MAX_VERSIONS + 1);
      try (ResultSet r = s.executeQuery()) {
        int rows = 0;
        while (r.next()) {
          if (++rows > MAX_VERSIONS || !name.equals(r.getString(1))) {
            throw new SQLDataException("Unsupported named version range");
          }
          Manifest m = versions.get(r.getLong(2));
          if (m == null && r.getBoolean(5)) {
            history.add(r.getLong(2));
            continue;
          }
          if (m == null || r.getInt(4) != NamedBlobState.READY.ordinal() || r.getBoolean(5)
              || !m.root.equals(Base64.getUrlEncoder().withoutPadding().encodeToString(r.getBytes(3)))) {
            throw new SQLDataException("Unrepresented or ineligible named version");
          }
          matches++;
        }
      }
    }
    if (matches != versions.size()) {
      throw new SQLDataException("Expected named version is absent");
    }
    try (PreparedStatement publish = c.prepareStatement("UPDATE " + HEADS
        + " SET publication_key=? WHERE manifest_id=? AND publication_key IS NULL");
        PreparedStatement retire = c.prepareStatement("UPDATE named_blobs_v2 SET retirement_fenced=TRUE,deleted_ts=NULL"
            + " WHERE retirement_fenced=FALSE AND " + KEY);
        PreparedStatement fence = c.prepareStatement("SELECT 1 FROM named_blobs_v2 WHERE retirement_fenced=TRUE"
            + " AND deleted_ts<=UTC_TIMESTAMP(6) AND " + KEY + " FOR UPDATE")) {
      for (Manifest m : versions.values()) {
        publish.setBytes(1, receipt);
        publish.setString(2, m.id);
        if (publish.executeUpdate() != 1) {
          throw new SQLDataException("Cannot publish sealed manifest");
        }
      }
      // History needs the TTL fence, not a fabricated cleanup manifest. All versions are still locked.
      history.addAll(versions.keySet());
      for (long version : history) {
        bindKey(retire, account, container, name, version);
        bindKey(fence, account, container, name, version);
        if (retire.executeUpdate() != 1) {
          throw new SQLDataException("Named version is no longer eligible");
        }
        // Only the installed fence turns the explicit NULL into a delete timestamp.
        try (ResultSet r = fence.executeQuery()) {
          if (!r.next()) {
            throw new SQLDataException("Named retirement fence is not installed");
          }
        }
      }
    }
  }

  private static void verifySealed(Connection c, Manifest m) throws SQLException {
    try (PreparedStatement s = c.prepareStatement("SELECT first_child,payload,page_hash FROM " + PAGES
        + " WHERE manifest_id=? ORDER BY first_child FOR UPDATE")) {
      s.setString(1, m.id);
      s.setFetchSize(Integer.MIN_VALUE); // Connector/J streams rows instead of buffering the manifest.
      try (ResultSet r = s.executeQuery()) {
        int first = 0;
        while (r.next()) {
          if (first >= m.count || r.getInt(1) != first
              || !MessageDigest.isEqual(r.getBytes(3), hash(m, first, r.getBytes(2)))) {
            throw new SQLDataException("Invalid sealed page");
          }
          first += Math.min(PAGE_SIZE, m.count - first);
        }
        if (first != m.count) {
          throw new SQLDataException("Missing sealed page");
        }
      }
    }
  }

  static final class Manifest {
    final String id;
    final short account;
    final short container;
    final String name;
    final long version;
    final String root;
    final int count;
    final byte[] publicationKey;

    private Manifest(ResultSet r) throws SQLException {
      id = r.getString(1);
      account = r.getShort(2);
      container = r.getShort(3);
      name = r.getString(4);
      version = r.getLong(5);
      root = Base64.getUrlEncoder().withoutPadding().encodeToString(r.getBytes(6));
      count = r.getInt(7);
      publicationKey = r.getBytes(8);
      if (count < 1 || count > MAX_CHILDREN) {
        throw new SQLDataException("Invalid manifest bounds: " + id);
      }
    }
  }

  /** Recover a known publication without resolving the name. Unknown and unpublished IDs return null. */
  Manifest getPublished(String manifestId) throws SQLException {
    String sql = "SELECT " + COLUMNS + " FROM " + HEADS + " WHERE manifest_id=? AND publication_key IS NOT NULL";
    try (Connection c = db.getConnection()) {
      requireCommittedReads(c);
      try (PreparedStatement s = c.prepareStatement(sql)) {
        s.setString(1, Objects.requireNonNull(manifestId, "manifestId"));
        try (ResultSet r = s.executeQuery()) {
          return r.next() ? new Manifest(r) : null;
        }
      }
    }
  }

  /** Read one immutable page; the caller owns any inspection progress. */
  List<String> read(Manifest m, int first) throws SQLException {
    byte[] body;
    try (Connection c = db.getConnection()) {
      requireCommittedReads(c);
      body = readPayload(c, m, first, false);
    }
    ByteBuffer in = ByteBuffer.wrap(body);
    int n = Math.min(PAGE_SIZE, m.count - first);
    List<String> ids = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      int length = in.hasRemaining() ? Byte.toUnsignedInt(in.get()) : 0;
      if (length == 0 || length > MAX_ID_BYTES || length > in.remaining()) {
        throw new SQLDataException("Invalid sealed child descriptor");
      }
      byte[] bytes = new byte[length];
      in.get(bytes);
      ids.add(Base64.getUrlEncoder().withoutPadding().encodeToString(bytes));
    }
    if (in.hasRemaining()) {
      throw new SQLDataException("Trailing bytes on sealed page");
    }
    return Collections.unmodifiableList(ids);
  }

  private static byte[] readPayload(Connection c, Manifest m, int first, boolean currentRead) throws SQLException {
    if (first < 0 || first >= m.count || first % PAGE_SIZE != 0) {
      throw new SQLDataException("Invalid manifest page offset");
    }
    try (PreparedStatement s = c.prepareStatement(
        "SELECT payload,page_hash FROM " + PAGES + " WHERE manifest_id=? AND first_child=?"
            + (currentRead ? " FOR UPDATE" : ""))) {
      s.setString(1, m.id);
      s.setInt(2, first);
      try (ResultSet r = s.executeQuery()) {
        if (!r.next()) {
          throw new SQLDataException("Missing sealed page: " + m.id + "/" + first);
        }
        byte[] body = r.getBytes(1);
        byte[] checksum = r.getBytes(2);
        if (!MessageDigest.isEqual(checksum, hash(m, first, body))) {
          throw new SQLDataException("Manifest checksum mismatch: " + m.id + "/" + first);
        }
        return body;
      }
    }
  }

  private static byte[] hash(Manifest m, int first, byte[] body) {
    MessageDigest d = sha256();
    byte[] name = m.name.getBytes(StandardCharsets.UTF_8);
    d.update(m.id.getBytes(StandardCharsets.US_ASCII));
    d.update(ByteBuffer.allocate(2 * Short.BYTES + Long.BYTES + 3 * Integer.BYTES)
        .putShort(m.account).putShort(m.container).putLong(m.version)
        .putInt(m.count).putInt(first).putInt(name.length).array());
    d.update(name);
    d.update(m.root.getBytes(StandardCharsets.US_ASCII));
    d.update((byte) 0);
    return d.digest(body);
  }

  private static MessageDigest sha256() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException impossible) {
      throw new AssertionError(impossible);
    }
  }
}
