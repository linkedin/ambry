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
import java.util.stream.Collectors;
import javax.sql.DataSource;
import com.github.ambry.commons.BlobId;
import com.github.ambry.protocol.NamedBlobState;
import com.github.ambry.store.StoreKey;

/**
 * Detached evidence for one immutable root at one named version. Seal means that all declared
 * descriptors were captured. Neither seal nor inspection grants permission to delete anything.
 * The reader data source must consistently supply connections to the same database at READ COMMITTED or stronger.
 *
 * <p>Each named version has exactly one row, keyed by the same {@code (account_id, container_id, blob_name,
 * version)} tuple that identifies it in {@code named_blobs_v2}. The ordered child list is stored as that row's own
 * payload; there is no surrogate manifest ID and no separate page table. A worker that has saved the exact tuple can
 * recover the published list directly, without resolving the latest version or fetching the original root.
 */
final class NamedBlobManifestStore {
  // Length-prefixed child descriptors: an unsigned length byte followed by 1..127 blob ID bytes, repeated for every
  // declared child. The maximum encoded body is 32 MiB before driver and protocol overhead.
  private static final int MAX_ID_BYTES = 127;
  private static final int MAX_CHILDREN = 262144;
  private static final int MAX_VERSIONS = 32;
  private static final String TABLE = "named_blob_manifests_v1";
  private static final String HEADER_COLUMNS =
      "account_id,container_id,blob_name,version,root_id,child_count,publication_key";
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
   * A concurrent replacement can leave an unpublished manifest row; it cannot redirect it to a different root.
   */
  static void capture(Connection c, short account, short container, String name, long version, BlobId root, int expected,
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
        + " AND blob_id=? AND blob_state=? AND (deleted_ts IS NULL OR deleted_ts > UTC_TIMESTAMP(6)) FOR UPDATE")) {
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

    // Reserve the row without touching an existing payload: a concurrent racer for the same tuple must observe
    // whatever this statement establishes, not silently reset a sealed manifest.
    try (PreparedStatement s = c.prepareStatement("INSERT INTO " + TABLE
        + " (account_id,container_id,blob_name,version,root_id,child_count)"
        + " VALUES (?,?,?,?,?,?) ON DUPLICATE KEY UPDATE account_id=account_id")) {
      bindKey(s, account, container, name, version);
      s.setBytes(5, rootBytes);
      s.setInt(6, expected);
      s.executeUpdate();
    }
    Manifest m;
    byte[] existingPayload;
    // The eligibility read may have established a snapshot before a competing capture committed.
    try (PreparedStatement s = c.prepareStatement(
        "SELECT " + HEADER_COLUMNS + ",payload FROM " + TABLE + " WHERE " + KEY + " FOR UPDATE")) {
      bindKey(s, account, container, name, version);
      try (ResultSet r = s.executeQuery()) {
        if (!r.next()) {
          throw new SQLDataException("No manifest for the requested named version");
        }
        m = new Manifest(r);
        existingPayload = r.getBytes(8);
      }
    }
    if (!m.name.equals(name) || !m.root.equals(root.getID()) || m.count != expected) {
      // A residual row for a different root must never be mistaken for this capture: the tuple cannot be reused
      // for a different root while any manifest or cleanup job may still depend on the original.
      throw new SQLDataException("Conflicting manifest for named version");
    }

    byte[] body = encode(descriptors, expected, rootBytes);
    if (existingPayload == null) {
      try (PreparedStatement update = c.prepareStatement(
          "UPDATE " + TABLE + " SET payload=?,payload_hash=? WHERE " + KEY + " AND payload IS NULL")) {
        update.setBytes(1, body);
        update.setBytes(2, hash(account, container, name, version, m.count, m.root, body));
        bindKey(update, account, container, name, version, 3);
        if (update.executeUpdate() != 1) {
          throw new SQLDataException("Concurrent capture already sealed this manifest");
        }
      }
    } else if (!Arrays.equals(body, existingPayload)) {
      throw new SQLDataException("Capture retry disagrees with sealed manifest");
    }
  }

  private static byte[] encode(Iterator<? extends StoreKey> descriptors, int expected, byte[] rootBytes)
      throws SQLException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (int i = 0; i < expected; i++) {
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
    if (descriptors.hasNext()) {
      throw new SQLDataException("More child descriptors than declared");
    }
    return out.toByteArray();
  }

  private static void bindKey(PreparedStatement s, short account, short container, String name, long version)
      throws SQLException {
    bindKey(s, account, container, name, version, 1);
  }

  private static void bindKey(PreparedStatement s, short account, short container, String name, long version, int at)
      throws SQLException {
    s.setInt(at, account);
    s.setInt(at + 1, container);
    s.setString(at + 2, name);
    s.setLong(at + 3, version);
  }

  static void checkManifestCount(List<Long> versions) {
    if (versions == null || versions.isEmpty() || versions.size() > MAX_VERSIONS) {
      throw new IllegalArgumentException("Invalid number of manifests");
    }
  }

  static List<Long> canonicalVersions(List<Long> versions) {
    checkManifestCount(versions);
    TreeSet<Long> sorted = new TreeSet<>();
    for (Long version : versions) {
      if (version == null || !sorted.add(version)) {
        throw new IllegalArgumentException("Expected distinct canonical manifest versions");
      }
    }
    return new ArrayList<>(sorted);
  }

  static void retire(Connection c, short account, short container, String name, List<Long> manifestVersions)
      throws SQLException {
    if (c.getAutoCommit() || c.getTransactionIsolation() != Connection.TRANSACTION_REPEATABLE_READ) {
      throw new IllegalStateException("Publication requires an InnoDB REPEATABLE READ transaction");
    }
    List<Long> requested = canonicalVersions(manifestVersions);
    byte[] receipt =
        sha256().digest(requested.stream().map(String::valueOf).collect(Collectors.joining(",")).getBytes(
            StandardCharsets.US_ASCII));
    Map<Long, Manifest> manifests = new LinkedHashMap<>();
    int published = 0;
    int totalChildren = 0;
    try (PreparedStatement s = c.prepareStatement(
        "SELECT " + HEADER_COLUMNS + ",payload,payload_hash FROM " + TABLE + " WHERE " + KEY + " FOR UPDATE")) {
      for (long version : requested) {
        bindKey(s, account, container, name, version);
        try (ResultSet r = s.executeQuery()) {
          if (!r.next()) {
            throw new SQLDataException("Unknown manifest");
          }
          Manifest m = new Manifest(r);
          byte[] payload = r.getBytes(8);
          byte[] payloadHash = r.getBytes(9);
          if (payload == null || payloadHash == null) {
            throw new SQLDataException("Manifest is not sealed");
          }
          if (!MessageDigest.isEqual(payloadHash, hash(account, container, name, version, m.count, m.root, payload))) {
            throw new SQLDataException("Invalid sealed manifest");
          }
          manifests.put(version, m);
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
    if (published == manifests.size()) {
      return;
    }
    if (published != 0 || totalChildren > MAX_CHILDREN) {
      throw new SQLDataException("Mixed or oversized publication");
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
          Manifest m = manifests.get(r.getLong(2));
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
    if (matches != manifests.size()) {
      throw new SQLDataException("Expected named version is absent");
    }
    try (PreparedStatement publish = c.prepareStatement("UPDATE " + TABLE
        + " SET publication_key=? WHERE " + KEY + " AND publication_key IS NULL");
        PreparedStatement retire = c.prepareStatement("UPDATE named_blobs_v2 SET retirement_fenced=TRUE,deleted_ts=NULL"
            + " WHERE retirement_fenced=FALSE AND " + KEY);
        PreparedStatement fence = c.prepareStatement("SELECT 1 FROM named_blobs_v2 WHERE retirement_fenced=TRUE"
            + " AND deleted_ts<=UTC_TIMESTAMP(6) AND " + KEY + " FOR UPDATE")) {
      for (Manifest m : manifests.values()) {
        publish.setBytes(1, receipt);
        bindKey(publish, account, container, name, m.version, 2);
        if (publish.executeUpdate() != 1) {
          throw new SQLDataException("Cannot publish sealed manifest");
        }
      }
      // History needs the TTL fence, not a fabricated cleanup manifest. All versions are still locked.
      history.addAll(manifests.keySet());
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

  static final class Manifest {
    final short account;
    final short container;
    final String name;
    final long version;
    final String root;
    final int count;
    final byte[] publicationKey;

    private Manifest(ResultSet r) throws SQLException {
      account = r.getShort(1);
      container = r.getShort(2);
      name = r.getString(3);
      version = r.getLong(4);
      root = Base64.getUrlEncoder().withoutPadding().encodeToString(r.getBytes(5));
      count = r.getInt(6);
      publicationKey = r.getBytes(7);
      if (count < 1 || count > MAX_CHILDREN) {
        throw new SQLDataException("Invalid manifest bounds: " + name + "/" + version);
      }
    }
  }

  /** Recover a known publication by its exact tuple, without resolving the name or reading the original root. */
  Manifest getPublished(short account, short container, String name, long version) throws SQLException {
    String sql = "SELECT " + HEADER_COLUMNS + " FROM " + TABLE + " WHERE " + KEY + " AND publication_key IS NOT NULL";
    try (Connection c = db.getConnection()) {
      requireCommittedReads(c);
      try (PreparedStatement s = c.prepareStatement(sql)) {
        bindKey(s, account, container, Objects.requireNonNull(name, "name"), version);
        try (ResultSet r = s.executeQuery()) {
          if (!r.next()) {
            return null;
          }
          Manifest manifest = new Manifest(r);
          // The named table's VARCHAR key uses a PAD SPACE collation.
          return name.equals(manifest.name) ? manifest : null;
        }
      }
    }
  }

  /** Read the full ordered child list for one sealed manifest row; the caller owns any inspection progress. */
  List<String> read(Manifest m) throws SQLException {
    byte[] body;
    try (Connection c = db.getConnection()) {
      requireCommittedReads(c);
      try (PreparedStatement s = c.prepareStatement("SELECT payload,payload_hash FROM " + TABLE + " WHERE " + KEY)) {
        bindKey(s, m.account, m.container, m.name, m.version);
        try (ResultSet r = s.executeQuery()) {
          if (!r.next()) {
            throw new SQLDataException("Missing sealed manifest: " + m.name + "/" + m.version);
          }
          byte[] payload = r.getBytes(1);
          byte[] payloadHash = r.getBytes(2);
          if (payload == null || payloadHash == null) {
            throw new SQLDataException("Manifest is not sealed: " + m.name + "/" + m.version);
          }
          if (!MessageDigest.isEqual(payloadHash, hash(m.account, m.container, m.name, m.version, m.count, m.root,
              payload))) {
            throw new SQLDataException("Manifest checksum mismatch: " + m.name + "/" + m.version);
          }
          body = payload;
        }
      }
    }
    ByteBuffer in = ByteBuffer.wrap(body);
    List<String> ids = new ArrayList<>(m.count);
    for (int i = 0; i < m.count; i++) {
      int length = in.hasRemaining() ? Byte.toUnsignedInt(in.get()) : 0;
      if (length == 0 || length > MAX_ID_BYTES || length > in.remaining()) {
        throw new SQLDataException("Invalid sealed child descriptor");
      }
      byte[] bytes = new byte[length];
      in.get(bytes);
      ids.add(Base64.getUrlEncoder().withoutPadding().encodeToString(bytes));
    }
    if (in.hasRemaining()) {
      throw new SQLDataException("Trailing bytes on sealed manifest");
    }
    return Collections.unmodifiableList(ids);
  }

  private static byte[] hash(short account, short container, String name, long version, int count, String root,
      byte[] body) {
    MessageDigest d = sha256();
    byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
    d.update(ByteBuffer.allocate(2 * Short.BYTES + Long.BYTES + 2 * Integer.BYTES)
        .putShort(account).putShort(container).putLong(version).putInt(count).putInt(nameBytes.length).array());
    d.update(nameBytes);
    d.update(root.getBytes(StandardCharsets.US_ASCII));
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
