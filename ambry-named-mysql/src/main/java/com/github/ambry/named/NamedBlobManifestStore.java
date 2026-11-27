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
 * Detached evidence for one immutable root at one named version. A row in {@code named_blob_manifests_v1} exists
 * only once it is fully formed: there is no durable pre-publication staging state, no nullable seal column, and no
 * separate capture transaction whose result another transaction later publishes. The single entry point below
 * validates the entire requested set, locks the named range, locks the manifest tuples, and -- only once every
 * check has passed -- inserts the rows and fences their history in one transaction. Neither a row's existence nor
 * its publication grants permission to delete anything.
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

  /** One requested version's immutable inputs: the caller must supply descriptors decoded from this exact root. */
  static final class PendingVersion {
    final BlobId root;
    final int expected;
    final Iterator<? extends StoreKey> descriptors;

    PendingVersion(BlobId root, int expected, Iterator<? extends StoreKey> descriptors) {
      this.root = Objects.requireNonNull(root, "root");
      this.expected = expected;
      this.descriptors = Objects.requireNonNull(descriptors, "descriptors");
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

  /**
   * Validate, publish and fence an entire set of named versions in one transaction. Every declared bound -- per
   * version child count, aggregate child count across the whole request, root shape -- is checked purely against
   * caller-supplied values before a single iterator is touched, a payload is encoded, or a statement is issued: a
   * doomed oversized request never reads or hashes a byte under lock. The named row range is then locked ahead of
   * the manifest table, the same order an ordinary named PUT's own AFTER INSERT/UPDATE guard uses, so the two can
   * never deadlock against each other. A replay that already carries this exact receipt, root and content for every
   * requested version returns successfully without resolving the name; anything less than all of them matching is
   * rejected rather than partially honored.
   */
  static void retire(Connection c, short account, short container, String name, Map<Long, PendingVersion> pending)
      throws SQLException {
    if (c.getAutoCommit() || c.getTransactionIsolation() != Connection.TRANSACTION_REPEATABLE_READ) {
      throw new IllegalStateException("Publication requires an InnoDB REPEATABLE READ transaction");
    }
    if (name == null || name.isEmpty()) {
      throw new SQLDataException("Unsupported named composite capture");
    }
    Objects.requireNonNull(pending, "pending");
    List<Long> requested = canonicalVersions(new ArrayList<>(pending.keySet()));

    long totalDeclared = 0;
    for (long version : requested) {
      PendingVersion p = pending.get(version);
      if (p == null) {
        throw new IllegalArgumentException("Missing pending descriptors for version " + version);
      }
      BlobId root = p.root;
      if (p.expected < 1 || p.expected > MAX_CHILDREN || root.sizeInBytes() < 1 || root.sizeInBytes() > 50
          || root.getAccountId() != account || root.getContainerId() != container
          || root.getBlobDataType() != BlobId.BlobDataType.METADATA) {
        throw new SQLDataException("Unsupported named composite capture");
      }
      totalDeclared += p.expected;
    }
    if (totalDeclared > MAX_CHILDREN) {
      throw new SQLDataException("Mixed or oversized publication");
    }

    byte[] receipt = receipt(requested);

    // Only now, with the whole request's declared size already bounded, consume each iterator and encode its body.
    Map<Long, byte[]> rootBytes = new LinkedHashMap<>();
    Map<Long, byte[]> bodies = new LinkedHashMap<>();
    Map<Long, String> rootIds = new LinkedHashMap<>();
    for (long version : requested) {
      PendingVersion p = pending.get(version);
      byte[] rb = p.root.toBytes();
      rootBytes.put(version, rb);
      rootIds.put(version, Base64.getUrlEncoder().withoutPadding().encodeToString(rb));
      bodies.put(version, encode(p.descriptors, p.expected, rb));
    }

    // Lock the named row range first: this is the same order (named_blobs_v2, then named_blob_manifests_v1) an
    // ordinary PUT's own trigger uses. At REPEATABLE READ this range also locks the gap where a new, initially
    // unfenced version must be inserted.
    Map<Long, Boolean> liveMatch = new LinkedHashMap<>();
    List<Long> history = new ArrayList<>();
    // An unrequested, still-live row blocks a fresh publish -- retirement only ever covers a contiguous prefix of
    // the unfenced range -- but a pure replay never needs to resolve the name at all, so this is recorded rather
    // than thrown here and is only enforced once the manifest lookup below has ruled out a full replay.
    boolean unrequestedLiveVersion = false;
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
          long version = r.getLong(2);
          if (!pending.containsKey(version)) {
            if (r.getBoolean(5)) {
              history.add(version);
            } else {
              unrequestedLiveVersion = true;
            }
            continue;
          }
          boolean eligible = r.getInt(4) == NamedBlobState.READY.ordinal() && !r.getBoolean(5)
              && Arrays.equals(r.getBytes(3), rootBytes.get(version));
          liveMatch.put(version, eligible);
        }
      }
    }

    // Pure exact-receipt lookup by tuple, independent of whatever the range above found: a prior commit's manifest
    // row is proof enough on its own, even if the named row it refers to was since purged entirely.
    int published = 0;
    try (PreparedStatement s = c.prepareStatement(
        "SELECT root_id,child_count,payload,payload_hash,publication_key FROM " + TABLE + " WHERE " + KEY
            + " FOR UPDATE")) {
      for (long version : requested) {
        bindKey(s, account, container, name, version);
        try (ResultSet r = s.executeQuery()) {
          if (!r.next()) {
            continue;
          }
          byte[] storedRoot = r.getBytes(1);
          int storedCount = r.getInt(2);
          byte[] storedPayload = r.getBytes(3);
          byte[] storedHash = r.getBytes(4);
          byte[] storedKey = r.getBytes(5);
          boolean sameInputs = Arrays.equals(storedRoot, rootBytes.get(version)) && storedCount == pending.get(
              version).expected && Arrays.equals(storedPayload, bodies.get(version)) && MessageDigest.isEqual(
              storedHash, hash(account, container, name, version, storedCount, rootIds.get(version), storedPayload));
          if (!sameInputs) {
            throw new SQLDataException("Conflicting manifest for named version");
          }
          if (!MessageDigest.isEqual(storedKey, receipt)) {
            throw new SQLDataException("Manifest belongs to a different publication");
          }
          published++;
        }
      }
    }
    // Never report success for only part of the requested set: either every version already carries this exact
    // receipt, root and content, or none of them may.
    if (published == requested.size()) {
      return;
    }
    if (published != 0) {
      throw new SQLDataException("Mixed or oversized publication");
    }
    if (unrequestedLiveVersion) {
      throw new SQLDataException("Unrepresented or ineligible named version");
    }
    for (long version : requested) {
      if (!Boolean.TRUE.equals(liveMatch.get(version))) {
        throw new SQLDataException("Unrepresented or ineligible named version");
      }
    }

    try (PreparedStatement insert = c.prepareStatement(
        "INSERT INTO " + TABLE + " (" + HEADER_COLUMNS + ",payload,payload_hash) VALUES (?,?,?,?,?,?,?,?,?)")) {
      for (long version : requested) {
        PendingVersion p = pending.get(version);
        byte[] body = bodies.get(version);
        insert.setInt(1, account);
        insert.setInt(2, container);
        insert.setString(3, name);
        insert.setLong(4, version);
        insert.setBytes(5, rootBytes.get(version));
        insert.setInt(6, p.expected);
        insert.setBytes(7, receipt);
        insert.setBytes(8, body);
        insert.setBytes(9, hash(account, container, name, version, p.expected, rootIds.get(version), body));
        if (insert.executeUpdate() != 1) {
          throw new SQLDataException("Cannot publish sealed manifest");
        }
      }
    }

    try (PreparedStatement retire = c.prepareStatement(
        "UPDATE named_blobs_v2 SET retirement_fenced=TRUE,deleted_ts=NULL WHERE retirement_fenced=FALSE AND " + KEY);
        PreparedStatement fence = c.prepareStatement("SELECT 1 FROM named_blobs_v2 WHERE retirement_fenced=TRUE"
            + " AND deleted_ts<=UTC_TIMESTAMP(6) AND " + KEY + " FOR UPDATE")) {
      // History needs the TTL fence, not a fabricated cleanup manifest. All versions are still locked.
      history.addAll(requested);
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

  private static byte[] receipt(List<Long> orderedVersions) {
    return sha256().digest(orderedVersions.stream().map(String::valueOf).collect(Collectors.joining(","))
        .getBytes(StandardCharsets.US_ASCII));
  }

  /** Resolve a lost commit reply from immutable receipts on the authoritative writer, without reading the name. */
  boolean isPublished(short account, short container, String name, List<Long> versions) throws SQLException {
    List<Long> requested = canonicalVersions(versions);
    byte[] key = receipt(requested);
    String placeholders = String.join(",", Collections.nCopies(requested.size(), "?"));
    String sql = "SELECT blob_name,publication_key FROM " + TABLE
        + " WHERE account_id=? AND container_id=? AND blob_name=? AND version IN (" + placeholders + ")";
    try (Connection c = db.getConnection()) {
      requireCommittedReads(c);
      try (PreparedStatement s = c.prepareStatement(sql)) {
        s.setInt(1, account);
        s.setInt(2, container);
        s.setString(3, name);
        for (int i = 0; i < requested.size(); i++) {
          s.setLong(i + 4, requested.get(i));
        }
        try (ResultSet r = s.executeQuery()) {
          int count = 0;
          while (r.next()) {
            if (!name.equals(r.getString(1)) || !MessageDigest.isEqual(key, r.getBytes(2))) {
              return false;
            }
            count++;
          }
          return count == requested.size();
        }
      }
    }
  }

  /**
   * Recover a known publication by its exact tuple, without resolving the name or reading the original root. Every
   * row in this table is published by construction, so existence alone is sufficient.
   */
  Manifest getPublished(short account, short container, String name, long version) throws SQLException {
    String sql = "SELECT " + HEADER_COLUMNS + " FROM " + TABLE + " WHERE " + KEY;
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

  /** Read the full ordered child list for one published manifest row; the caller owns any inspection progress. */
  List<String> read(Manifest m) throws SQLException {
    byte[] body;
    try (Connection c = db.getConnection()) {
      requireCommittedReads(c);
      try (PreparedStatement s = c.prepareStatement("SELECT payload,payload_hash FROM " + TABLE + " WHERE " + KEY)) {
        bindKey(s, m.account, m.container, m.name, m.version);
        try (ResultSet r = s.executeQuery()) {
          if (!r.next()) {
            throw new SQLDataException("Missing published manifest: " + m.name + "/" + m.version);
          }
          byte[] payload = r.getBytes(1);
          byte[] payloadHash = r.getBytes(2);
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
        throw new SQLDataException("Invalid published child descriptor");
      }
      byte[] bytes = new byte[length];
      in.get(bytes);
      ids.add(Base64.getUrlEncoder().withoutPadding().encodeToString(bytes));
    }
    if (in.hasRemaining()) {
      throw new SQLDataException("Trailing bytes on published manifest");
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
