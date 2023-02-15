/**
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.store;

import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.replication.FindToken;
import com.github.ambry.utils.CrcInputStream;
import com.github.ambry.utils.CrcOutputStream;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link RemoteTokenTracker} tracks tokens from all peer replicas and updates them when handling metadata request from
 * peer node.
 * We persist the peer replicas' token and the timestamp when the token is updated to a file.
 */
public class RemoteTokenTracker implements Closeable {
  private static String REMOTE_TOKEN_FILE_NAME = "remoteTokenTracker";

  private static final String DELIMITER = ":";
  private final ReplicaId localReplica;
  // The key of peerReplicaAndToken is a string containing hostname and path of peer replica.
  // format: Hostname:mountpoint, for example: localhost:/mnt/u001/p1
  // The value is a pair of timestamp and FindToken. timestamp is the time when the token is updated.
  private ConcurrentMap<String, Pair<Long, FindToken>> peerReplicaAndToken;

  private final ScheduledExecutorService scheduler;
  private final RemoteTokenPersistor persistor;
  private ScheduledFuture<?> persistorFuture = null;
  private final RemoteReplicaTokenSerde tokenSerde;

  private static final Logger logger = LoggerFactory.getLogger(RemoteTokenTracker.class);

  public RemoteTokenTracker(ReplicaId localReplica, ScheduledExecutorService scheduler,
      StoreKeyFactory storeKeyFactory) {
    this.localReplica = localReplica;
    this.scheduler = scheduler;
    this.tokenSerde = new RemoteReplicaTokenSerde(storeKeyFactory);
    this.persistor = new RemoteTokenTracker.RemoteTokenPersistor();
    // get the remote token from the persistent file.
    this.peerReplicaAndToken = persistor.retrieve();
    // then update with the latest peers.
    refreshPeerReplicaTokens();
  }

  /**
   * Update peer replica token within this tracker.
   * @param token the most recent token from peer replica.
   * @param remoteHostName the hostname of peer node (where the peer replica resides).
   * @param remoteReplicaPath the path of peer replica on remote peer node.
   */
  void updateTokenFromPeerReplica(FindToken token, String remoteHostName, String remoteReplicaPath) {
    // this already handles newly added peer replica (i.e. move replica)
    Pair<Long, FindToken> pair = new Pair<>(System.currentTimeMillis(), token);
    peerReplicaAndToken.put(remoteHostName + DELIMITER + remoteReplicaPath, pair);
  }

  /**
   * Refresh the peerReplicaAndToken map in case peer replica has changed (i.e. new replica is added and old replica is removed)
   */
  void refreshPeerReplicaTokens() {
    ConcurrentMap<String, Pair<Long, FindToken>> newPeerReplicaAndToken = new ConcurrentHashMap<>();
    // this should remove peer replica that no longer exists (i.e original replica is moved to other node)
    localReplica.getPeerReplicaIds().forEach(r -> {
      String hostnameAndPath = r.getDataNodeId().getHostname() + DELIMITER + r.getReplicaPath();
      newPeerReplicaAndToken.put(hostnameAndPath, peerReplicaAndToken.getOrDefault(hostnameAndPath,
          new Pair<>(System.currentTimeMillis(), new StoreFindToken())));
    });
    // atomic switch
    peerReplicaAndToken = newPeerReplicaAndToken;
  }

  /**
   * @return a snapshot of peer replica to token map.
   */
  Map<String, Pair<Long, FindToken>> getPeerReplicaAndToken() {
    return new HashMap<>(peerReplicaAndToken);
  }

  /**
   * @return the file path which persists the remote token.
   */
  protected String getPersistFilePath() {
    return localReplica.getReplicaPath() + File.separator + REMOTE_TOKEN_FILE_NAME;
  }

  /**
   * Provide a way to explicitly persist the token besides the background persistor
   */
  protected boolean persistToken() {
    try {
      persistor.persist();
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  /**
   * Disable this {@link RemoteTokenTracker} and cancel the scheduled persistor.
   */
  @Override
  public void close() {
    if (persistorFuture != null) {
      // cancel the future.
      persistorFuture.cancel(false);
      // refresh the token one more time.
      persistToken();
    }
  }

  /**
   * Start the background persistor.
   */
  public void start(int persistIntervalInSeconds) {
    if (persistIntervalInSeconds > 0) {
      if (this.scheduler != null) {
        persistorFuture = this.scheduler.scheduleAtFixedRate(persistor, new Random().nextInt(Time.SecsPerMin),
            persistIntervalInSeconds, TimeUnit.SECONDS);
        logger.info("flush peer's remote token every {} seconds", persistIntervalInSeconds);
      } else {
        logger.error("scheduler is null, couldn't persist peer's remote token.");
      }
    }
  }

  /**
   * Runner that persist the remote peers and their tokens.
   */
  private class RemoteTokenPersistor implements Runnable {

    @Override
    public void run() {
      try {
        persist();
      } catch (IOException e) {
        logger.debug("Failed to persist the token file", e);
      }
    }

    /**
     * Persist the remote peers and their tokens.
     */
    protected synchronized void persist() throws IOException {
      String localReplicaMountPath = localReplica.getReplicaPath();
      File temp = new File(localReplicaMountPath, REMOTE_TOKEN_FILE_NAME + ".tmp");
      File actual = new File(localReplicaMountPath, REMOTE_TOKEN_FILE_NAME);
      Map<String, Pair<Long, FindToken>> peerTokenSnapshot = getPeerReplicaAndToken();
      try {
        FileOutputStream fileStream = new FileOutputStream(temp);
        tokenSerde.serializeTokens(peerTokenSnapshot, fileStream);
        // swap temp file with the original file
        temp.renameTo(actual);
        logger.debug("Completed writing remote tokens to file {}", actual.getAbsolutePath());
      } catch (IOException e) {
        logger.error("IO error while persisting tokens to disk {}", temp.getAbsoluteFile());
        throw e;
      }
    }

    /**
     * Retrieve the remote peers and their tokens.
     * @return the hash map from the replicas to the tokens
     */
    public ConcurrentMap<String, Pair<Long, FindToken>> retrieve() {
      String localReplicaMountPath = localReplica.getReplicaPath();
      File replicaTokenFile = new File(localReplicaMountPath, REMOTE_TOKEN_FILE_NAME);
      if (replicaTokenFile.exists()) {
        try {
          FileInputStream fileInputStream = new FileInputStream(replicaTokenFile);
          return tokenSerde.deserializeTokens(fileInputStream);
        } catch (IOException e) {
          logger.error("IO error while retrieving tokens from disk {}", replicaTokenFile.getAbsolutePath(), e);
          return new ConcurrentHashMap<>();
        }
      } else {
        return new ConcurrentHashMap<>();
      }
    }
  }

  /**
   * Class to serialize and deserialize replica tokens
   */
  private static class RemoteReplicaTokenSerde {
    private static final short Crc_Size = 8;
    private static final short VERSION_0 = 0;
    private static final short CURRENT_VERSION = VERSION_0;
    private final StoreKeyFactory keyFactory;

    public RemoteReplicaTokenSerde(StoreKeyFactory keyFactory) {
      this.keyFactory = keyFactory;
    }

    /**
     * Serialize the remote tokens to the file
     * @param peerReplicaAndToken the mapping from the replicas to the remote tokens
     * @param outputStream the file output stream to write to
     */
    public void serializeTokens(Map<String, Pair<Long, FindToken>> peerReplicaAndToken, OutputStream outputStream)
        throws IOException {
      CrcOutputStream crcOutputStream = new CrcOutputStream(outputStream);
      DataOutputStream writer = new DataOutputStream(crcOutputStream);
      try {
        // write the current version
        writer.writeShort(CURRENT_VERSION);
        for (Map.Entry<String, Pair<Long, FindToken>> peerToken : peerReplicaAndToken.entrySet()) {
          // write the peer key: hostname:mountpoint
          writer.writeInt(peerToken.getKey().getBytes().length);
          writer.write(peerToken.getKey().getBytes());
          // write the timestamp
          writer.writeLong(peerToken.getValue().getFirst());
          // Write the remote token
          writer.write(peerToken.getValue().getSecond().toBytes());
        }

        long crcValue = crcOutputStream.getValue();
        writer.writeLong(crcValue);
      } catch (IOException e) {
        logger.error("IO error while serializing remote peer tokens", e);
        throw e;
      } finally {
        if (outputStream instanceof FileOutputStream) {
          // flush and overwrite file
          ((FileOutputStream) outputStream).getChannel().force(true);
        }
        writer.close();
      }
    }

    /**
     * Deserialize the remote tokens
     * @param inputStream the input stream from the persistent file
     * @return the mapping from replicas to remote tokens
     */
    public ConcurrentMap<String, Pair<Long, FindToken>> deserializeTokens(InputStream inputStream) throws IOException {
      CrcInputStream crcStream = new CrcInputStream(inputStream);
      DataInputStream stream = new DataInputStream(crcStream);
      ConcurrentMap<String, Pair<Long, FindToken>> peerTokens = new ConcurrentHashMap<>();
      try {
        short version = stream.readShort();
        if (version != VERSION_0) {
          logger.error("Invalid version found during remote peer token deserialization: " + version);
          return new ConcurrentHashMap<>();
        }
        while (stream.available() > Crc_Size) {
          // read the peer key: hostname:mountpoint
          String peerKey = Utils.readIntString(stream);
          // read the timestamp
          long timestamp = stream.readLong();
          // read remote replica token
          FindToken token = StoreFindToken.fromBytes(stream, keyFactory);
          peerTokens.put(peerKey, new Pair<>(timestamp, token));
          logger.info("Retrieve remote token " + peerKey + " : " + timestamp + " " + token.toString());
        }

        long computedCrc = crcStream.getValue();
        long readCrc = stream.readLong();
        if (computedCrc != readCrc) {
          logger.error("Crc mismatch during peer token deserialization, computed " + computedCrc + ", read " + readCrc);
          return new ConcurrentHashMap<>();
        }
        return peerTokens;
      } catch (IOException e) {
        logger.error("IO error deserializing remote peer tokens", e);
        return new ConcurrentHashMap<>();
      } finally {
        stream.close();
      }
    }
  }
}
