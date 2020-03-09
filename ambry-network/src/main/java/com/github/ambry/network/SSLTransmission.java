/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.network;

import com.github.ambry.commons.SSLFactory;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static javax.net.ssl.SSLEngineResult.*;


/**
 * Handles all the SSL related interactions. It is mainly responsible for establishing the handshake completely,
 * performing reads/writes and closing the transmission safely. This class also implements
 * ReadableByteChannel and WritableByteChannel to provide a way to encrypt and decrypt to/from a channel
 */
public class SSLTransmission extends Transmission implements ReadableByteChannel, WritableByteChannel {

  private static final Logger logger = LoggerFactory.getLogger(SSLTransmission.class);
  private final SSLEngine sslEngine;
  private HandshakeStatus handshakeStatus;
  private SSLEngineResult handshakeResult;
  private boolean handshakeComplete = false;
  private boolean closing = false;
  private ByteBuffer netReadBuffer;
  // buffer used to hold the encrypted data that is read from the network
  private ByteBuffer netWriteBuffer;
  // buffer used to hold the encrypted data to be sent over the network
  private ByteBuffer appReadBuffer;
  // buffer used to hold the decrypted data decrypted from networkReadBuffer
  private ByteBuffer emptyBuf = ByteBuffer.allocate(0);
  private long handshakeStartTime;

  public SSLTransmission(SSLFactory sslFactory, String connectionId, SocketChannel socketChannel, SelectionKey key,
      String remoteHost, int remotePort, Time time, NetworkMetrics metrics, SSLFactory.Mode mode, NetworkConfig config)
      throws IOException {
    super(connectionId, socketChannel, key, time, config, metrics);
    this.sslEngine = sslFactory.createSSLEngine(remoteHost, remotePort, mode);
    if (config.selectorUseDirectBuffers) {
      this.netReadBuffer = ByteBuffer.allocateDirect(netReadBufferSize());
      this.netWriteBuffer = ByteBuffer.allocateDirect(netWriteBufferSize());
      this.appReadBuffer = ByteBuffer.allocateDirect(appReadBufferSize());
    } else {
      this.netReadBuffer = ByteBuffer.allocate(netReadBufferSize());
      this.netWriteBuffer = ByteBuffer.allocate(netWriteBufferSize());
      this.appReadBuffer = ByteBuffer.allocate(appReadBufferSize());
    }
    startHandshake();
  }

  /**
   * starts sslEngine handshake process
   */
  private void startHandshake() throws IOException {
    //clear & set netRead & netWrite buffers
    netWriteBuffer.position(0);
    netWriteBuffer.limit(0);
    netReadBuffer.position(0);
    netReadBuffer.limit(0);
    handshakeComplete = false;
    closing = false;
    //initiate handshake
    handshakeStartTime = -1;
    sslEngine.beginHandshake();
    handshakeStatus = sslEngine.getHandshakeStatus();
  }

  /**
   * Returns the handshake status
   */
  @Override
  public boolean ready() {
    return handshakeComplete;
  }

  /**
   * Prepares the channel to accept read or write. We do handshake if not yet complete
   * @throws IOException
   */
  @Override
  public void prepare() throws IOException {
    if (handshakeStartTime == -1) {
      handshakeStartTime = time.milliseconds();
    }
    if (!ready()) {
      handshake();
    }
  }

  @Override
  public boolean isOpen() {
    return socketChannel.isOpen();
  }

  /**
   * Sends a SSL close message and closes socketChannel.
   */
  @Override
  public void close() {
    if (closing) {
      return;
    }
    closing = true;
    sslEngine.closeOutbound();
    try {
      if (socketChannel.isConnected()) {
        if (!flush(netWriteBuffer)) {
          throw new IOException("Remaining data in the network buffer, can't send SSL close message.");
        }
        //prep the buffer for the close message
        netWriteBuffer.clear();
        //perform the close, since we called sslEngine.closeOutbound
        SSLEngineResult handshake = sslEngine.wrap(emptyBuf, netWriteBuffer);
        //we should be in a close state
        if (handshake.getStatus() != Status.CLOSED) {
          throw new IOException("Invalid close state, will not send network data.");
        }
        netWriteBuffer.flip();
        flush(netWriteBuffer);
      }
    } catch (IOException ie) {
      metrics.selectorCloseSocketErrorCount.inc();
      logger.debug("Failed to send SSL close message ", ie);
    } finally {
      try {
        release();
        clearReceive();
        clearSend();
        clearBuffers();
        socketChannel.socket().close();
        socketChannel.close();
      } catch (IOException ie) {
        metrics.selectorCloseSocketErrorCount.inc();
        logger.debug("Failed to close socket", ie);
      } finally {
        key.attach(null);
        key.cancel();
      }
    }
  }

  /**
   * Reads available bytes from socket channel to `netReadBuffer`.
   * @return number of bytes read
   */
  private int readFromSocketChannel() throws IOException {
    return socketChannel.read(netReadBuffer);
  }

  /**
   * Flushes the buffer to the network
   * @param buf ByteBuffer
   * @return boolean true if the buffer has been emptied out, false otherwise
   * @throws IOException
   */
  private boolean flush(ByteBuffer buf) throws IOException {
    int remaining = buf.remaining();
    if (remaining > 0) {
      long startNs = SystemTime.getInstance().nanoseconds();
      int written = socketChannel.write(buf);
      logger.trace("Flushed {} bytes in {} ns", written, SystemTime.getInstance().nanoseconds() - startNs);
      return written >= remaining;
    }
    return true;
  }

  /**
   * Performs SSL handshake, non blocking.
   * Before application data (ambry protocols) can be sent client & ambry server must
   * perform ssl handshake.
   * During the handshake SSLEngine generates encrypted data  that will be transported over socketChannel.
   * Each SSLEngine operation generates SSLEngineResult, of which SSLEngineResult.handshakeStatus field is used to
   * determine what operation needs to occur to move handshake along.
   * A typical handshake might look like this.
   * +-------------+----------------------------------+-------------+
   * |  client     |  SSL/TLS message                 | HSStatus    |
   * +-------------+----------------------------------+-------------+
   * | wrap()      | ClientHello                      | NEED_UNWRAP |
   * | unwrap()    | ServerHello/Cert/ServerHelloDone | NEED_WRAP   |
   * | wrap()      | ClientKeyExchange                | NEED_WRAP   |
   * | wrap()      | ChangeCipherSpec                 | NEED_WRAP   |
   * | wrap()      | Finished                         | NEED_UNWRAP |
   * | unwrap()    | ChangeCipherSpec                 | NEED_UNWRAP |
   * | unwrap()    | Finished                         | FINISHED    |
   * +-------------+----------------------------------+-------------+
   *
   * @throws IOException
   */
  private void handshake() throws IOException {
    boolean read = key.isReadable();
    boolean write = key.isWritable();
    handshakeComplete = false;
    handshakeStatus = sslEngine.getHandshakeStatus();
    if (!flush(netWriteBuffer)) {
      key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
      return;
    }
    try {
      switch (handshakeStatus) {
        case NEED_TASK:
          logger.trace(
              "SSLHandshake NEED_TASK channelId {}, appReadBuffer pos {}, netReadBuffer pos {}, netWriteBuffer pos {}",
              getConnectionId(), appReadBuffer.position(), netReadBuffer.position(), netWriteBuffer.position());
          handshakeStatus = runDelegatedTasks();
          break;
        case NEED_WRAP:
          logger.trace(
              "SSLHandshake NEED_WRAP channelId {}, appReadBuffer pos {}, netReadBuffer pos {}, netWriteBuffer pos {}",
              getConnectionId(), appReadBuffer.position(), netReadBuffer.position(), netWriteBuffer.position());
          do {
            handshakeResult = handshakeWrap(write);
            if (handshakeResult.getStatus() == Status.BUFFER_OVERFLOW) {
              handleWrapOverflow();
            }
          } while (handshakeResult.getStatus() == Status.BUFFER_OVERFLOW);
          if (handshakeResult.getStatus() == Status.BUFFER_UNDERFLOW) {
            throw new IllegalStateException("Should not have received BUFFER_UNDERFLOW during handshake WRAP.");
          } else if (handshakeResult.getStatus() == Status.CLOSED) {
            throw new EOFException("SSL handshake status CLOSED during handshake WRAP");
          }
          logger.trace(
              "SSLHandshake NEED_WRAP channelId {}, handshakeResult {}, appReadBuffer pos {}, netReadBuffer pos {}, netWriteBuffer pos {}",
              getConnectionId(), handshakeResult, appReadBuffer.position(), netReadBuffer.position(),
              netWriteBuffer.position());
          //if handshake status is not NEED_UNWRAP or unable to flush netWriteBuffer contents
          //we will break here otherwise we can do need_unwrap in the same call.
          if (handshakeStatus != HandshakeStatus.NEED_UNWRAP || !flush(netWriteBuffer)) {
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            break;
          }
        case NEED_UNWRAP:
          logger.trace(
              "SSLHandshake NEED_UNWRAP channelId {}, appReadBuffer pos {}, netReadBuffer pos {}, netWriteBuffer pos {}",
              getConnectionId(), appReadBuffer.position(), netReadBuffer.position(), netWriteBuffer.position());
          do {
            handshakeResult = handshakeUnwrap(read);
            if (handshakeResult.getStatus() == Status.BUFFER_OVERFLOW) {
              handleUnwrapOverflow();
            }
          } while (handshakeResult.getStatus() == Status.BUFFER_OVERFLOW);
          if (handshakeResult.getStatus() == Status.BUFFER_UNDERFLOW) {
            handleUnwrapUnderflow();
          } else if (handshakeResult.getStatus() == Status.CLOSED) {
            throw new EOFException("SSL handshake status CLOSED during handshake UNWRAP");
          }
          logger.trace(
              "SSLHandshake NEED_UNWRAP channelId {}, handshakeResult {}, appReadBuffer pos {}, netReadBuffer pos {}, netWriteBuffer pos {}",
              getConnectionId(), handshakeResult, appReadBuffer.position(), netReadBuffer.position(),
              netWriteBuffer.position());

          //if handshakeStatus completed than fall-through to finished status.
          //after handshake is finished there is no data left to read/write in socketChannel.
          //so the selector won't invoke this channel if we don't go through the handshakeFinished here.
          if (handshakeStatus != HandshakeStatus.FINISHED) {
            if (handshakeStatus == HandshakeStatus.NEED_WRAP) {
              key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            } else if (handshakeStatus == HandshakeStatus.NEED_UNWRAP) {
              key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
            }
            break;
          }
        case FINISHED:
          handshakeFinished();
          break;
        case NOT_HANDSHAKING:
          handshakeFinished();
          break;
        default:
          throw new IllegalStateException(String.format("Unexpected status [%s]", handshakeStatus));
      }
    } catch (SSLException e) {
      handshakeFailure();
      throw e;
    }
  }

  /**
   * Executes the SSLEngine tasks needed.
   * @return HandshakeStatus
   */
  private HandshakeStatus runDelegatedTasks() {
    for (; ; ) {
      Runnable task = delegatedTask();
      if (task == null) {
        break;
      }
      task.run();
    }
    return sslEngine.getHandshakeStatus();
  }

  /**
   * Checks if the handshake status is finished
   * Sets the interestOps for the selectionKey.
   */
  private void handshakeFinished() throws IOException {
    // SSLEnginge.getHandshakeStatus is transient and it doesn't record FINISHED status properly.
    // It can move from FINISHED status to NOT_HANDSHAKING after the handshake is completed.
    // Hence we also need to check handshakeResult.getHandshakeStatus() if the handshake finished or not
    if (handshakeResult.getHandshakeStatus() == HandshakeStatus.FINISHED) {
      //we are complete if we have delivered the last package
      handshakeComplete = !netWriteBuffer.hasRemaining();
      //remove OP_WRITE if we are complete, otherwise we still have data to write
      if (!handshakeComplete) {
        key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
      } else {
        key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
        metrics.sslHandshakeCount.inc();
        metrics.sslHandshakeTime.update(time.milliseconds() - handshakeStartTime);
      }

      logger.trace(
          "SSLHandshake FINISHED channelId {}, appReadBuffer pos {}, netReadBuffer pos {}, netWriteBuffer pos {} ",
          getConnectionId(), appReadBuffer.position(), netReadBuffer.position(), netWriteBuffer.position());
    } else {
      throw new IOException("NOT_HANDSHAKING during handshake");
    }
  }

  /**
   * Performs the WRAP function
   * @param doWrite boolean
   * @return SSLEngineResult
   * @throws IOException
   */
  private SSLEngineResult handshakeWrap(boolean doWrite) throws IOException {
    logger.trace("SSLHandshake handshakeWrap {}", getConnectionId());
    if (netWriteBuffer.hasRemaining()) {
      throw new IllegalStateException("handshakeWrap called with netWriteBuffer not empty");
    }
    //this should never be called with a network buffer that contains data
    //so we can clear it here.
    netWriteBuffer.clear();
    SSLEngineResult result = sslEngine.wrap(emptyBuf, netWriteBuffer);
    //prepare the results to be written
    netWriteBuffer.flip();
    handshakeStatus = result.getHandshakeStatus();
    if (result.getStatus() == Status.OK && result.getHandshakeStatus() == HandshakeStatus.NEED_TASK) {
      handshakeStatus = runDelegatedTasks();
    }
    if (doWrite) {
      flush(netWriteBuffer);
    }
    return result;
  }

  /**
   * Perform handshake unwrap
   * @param doRead boolean
   * @return SSLEngineResult
   * @throws IOException
   */
  private SSLEngineResult handshakeUnwrap(boolean doRead) throws IOException {
    logger.trace("SSLHandshake handshakeUnwrap {}", getConnectionId());
    int read;
    if (doRead) {
      read = readFromSocketChannel();
      if (read == -1) {
        throw new EOFException("EOF during handshake.");
      }
    }
    SSLEngineResult result;
    boolean cont;
    do {
      //prepare the buffer with the incoming data
      netReadBuffer.flip();
      result = sslEngine.unwrap(netReadBuffer, appReadBuffer);
      netReadBuffer.compact();
      handshakeStatus = result.getHandshakeStatus();
      if (result.getStatus() == Status.OK && result.getHandshakeStatus() == HandshakeStatus.NEED_TASK) {
        handshakeStatus = runDelegatedTasks();
      }
      cont = result.getStatus() == Status.OK && handshakeStatus == HandshakeStatus.NEED_UNWRAP;
      logger.trace("SSLHandshake handshakeUnwrap: handshakeStatus {}", handshakeStatus);
    } while (netReadBuffer.position() != 0 && cont);

    return result;
  }

  @Override
  public boolean read() throws IOException {
    if (!hasReceive()) {
      initializeNetworkReceive();
      metrics.transmissionRoundTripTime.update(time.milliseconds() - sendCompleteTime);
    }
    long startTimeMs = time.milliseconds();
    long bytesRead = networkReceive.getReceivedBytes().readFrom(this);
    long readTimeMs = time.milliseconds() - startTimeMs;
    logger.trace("Bytes read {} from {} using key {} Time: {} Ms.", bytesRead,
        socketChannel.socket().getRemoteSocketAddress(), getConnectionId(), readTimeMs);
    if (bytesRead > 0) {
      metrics.transmissionReceiveTime.update(readTimeMs);
      metrics.transmissionReceiveSize.update(bytesRead);
    }
    return networkReceive.getReceivedBytes().isReadComplete();
  }

  /**
   * Reads a sequence of bytes from this channel into the given buffer. Reads as much as possible
   * until either the dst buffer is full or there is no more data in the socket.
   *
   * @param dst The buffer into which bytes are to be transferred
   * @return The number of bytes read, possible zero or -1 if the channel has reached end-of-stream
   *         and no more data is available
   * @throws IOException if some other I/O error occurs
   */
  @Override
  public int read(ByteBuffer dst) throws IOException {
    if (closing) {
      return -1;
    } else if (!handshakeComplete) {
      return 0;
    }

    //if we have unread decrypted data in appReadBuffer read that into dst buffer.
    int read = 0;
    if (appReadBuffer.position() > 0) {
      read = readFromAppBuffer(dst);
    }
    boolean isClosed = false;
    // Each loop reads at most once from the socket.
    while (dst.remaining() > 0) {
      int netread = 0;
      netReadBuffer = Utils.ensureCapacity(netReadBuffer, netReadBufferSize());
      if (netReadBuffer.remaining() > 0) {
        netread = readFromSocketChannel();
      }

      while (netReadBuffer.position() > 0) {
        netReadBuffer.flip();
        long startTimeMs = System.currentTimeMillis();
        SSLEngineResult unwrapResult = sslEngine.unwrap(netReadBuffer, appReadBuffer);
        long decryptionTimeMs = System.currentTimeMillis() - startTimeMs;
        logger.trace("SSL decryption time: {} ms for {} bytes", decryptionTimeMs, unwrapResult.bytesProduced());
        if (unwrapResult.bytesProduced() > 0) {
          metrics.sslDecryptionTimeInUsPerKB.mark(
              TimeUnit.MILLISECONDS.toMicros(decryptionTimeMs) * 1024 / unwrapResult.bytesProduced());
        }
        netReadBuffer.compact();
        // handle ssl renegotiation.
        if (unwrapResult.getHandshakeStatus() != HandshakeStatus.NOT_HANDSHAKING
            && unwrapResult.getStatus() == Status.OK) {
          logger.trace(
              "SSLChannel Read begin renegotiation getConnectionId() {}, appReadBuffer pos {}, netReadBuffer pos {}, netWriteBuffer pos {}",
              getConnectionId(), appReadBuffer.position(), netReadBuffer.position(), netWriteBuffer.position());
          handshake();
          metrics.sslRenegotiationCount.inc();
          break;
        }

        if (unwrapResult.getStatus() == Status.OK) {
          read += readFromAppBuffer(dst);
        } else if (unwrapResult.getStatus() == Status.BUFFER_OVERFLOW) {
          handleUnwrapOverflow();

          // appReadBuffer will extended up to currentApplicationBufferSize.
          // We need to read the existing content into dst before we can do unwrap again.
          // If there is no space in dst we can break here.
          if (dst.hasRemaining()) {
            read += readFromAppBuffer(dst);
          } else {
            break;
          }
        } else if (unwrapResult.getStatus() == Status.BUFFER_UNDERFLOW) {
          handleUnwrapUnderflow();
          break;
        } else if (unwrapResult.getStatus() == Status.CLOSED) {
          // If data has been read and unwrapped, return the data. Close will be handled on the next poll.
          if (appReadBuffer.position() == 0 && read == 0) {
            throw new EOFException();
          } else {
            isClosed = true;
            break;
          }
        }
      }
      if (read == 0 && netread < 0) {
        throw new EOFException("EOF during read");
      }
      if (netread <= 0 || isClosed) {
        break;
      }
    }
    // If data has been read and unwrapped, return the data even if end-of-stream, channel will be closed
    // on a subsequent poll.
    return read;
  }

  @Override
  public boolean write() throws IOException {
    Send send = networkSend.getPayload();
    if (send == null) {
      throw new IllegalStateException("Registered for write interest but no response attached to key.");
    }
    if (!closing && handshakeComplete) {
      if (!flush(netWriteBuffer)) {
        return false;
      }
    }
    if (networkSend.maySetSendStartTimeInMs()) {
      metrics.transmissionSendPendingTime.update(time.milliseconds() - networkSend.getSendCreateTimeInMs());
    }
    long startTimeMs = time.milliseconds();
    long bytesWritten = send.writeTo(this);
    long writeTimeMs = time.milliseconds() - startTimeMs;
    logger.trace("Bytes written {} to {} using key {} Time: {} ms", bytesWritten,
        socketChannel.socket().getRemoteSocketAddress(), getConnectionId(), writeTimeMs);
    if (bytesWritten > 0) {
      metrics.transmissionSendTime.update(writeTimeMs);
      metrics.transmissionSendSize.update(bytesWritten);
    }
    return (send.isSendComplete() && netWriteBuffer.remaining() == 0);
  }

  /**
   * Writes a sequence of bytes to this channel from the given buffer.
   *
   * @param src The buffer from which bytes are to be retrieved
   * @return The number of bytes decrypted and written to netWriteBuffer. No guarantee that data in the temporary
   * buffer will be completely written to the underlying channel right away. So the caller has to make sure to check
   * the remaining bytes in the netWriteBuffer apart from checking the remaining bytes in src bytebuffer. This method
   * is called from write() in the same class
   * @throws IOException If some other I/O error occurs
   */
  @Override
  public int write(ByteBuffer src) throws IOException {
    if (closing) {
      throw new IllegalStateException("Channel is in closing state");
    } else if (!handshakeComplete) {
      return 0;
    } else if (!flush(netWriteBuffer)) {
      return 0;
    }

    int written = 0;
    while (src.remaining() != 0) {
      netWriteBuffer.clear();
      long startTimeNs = SystemTime.getInstance().nanoseconds();
      SSLEngineResult wrapResult = sslEngine.wrap(src, netWriteBuffer);
      long encryptionTimeNs = SystemTime.getInstance().nanoseconds() - startTimeNs;
      logger.trace("SSL encryption time: {} ns for {} bytes", encryptionTimeNs, wrapResult.bytesConsumed());
      if (wrapResult.bytesConsumed() > 0) {
        metrics.sslEncryptionTimeInUsPerKB.mark(encryptionTimeNs / wrapResult.bytesConsumed());
      }
      netWriteBuffer.flip();
      //handle ssl renegotiation
      if (wrapResult.getHandshakeStatus() != HandshakeStatus.NOT_HANDSHAKING && wrapResult.getStatus() == Status.OK) {
        handshake();
        metrics.sslRenegotiationCount.inc();
        break;
      }

      if (wrapResult.getStatus() == SSLEngineResult.Status.OK) {
        written += wrapResult.bytesConsumed();
        if (!flush(netWriteBuffer)) {
          // break if socketChannel can't accept all data in netWriteBuffer
          break;
        }
        // otherwise, we are safe to clear the buffer for next iteration.
      } else if (wrapResult.getStatus() == Status.BUFFER_OVERFLOW) {
        handleWrapOverflow();
      } else if (wrapResult.getStatus() == Status.BUFFER_UNDERFLOW) {
        throw new IllegalStateException("SSL BUFFER_UNDERFLOW during write");
      } else if (wrapResult.getStatus() == Status.CLOSED) {
        throw new EOFException();
      }
    }
    return written;
  }

  /**
   * returns delegatedTask for the SSLEngine.
   */
  private Runnable delegatedTask() {
    return sslEngine.getDelegatedTask();
  }

  /**
   * transfers appReadBuffer contents (decrypted data) into dst bytebuffer
   * @param dst ByteBuffer
   */
  private int readFromAppBuffer(ByteBuffer dst) {
    appReadBuffer.flip();
    int remaining = Math.min(appReadBuffer.remaining(), dst.remaining());
    if (remaining > 0) {
      int limit = appReadBuffer.limit();
      appReadBuffer.limit(appReadBuffer.position() + remaining);
      dst.put(appReadBuffer);
      appReadBuffer.limit(limit);
    }
    appReadBuffer.compact();
    return remaining;
  }

  /**
   * Exposed to support buffer resizing unit tests.
   * @return the buffer used for reads from network.
   */
  ByteBuffer netReadBuffer() {
    return netReadBuffer;
  }

  /**
   * Exposed to support buffer resizing unit tests.
   * @return the recommended size for {@link #netReadBuffer}.
   */
  int netReadBufferSize() {
    return sslEngine.getSession().getPacketBufferSize();
  }

  /**
   * Exposed to support buffer resizing unit tests.
   * @return the recommended size for {@link #netWriteBuffer}.
   */
  int netWriteBufferSize() {
    return sslEngine.getSession().getPacketBufferSize();
  }

  /**
   * Exposed to support buffer resizing unit tests.
   * @return the recommended size for {@link #appReadBuffer}.
   */
  int appReadBufferSize() {
    return sslEngine.getSession().getApplicationBufferSize();
  }

  /**
   * Increase the size of {@link #appReadBuffer}. To be called when an unwrap call returns
   * {@link Status#BUFFER_OVERFLOW}.
   */
  private void handleUnwrapOverflow() {
    int currentAppReadBufferSize = appReadBufferSize();
    appReadBuffer = Utils.ensureCapacity(appReadBuffer, currentAppReadBufferSize);
    if (appReadBuffer.position() > currentAppReadBufferSize) {
      throw new IllegalStateException(
          "Buffer overflow when available data size (" + appReadBuffer.position() + ") > application buffer size ("
              + currentAppReadBufferSize + ")");
    }
  }

  /**
   * Increase the size of {@link #netReadBuffer}. To be called when an unwrap call returns
   * {@link Status#BUFFER_UNDERFLOW}.
   */
  private void handleUnwrapUnderflow() {
    int currentNetReadBufferSize = netReadBufferSize();
    netReadBuffer = Utils.ensureCapacity(netReadBuffer, currentNetReadBufferSize);
    if (netReadBuffer.position() >= currentNetReadBufferSize) {
      throw new IllegalStateException(
          "Buffer underflow when available data size (" + netReadBuffer.position() + ") >= packet buffer size ("
              + currentNetReadBufferSize + ")");
    }
  }

  /**
   * Increase the size of {@link #netWriteBuffer}. To be called when a wrap call returns {@link Status#BUFFER_OVERFLOW}.
   */
  private void handleWrapOverflow() {
    int currentNetWriteBufferSize = netWriteBufferSize();
    netWriteBuffer.compact();
    netWriteBuffer = Utils.ensureCapacity(netWriteBuffer, currentNetWriteBufferSize);
    netWriteBuffer.flip();
    if (netWriteBuffer.limit() >= currentNetWriteBufferSize) {
      throw new IllegalStateException(
          "Buffer overflow when available data size (" + netWriteBuffer.limit() + ") >= network buffer size ("
              + currentNetWriteBufferSize + ")");
    }
  }

  private void handshakeFailure() {
    //Release all resources such as internal buffers that SSLEngine is managing
    sslEngine.closeOutbound();
    metrics.sslHandshakeErrorCount.inc();
    try {
      sslEngine.closeInbound();
    } catch (SSLException e) {
      logger.debug("SSLEngine.closeInBound() raised an exception.", e);
    }
  }

  /**
   * Nullify buffers used for I/O with {@link SSLEngine}.
   */
  private void clearBuffers() {
    appReadBuffer = null;
    netReadBuffer = null;
    netWriteBuffer = null;
  }
}
