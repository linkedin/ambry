package com.github.ambry.network;

import com.github.ambry.utils.Time;
import java.io.IOException;
import java.nio.channels.SocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AmbryChannel {
  private static final Logger logger = LoggerFactory.getLogger(AmbryChannel.class);
  private final String connectionId;
  private Transmission transmission;
  private NetworkReceive networkReceive;
  private NetworkSend networkSend;
  private Time time;

  public AmbryChannel(String connectionId, Transmission transmission, Time time) throws  IOException{
    this.connectionId = connectionId;
    this.transmission = transmission;
    this.time = time;
  }

  public void close() throws IOException {
    clearReceive();
    clearSend();
    transmission.close();
  }

  /**
   * Does handshake of transportLayer and Authentication using configured authenticator
   */
  public void prepare() throws IOException {
    transmission.prepare();
  }

  public void finishConnect() throws IOException {
    transmission.finishConnect();
  }

  public boolean isConnected() {
    return transmission.isConnected();
  }

  public String getConnectionId() {
    return connectionId;
  }

  public boolean ready() {
    return transmission.ready();
  }

  public boolean hasNetworkSend() {
    return networkSend != null;
  }

  protected void clearSend() {
    networkSend = null;
  }

  protected NetworkSend getNetworkSend() {
    return this.networkSend;
  }

  public void setNetworkSend(NetworkSend networkSend) {
    if (hasNetworkSend()) {
      throw new IllegalStateException(
          "Attempt to begin a networkSend operation with prior networkSend operation still in progress.");
    }
    this.networkSend = networkSend;
    transmission.setNetworkSend(networkSend);
  }

  public long read() throws IOException {
    long x = transmission.read();
    return x;
  }

  public boolean write() throws IOException {
    if (networkSend == null) {
      throw new IllegalStateException("Registered for write interest but no response attached to key.");
    }
    transmission.write();
    logger
        .trace("Bytes written to {} using key {}", transmission.getSocketChannel().socket().getRemoteSocketAddress(), getConnectionId());
    return networkSend.getPayload().isSendComplete();
  }

  protected boolean hasReceive() {
    return networkReceive != null;
  }

  protected void clearReceive() {
    networkReceive = null;
  }

  protected NetworkReceive getNetworkReceive() {
    return this.networkReceive;
  }

  SocketChannel getSocketChannel(){
    return transmission.getSocketChannel();
  }
}