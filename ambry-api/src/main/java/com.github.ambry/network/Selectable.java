package com.github.ambry.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;


/**
 * An interface for asynchronous, multi-channel network I/O
 */
public interface Selectable {

  /**
   * Begin establishing a socket connection to the given address identified by the given address
   * @param address The address to connect to
   * @param sendBufferSize The send buffer for the socket
   * @param receiveBufferSize The receive buffer for the socket
   * @return The id for the connection that was created
   * @throws java.io.IOException If we cannot begin connecting
   */
  public String connect(InetSocketAddress address, int sendBufferSize, int receiveBufferSize)
      throws IOException;

  /**
   * Begin disconnecting the connection identified by the given connection id
   */
  public void disconnect(String connectionId);

  /**
   * Wakeup this selector if it is blocked on I/O
   */
  public void wakeup();

  /**
   * Close a connection by given connection id
   * @param connectionId
   */
  public void close(String connectionId);

  /**
   * Close this selector
   */
  public void close();

  /**
   * Firstly initiate any sends provided, and then make progress on any other I/O operations in-flight (connections,
   * disconnections, existing sends, and receives)
   * @param timeoutMs The amount of time to block if there is nothing to do in ms
   * @param sends The new sends to initiate
   * @throws IOException
   */
  public void poll(long timeoutMs, List<NetworkSend> sends)
      throws IOException;

  /**
   * Make progress on any I/O operations in-flight (connections, disconnections, existing sends, and receives)
   * @param timeoutMs The amount of time to block if there is nothing to do in ms
   * @throws IOException
   */
  public void poll(long timeoutMs)
      throws IOException;

  /**
   * The list of sends that completed on the last {@link #poll(long, List) poll()} call.
   */
  public List<NetworkSend> completedSends();

  /**
   * The list of receives that completed on the last {@link #poll(long, List) poll()} call.
   */
  public List<NetworkReceive> completedReceives();

  /**
   * The list of connections that finished disconnecting on the last {@link #poll(long, List) poll()}
   * call.
   */
  public List<String> disconnected();

  /**
   * The list of connections that completed their connection on the last {@link #poll(long, List) poll()}
   * call.
   */
  public List<String> connected();
}
