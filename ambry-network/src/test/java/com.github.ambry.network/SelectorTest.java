package com.github.ambry.network;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.utils.SystemTime;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/**
 * A set of tests for the selector. These use a test harness that runs a simple socket server that echos back responses.
 */
public class SelectorTest {

  private static final List<NetworkSend> EMPTY = new ArrayList<NetworkSend>();
  private static final int BUFFER_SIZE = 4 * 1024;

  private EchoServer server;
  private Selectable selector;

  @Before
  public void setup()
      throws Exception {
    this.server = new EchoServer();
    this.server.start();
    this.selector = new Selector(new NetworkMetrics(new SocketRequestResponseChannel(1, 2), new MetricRegistry()),
        SystemTime.getInstance());
  }

  @After
  public void teardown()
      throws Exception {
    this.selector.close();
    this.server.close();
  }

  /**
   * Validate that when the server disconnects, a client send ends up with that node in the disconnected list.
   */
  @Test
  public void testServerDisconnect()
      throws Exception {
    long node = 0;

    // connect and do a simple request
    node = blockingConnect();
    assertEquals("hello", blockingRequest(node, "hello"));

    // disconnect
    this.server.closeConnections();
    while (!selector.disconnected().contains(node)) {
      selector.poll(1000L, EMPTY);
    }

    // reconnect and do another request
    node = blockingConnect();
    assertEquals("hello", blockingRequest(node, "hello"));
  }

  /**
   * Validate that the client can intentionally disconnect and reconnect
   */
  @Test
  public void testClientDisconnect()
      throws Exception {
    long node = 0;
    node = blockingConnect();
    selector.disconnect(node);
    selector.poll(10, asList(createSend(node, "hello1")));
    assertEquals("Request should not have succeeded", 0, selector.completedSends().size());
    assertEquals("There should be a disconnect", 1, selector.disconnected().size());
    assertTrue("The disconnect should be from our node", selector.disconnected().contains(node));
    node = blockingConnect();
    assertEquals("hello2", blockingRequest(node, "hello2"));
  }

  /**
   * Sending a request with one already in flight should result in an exception
   */
  @Test(expected = IllegalStateException.class)
  public void testCantSendWithInProgress()
      throws Exception {
    long node = 0;
    node = blockingConnect();
    selector.poll(1000L, asList(createSend(node, "test1"), createSend(node, "test2")));
  }

  /**
   * Sending a request to a node without an existing connection should result in an exception
   */
  @Test(expected = IllegalStateException.class)
  public void testCantSendWithoutConnecting()
      throws Exception {
    selector.poll(1000L, asList(createSend(0, "test")));
  }

  /**
   * Sending a request to a node with a bad hostname should result in an exception during connect
   */
  @Test(expected = IOException.class)
  public void testNoRouteToHost()
      throws Exception {
    long id = selector.connect(new InetSocketAddress("asdf.asdf.dsc", server.port), BUFFER_SIZE, BUFFER_SIZE);
  }

  /**
   * Sending a request to a node not listening on that port should result in disconnection
   */
  @Test
  public void testConnectionRefused()
      throws Exception {
    long node = 0;
    node = selector.connect(new InetSocketAddress("localhost", 6668), BUFFER_SIZE, BUFFER_SIZE);
    while (selector.disconnected().contains(node)) {
      selector.poll(1000L, EMPTY);
    }
  }

  /**
   * Send multiple requests to several connections in parallel. Validate that responses are received in the order that
   * requests were sent.
   */
  @Test
  public void testNormalOperation()
      throws Exception {
    int conns = 5;
    int reqs = 500;

    // create connections
    InetSocketAddress addr = new InetSocketAddress("localhost", server.port);
    for (int i = 0; i < conns; i++) {
      selector.connect(addr, BUFFER_SIZE, BUFFER_SIZE);
    }

    // send echo requests and receive responses
    int[] requests = new int[conns];
    int[] responses = new int[conns];
    int responseCount = 0;
    List<NetworkSend> sends = new ArrayList<NetworkSend>();
    for (int i = 0; i < conns; i++) {
      sends.add(createSend(i, i + "-" + 0));
    }

    // loop until we complete all requests
    while (responseCount < conns * reqs) {
      // do the i/o
      selector.poll(0L, sends);

      assertEquals("No disconnects should have occurred.", 0, selector.disconnected().size());

      // handle any responses we may have gotten
      for (NetworkReceive receive : selector.completedReceives()) {
        String[] pieces = asString(receive).split("-");
        assertEquals("Should be in the form 'conn-counter'", 2, pieces.length);
        assertEquals("Check the source", receive.getConnectionId(), Integer.parseInt(pieces[0]));
        assertEquals("Check that the receive has kindly been rewound", 0,
            receive.getReceivedBytes().getPayload().position());
        assertEquals("Check the request counter", responses[(int) receive.getConnectionId()],
            Integer.parseInt(pieces[1]));
        responses[(int) receive.getConnectionId()]++; // increment the expected counter
        responseCount++;
      }

      // prepare new sends for the next round
      sends.clear();
      for (NetworkSend send : selector.completedSends()) {
        long dest = send.getConnectionId();
        requests[(int) dest]++;
        if (requests[(int) dest] < reqs) {
          sends.add(createSend(dest, dest + "-" + requests[(int) dest]));
        }
      }
    }
  }

  /**
   * Validate that we can send and receive a message larger than the receive and send buffer size
   */
  @Test
  public void testSendLargeRequest()
      throws Exception {
    long node = blockingConnect();
    String big = randomString(10 * BUFFER_SIZE, new Random());
    assertEquals(big, blockingRequest(node, big));
  }

  /**
   * Test sending an empty string
   */
  @Test
  public void testEmptyRequest()
      throws Exception {
    long node = 0;
    node = blockingConnect();
    assertEquals("", blockingRequest(node, ""));
  }

  private String blockingRequest(long node, String s)
      throws IOException {
    selector.poll(1000L, asList(createSend(node, s)));
    while (true) {
      selector.poll(1000L, EMPTY);
      for (NetworkReceive receive : selector.completedReceives()) {
        if (receive.getConnectionId() == node) {
          return asString(receive);
        }
      }
    }
  }

  /* connect and wait for the connection to complete */
  private long blockingConnect()
      throws IOException {
    long node = selector.connect(new InetSocketAddress("localhost", server.port), BUFFER_SIZE, BUFFER_SIZE);
    while (!selector.connected().contains(node)) {
      selector.poll(10000L, EMPTY);
    }
    return node;
  }

  private NetworkSend createSend(long node, String s) {
    ByteBuffer buf = ByteBuffer.allocate(8 + s.getBytes().length);
    buf.putLong(s.getBytes().length + 8);
    buf.put(s.getBytes());
    buf.flip();
    return new NetworkSend(node, new BoundedByteBufferSend(buf), SystemTime.getInstance());
  }

  private String asString(NetworkReceive receive) {
    return new String(receive.getReceivedBytes().getPayload().array());
  }

  /**
   * Generate a random string of letters and digits of the given length
   *
   * @param len The length of the string
   * @return The random string
   */
  public static String randomString(int len, Random random) {
    String LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    String DIGITS = "0123456789";
    String LETTERS_AND_DIGITS = LETTERS + DIGITS;

    StringBuilder b = new StringBuilder();
    for (int i = 0; i < len; i++) {
      b.append(LETTERS_AND_DIGITS.charAt(random.nextInt(LETTERS_AND_DIGITS.length())));
    }
    return b.toString();
  }

  /**
   * A simple server that takes size delimited byte arrays and just echos them back to the sender.
   */
  static class EchoServer extends Thread {
    public final int port;
    private final ServerSocket serverSocket;
    private final List<Thread> threads;
    private final List<Socket> sockets;

    public EchoServer()
        throws Exception {
      this.port = 18283;
      this.serverSocket = new ServerSocket(port);
      this.threads = Collections.synchronizedList(new ArrayList<Thread>());
      this.sockets = Collections.synchronizedList(new ArrayList<Socket>());
    }

    public void run() {
      try {
        while (true) {
          final Socket socket = serverSocket.accept();
          sockets.add(socket);
          Thread thread = new Thread() {
            public void run() {
              try {
                DataInputStream input = new DataInputStream(socket.getInputStream());
                DataOutputStream output = new DataOutputStream(socket.getOutputStream());
                while (socket.isConnected() && !socket.isClosed()) {
                  long size = input.readLong();
                  byte[] bytes = new byte[(int) size - 8];
                  input.readFully(bytes);
                  output.writeLong(size);
                  output.write(bytes);
                  output.flush();
                }
              } catch (IOException e) {
                // ignore
              } finally {
                try {
                  socket.close();
                } catch (IOException e) {
                  // ignore
                }
              }
            }
          };
          thread.start();
          threads.add(thread);
        }
      } catch (IOException e) {
        // ignore
      }
    }

    public void closeConnections()
        throws IOException {
      for (Socket socket : sockets) {
        socket.close();
      }
    }

    public void close()
        throws IOException, InterruptedException {
      this.serverSocket.close();
      closeConnections();
      for (Thread t : threads) {
        t.join();
      }
      join();
    }
  }
}
