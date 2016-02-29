package com.github.ambry.network;

/**
 * A class that consists of a request to be sent over the network in the form of {@link Send}, and a destination for it
 * in the form of a host and a {@link Port}.
 */
public class RequestInfo {
  private final String host;
  private final Port port;
  private final Send request;

  /**
   * Construct a SendInfo with the given parameters
   * @param host the host to which the data is meant for
   * @param port the port on the host to which the data is meant for
   * @param request the data to be sent.
   */
  public RequestInfo(String host, Port port, Send request) {
    this.host = host;
    this.port = port;
    this.request = request;
  }

  /**
   * @return the host of the destination for the data associated with this object.
   */
  public String getHost() {
    return host;
  }

  /**
   * @return the {@link Port} of the destination for the data associated with this object.
   */
  public Port getPort() {
    return port;
  }

  /**
   * @return the request in the form of {@link Send} associated with this object.
   */
  public Send getRequest() {
    return request;
  }
}
